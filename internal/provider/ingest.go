package provider

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/auth104"
	"github.com/lightwebinc/bstore/internal/storerec"
)

// handleSubmit takes objects the way an overlay host does (POST /submit,
// the BEEF as the body, x-topics naming the topic): the plane's delivery to
// a subscriber, or a writer that unicasts to the provider as to a host.
// tm_bstoreblob_<b> takes piece carriers; tm_bstore_<b> takes entry
// carriers (and passes over everything else a records topic carries). The
// answer is the engine's: per topic, the outputs taken.
func (p *Provider) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		auth104.Write(w, auth104.Error(http.StatusMethodNotAllowed, "ERR_METHOD", "submit is a POST"))
		return
	}
	if ok, retry := p.limits.submits.Take(auth104.AddressKey(r.RemoteAddr)); !ok {
		p.metrics.inc("bstore_provider_submits_total", "result=limited")
		auth104.Write(w, auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many submissions; try again later", "Retry-After", strconv.Itoa(retry)))
		return
	}
	topics, err := parseTopics(r.Header.Get("x-topics"))
	if err != nil {
		auth104.Write(w, auth104.Error(http.StatusBadRequest, "ERR_TOPICS", err.Error()))
		return
	}
	bound := int64(maxRecordsBEEF)
	for _, t := range topics {
		if strings.HasPrefix(t, storerec.BlobPrefix) {
			bound = storerec.MaxPieceBEEF
		}
	}
	select {
	case p.limits.ingest <- struct{}{}:
		defer func() { <-p.limits.ingest }()
	case <-r.Context().Done():
		return
	}
	if r.ContentLength > bound {
		auth104.Write(w, auth104.Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the submission is over this topic's bound"))
		return
	}
	beef, err := io.ReadAll(http.MaxBytesReader(w, r.Body, bound))
	if err != nil {
		auth104.Write(w, auth104.Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the submission is over this topic's bound"))
		return
	}
	out := map[string]any{}
	for _, t := range topics {
		admitted, why := p.take(r.Context(), t, beef)
		res := map[string]any{"outputsToAdmit": admitted, "coinsToRetain": []int{}}
		if why != "" {
			res["refused"] = why
			p.metrics.inc("bstore_provider_submits_total", "result=refused")
		} else {
			p.metrics.inc("bstore_provider_submits_total", "result=taken")
		}
		out[t] = res
	}
	auth104.Write(w, auth104.JSON(http.StatusOK, out))
}

// parseTopics reads x-topics: one bare name, or a JSON array of names.
func parseTopics(h string) ([]string, error) {
	h = strings.TrimSpace(h)
	var ts []string
	if strings.HasPrefix(h, "[") {
		if err := json.Unmarshal([]byte(h), &ts); err != nil {
			return nil, errors.New("x-topics is not a JSON array of names")
		}
	} else if h != "" {
		ts = []string{h}
	}
	if len(ts) == 0 || len(ts) > 4 {
		return nil, errors.New("x-topics names one to four topics")
	}
	return ts, nil
}

// bucketOf is the bucket a topic of this provider's names, and whether the
// topic is its blob topic.
func (p *Provider) bucketOf(topic string) (*BucketConfig, bool) {
	for i := range p.cfg.Buckets {
		b := &p.cfg.Buckets[i]
		switch topic {
		case storerec.BlobTopic(b.Bucket):
			return b, true
		case storerec.RecordsTopic(b.Bucket):
			return b, false
		}
	}
	return nil, false
}

func writerAllowed(b *BucketConfig, writer []byte) bool {
	if len(b.Writers) == 0 {
		return true
	}
	return slices.Contains(b.Writers, hex.EncodeToString(writer))
}

// take applies one submission to one topic; it returns the outputs taken
// and, when nothing was, why.
func (p *Provider) take(ctx context.Context, topic string, beef []byte) ([]int, string) {
	b, blob := p.bucketOf(topic)
	if b == nil {
		return []int{}, "not a topic of this provider's"
	}
	if blob {
		pc, err := storerec.CheckPieceCarrier(ctx, beef, b.Bucket, p.hc)
		if err != nil {
			return []int{}, "piece: " + why(err)
		}
		if !writerAllowed(b, pc.Piece.Writer) {
			return []int{}, "writer: not a writer this provider takes for the bucket"
		}
		if why := p.keepPiece(pc.Piece.Piece, hex.EncodeToString(pc.Piece.Root[:]), b.Bucket, hex.EncodeToString(pc.Piece.Writer)); why != "" {
			return []int{}, why
		}
		return []int{0}, ""
	}
	if len(beef) > maxRecordsBEEF {
		return []int{}, "beef: over the bound"
	}
	a, err := storerec.Admit(ctx, beef, nil, storerec.Host{Bucket: b.Bucket, Headers: p.hc})
	if err != nil {
		return []int{}, why(err)
	}
	if a.Kind != storerec.TxEntry || a.Carrier == nil || a.Carrier.Entry == nil {
		return []int{}, "" // the topic's other objects are a host's, not a provider's
	}
	e := a.Carrier.Entry
	if e.Kind != storerec.KindPut {
		return []int{}, ""
	}
	if !writerAllowed(b, e.Writer) {
		return []int{}, "writer: not a writer this provider takes for the bucket"
	}
	p.noteEntry(chainhash.Hash(a.Txid).String(), e)
	return []int{0}, ""
}

// noteEntry records a put entry, once, and asks for its root to be looked
// at.
func (p *Provider) noteEntry(c string, e *storerec.Entry) {
	rootHex := hex.EncodeToString(e.Root[:])
	p.mu.Lock()
	if _, ok := p.st.Entries[c]; !ok {
		rec := &EntryRec{C: c, Bucket: e.Bucket, Writer: hex.EncodeToString(e.Writer), Root: rootHex, Size: e.Size,
			ContentHash: hex.EncodeToString(e.ContentHash[:]), ProofRoot: hex.EncodeToString(e.ProofRoot[:]), Created: e.Created,
			At: p.now().UnixNano()}
		if e.Hold != nil {
			rec.Hold = &HoldRec{Txid: chainhash.Hash(e.Hold.Txid).String(), Vout: uint32(e.Hold.Vout), Sats: e.Hold.Satoshis, T: e.Hold.T,
				K: e.Hold.K, Retention: e.Hold.Retention, Deadline: e.Hold.Deadline, CheckC: e.Hold.CheckC}
		}
		p.st.Entries[c] = rec
		p.pruneEntries()
		_ = p.save()
		p.logf("entry %s: %s %s, %d bytes, root %s", short(c), e.Bucket, e.Key, e.Size, short(rootHex))
	}
	p.mu.Unlock()
	p.enqueue(rootHex)
}

// maxEntries bounds the entries kept; past it the oldest declined go first.
const maxEntries = 100000

func (p *Provider) pruneEntries() {
	if len(p.st.Entries) <= maxEntries {
		return
	}
	var declined []*EntryRec
	for _, e := range p.st.Entries {
		if e.Decision != "" && e.Decision != decisionHeld {
			declined = append(declined, e)
		}
	}
	slices.SortFunc(declined, func(a, b *EntryRec) int { return int(a.At - b.At) })
	for _, e := range declined {
		if len(p.st.Entries) <= maxEntries {
			return
		}
		delete(p.st.Entries, e.C)
	}
}

// keepPiece stores a piece's bytes as unclaimed, if there is room.
func (p *Provider) keepPiece(data []byte, rootHex, bucket, writer string) string {
	h := sha256.Sum256(data)
	hh := hex.EncodeToString(h[:])
	if !p.store.Has(h) {
		if why := p.room(uint64(len(data)), writer); why != "" {
			return why
		}
		if err := p.store.Put(h, data); err != nil {
			p.logf("storing piece %s: %v", short(hh), err)
			return "store: the piece could not be stored"
		}
		p.mu.Lock()
		p.st.Unclaimed[hh] = &Unclaimed{Root: rootHex, Bucket: bucket, Writer: writer, Size: uint64(len(data)), At: p.now().Unix()}
		_ = p.save()
		p.mu.Unlock()
	}
	p.mu.Lock()
	wanted := false
	for _, e := range p.st.Entries {
		if e.Root == rootHex && e.Decision == "" {
			wanted = true
			break
		}
	}
	p.mu.Unlock()
	if wanted && p.complete(mustHash(rootHex)) {
		p.enqueue(rootHex)
	}
	return ""
}

// complete reports, without reading a blob, whether the store holds every
// object a root's closure names: the root and branch nodes are read, the
// blobs only looked for. It bounds itself as a closure check does.
func (p *Provider) complete(root [32]byte) bool {
	rb, err := p.fetch(root)
	if err != nil {
		return false
	}
	r, err := chirp.DecodeRoot(rb)
	if err != nil {
		return true // the full check says why
	}
	refs := 0
	var walk func(cs []chirp.Child, depth int) bool
	walk = func(cs []chirp.Child, depth int) bool {
		for _, c := range cs {
			refs++
			if refs > chirp.DefaultMaxReferences || depth > chirp.MaxDepth {
				return true // the full check refuses it
			}
			if !p.store.Has(c.Hash) {
				return false
			}
			if c.Kind == chirp.ChildBranch {
				b, err := p.fetch(c.Hash)
				if err != nil {
					return false
				}
				br, err := chirp.DecodeBranch(b)
				if err != nil {
					return true
				}
				if !walk(br.Children, depth+1) {
					return false
				}
			}
		}
		return true
	}
	return walk(r.Children, 1)
}

// uploads are BRC-167's baseline upload sessions, the unicast path: a
// writer stages a closure's objects and commits its root. A session belongs
// to the identity that opened it (BRC-104); its objects are unclaimed
// pieces, kept until an entry claims their root or their time runs out.
type uploads struct {
	mu   sync.Mutex
	max  int
	life time.Duration
	now  func() time.Time
	byID map[string]*upload
}

type upload struct {
	id      string
	owner   string
	expires time.Time
	objects map[[32]byte]bool
}

func newUploads(c *Config, now func() time.Time) *uploads {
	return &uploads{max: c.Limits.UploadSessions, life: time.Duration(c.Limits.UploadSessionSeconds) * time.Second, now: now, byID: map[string]*upload{}}
}

// maxUploadsPerIdentity bounds the sessions one identity holds open.
const maxUploadsPerIdentity = 4

func (u *uploads) open(owner string) (*upload, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	now := u.now()
	n := 0
	for id, s := range u.byID {
		if now.After(s.expires) {
			delete(u.byID, id)
			continue
		}
		if s.owner == owner {
			n++
		}
	}
	if len(u.byID) >= u.max || n >= maxUploadsPerIdentity {
		return nil, errors.New("too many upload sessions open; commit or let one expire")
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	s := &upload{id: hex.EncodeToString(b[:]), owner: owner, expires: now.Add(u.life), objects: map[[32]byte]bool{}}
	u.byID[s.id] = s
	return s, nil
}

func (u *uploads) get(id, owner string) *upload {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := u.byID[id]
	if s == nil || s.owner != owner || u.now().After(s.expires) {
		return nil
	}
	return s
}

// maxObjectsPerUpload bounds one session's objects.
const maxObjectsPerUpload = 1 << 16

// uploadCreate is POST <prefix>/chirp/v1/uploads.
func (p *Provider) uploadCreate(_ context.Context, _ *http.Request, body []byte, who *auth104.Caller) auth104.Reply {
	if who == nil {
		return auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", "an upload session is opened over BRC-104 authentication")
	}
	var req struct {
		RetentionSeconds string  `json:"retentionSeconds"`
		LogicalLength    *string `json:"logicalLength"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return auth104.Error(http.StatusBadRequest, "ERR_BAD_REQUEST", "the body is not {retentionSeconds, logicalLength}")
	}
	if v, err := strconv.ParseUint(req.RetentionSeconds, 10, 64); err != nil || v == 0 || strconv.FormatUint(v, 10) != req.RetentionSeconds {
		return auth104.Error(http.StatusBadRequest, "ERR_BAD_REQUEST", "retentionSeconds is a positive decimal string")
	}
	if req.LogicalLength != nil {
		v, err := strconv.ParseUint(*req.LogicalLength, 10, 64)
		if err != nil || strconv.FormatUint(v, 10) != *req.LogicalLength {
			return auth104.Error(http.StatusBadRequest, "ERR_BAD_REQUEST", "logicalLength is null or a decimal string")
		}
		if v > p.cfg.Policy.MaxObject {
			return auth104.Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the object is larger than this provider takes")
		}
	}
	s, err := p.uploads.open(who.Hex)
	if err != nil {
		return auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", err.Error())
	}
	return auth104.JSON(http.StatusCreated, map[string]string{"uploadId": s.id, "stagingExpiresAt": strconv.FormatInt(s.expires.Unix(), 10)})
}

// handleUpload is PUT and HEAD <prefix>/chirp/v1/uploads/<id>/objects/<object>
// and POST <prefix>/chirp/v1/uploads/<id>/commit.
func (p *Provider) handleUpload(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, p.prefix()+"/chirp/v1/uploads/")
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) == 3 && parts[1] == "objects" && (r.Method == http.MethodPut || r.Method == http.MethodHead):
		obj, err := chirp.ParseIdentifier(parts[2])
		if err != nil {
			auth104.Write(w, auth104.Error(http.StatusNotFound, "ERR_NOT_FOUND", "not an object identifier"))
			return
		}
		select {
		case p.limits.ingest <- struct{}{}:
			defer func() { <-p.limits.ingest }()
		case <-r.Context().Done():
			return
		}
		p.auth.Serve(w, r, maxUploadObject, func(_ context.Context, r *http.Request, body []byte, who *auth104.Caller) auth104.Reply {
			return p.uploadObject(r.Method, parts[0], obj, body, who)
		})
	case len(parts) == 2 && parts[1] == "commit" && r.Method == http.MethodPost:
		p.auth.Serve(w, r, maxCommitReq, func(ctx context.Context, _ *http.Request, body []byte, who *auth104.Caller) auth104.Reply {
			return p.uploadCommit(ctx, parts[0], body, who)
		})
	default:
		auth104.Write(w, auth104.Error(http.StatusNotFound, "ERR_NOT_FOUND", "no such upload route"))
	}
}

func (p *Provider) uploadObject(method, id string, obj [32]byte, body []byte, who *auth104.Caller) auth104.Reply {
	if who == nil {
		return auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", "uploads are over BRC-104 authentication")
	}
	s := p.uploads.get(id, who.Hex)
	if s == nil {
		return auth104.Error(http.StatusNotFound, "ERR_NOT_FOUND", "no such upload session for this identity")
	}
	if method == http.MethodHead {
		p.uploads.mu.Lock()
		has := s.objects[obj]
		p.uploads.mu.Unlock()
		if has && p.store.Has(obj) {
			return auth104.Reply{Status: http.StatusOK}
		}
		return auth104.Reply{Status: http.StatusNotFound}
	}
	if sha256.Sum256(body) != obj {
		return auth104.Error(http.StatusUnprocessableEntity, "ERR_DIGEST", "the body does not hash to the object identifier")
	}
	p.uploads.mu.Lock()
	full := len(s.objects) >= maxObjectsPerUpload
	had := s.objects[obj]
	p.uploads.mu.Unlock()
	if full && !had {
		return auth104.Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the session holds as many objects as it may")
	}
	status := http.StatusCreated
	if p.store.Has(obj) {
		status = http.StatusNoContent
	} else {
		if why := p.room(uint64(len(body)), who.Hex); why != "" {
			return auth104.Error(http.StatusInsufficientStorage, "ERR_NO_ROOM", why)
		}
		if err := p.store.Put(obj, body); err != nil {
			return auth104.Error(http.StatusInternalServerError, "ERR_STORE", "the object could not be stored")
		}
		p.mu.Lock()
		p.st.Unclaimed[hex.EncodeToString(obj[:])] = &Unclaimed{Writer: who.Hex, Size: uint64(len(body)), At: p.now().Unix()}
		_ = p.save()
		p.mu.Unlock()
	}
	p.uploads.mu.Lock()
	s.objects[obj] = true
	p.uploads.mu.Unlock()
	return auth104.Reply{Status: status}
}

// uploadCommit checks the committed root's closure as BRC-167's commit
// does, against the session's objects and the store, and claims them for
// the root. A provider advertises a root only for an entry that names it,
// so the commit answers 201 with the advertised location once one does and
// the root is advertised, and 202 while it waits for the entry.
func (p *Provider) uploadCommit(ctx context.Context, id string, body []byte, who *auth104.Caller) auth104.Reply {
	if who == nil {
		return auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", "uploads are over BRC-104 authentication")
	}
	s := p.uploads.get(id, who.Hex)
	if s == nil {
		return auth104.Error(http.StatusNotFound, "ERR_NOT_FOUND", "no such upload session for this identity")
	}
	var req struct {
		RootIdentifier string `json:"rootIdentifier"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return auth104.Error(http.StatusBadRequest, "ERR_BAD_REQUEST", "the body is not {rootIdentifier}")
	}
	root, err := chirp.ParseIdentifier(req.RootIdentifier)
	if err != nil {
		return auth104.Error(http.StatusBadRequest, "ERR_BAD_REQUEST", "rootIdentifier is not a UHRP identifier")
	}
	p.uploads.mu.Lock()
	staged := s.objects[root]
	p.uploads.mu.Unlock()
	if !staged && !p.store.Has(root) {
		return auth104.Error(http.StatusUnprocessableEntity, "ERR_INCOMPLETE", "the root node was not uploaded")
	}
	c, err := storerec.VerifyCHIRP(root, p.fetch)
	if err != nil {
		return auth104.JSON(http.StatusUnprocessableEntity, map[string]string{"status": "error", "code": "ERR_CLOSURE", "description": why(err)})
	}
	rootHex := hex.EncodeToString(root[:])
	p.mu.Lock()
	for _, o := range c.Objects {
		if u := p.st.Unclaimed[hex.EncodeToString(o[:])]; u != nil {
			u.Root = rootHex
		}
	}
	_ = p.save()
	h := p.st.Holdings[rootHex]
	var ad *AdRec
	if h != nil && h.Ad != nil && !h.Damaged {
		ad = h.Ad
	}
	p.mu.Unlock()
	p.enqueue(rootHex)
	if ad == nil {
		return auth104.JSON(http.StatusAccepted, map[string]string{"rootIdentifier": req.RootIdentifier, "state": "staged",
			"description": "the closure is verified; it is advertised once an entry for the root arrives and this provider takes it"})
	}
	return auth104.JSON(http.StatusCreated, map[string]any{"chirpURL": chirp.URL(root), "uhrpURL": "uhrp://" + req.RootIdentifier,
		"hostedFileLocation": p.be.ServeURL(root), "expiryTime": ad.Expiry})
}
