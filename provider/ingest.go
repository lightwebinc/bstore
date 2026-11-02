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
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/auth104"
	"github.com/lightwebinc/bstore/storerec"
)

// handleSubmit takes objects the way an overlay host does (POST /submit,
// the BEEF as the body, x-topics naming the topic): the plane's delivery to
// a subscriber, or a writer that unicasts to the provider as to a host.
// tm_bstoreblob_<b> takes piece carriers; tm_bstore_<b> takes entry
// carriers (and passes over everything else a records topic carries). The
// answer is the engine's: per topic, the outputs taken.
//
// A piece is checked before any policy of this provider's, on any blob
// topic, served or not, and answered as an engine answers: a carrier that
// breaks a rule is refused (400, nothing taken on any topic), and one whose
// funding tree is not yet in this provider's headers is a retry (503). A
// valid piece is answered 200 whether or not this provider keeps it, with a
// `refused` reason when it does not. So an overlay bridge whose facade
// routes a blob topic here publishes exactly the valid pieces, and a bridge
// feeding this provider from the plane retries a header lag and never a
// refusal. A feeder is answered as soon as its pieces have passed their
// checks and its takes run after (takeLater); a unicast writer, which reads
// the refusal, is answered after its take.
func (p *Provider) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		auth104.Write(w, auth104.Error(http.StatusMethodNotAllowed, "ERR_METHOD", "submit is a POST"))
		return
	}
	// A configured feeder (the local bridge) skips the per-address budgets.
	feeder := p.feeder(r)
	if !feeder {
		if ok, retry := p.limits.submits.Take(auth104.AddressKey(p.clientAddr(r))); !ok {
			p.metrics.inc("bstore_provider_submits_total", "result=limited")
			auth104.Write(w, auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many submissions; try again later", "Retry-After", strconv.Itoa(retry)))
			return
		}
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
	if !feeder {
		addr := auth104.AddressKey(p.clientAddr(r))
		if !p.limits.inflight.enter(addr) {
			auth104.Write(w, auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many submissions in flight from this address", "Retry-After", "1"))
			return
		}
		defer p.limits.inflight.leave(addr)
	}
	if r.ContentLength > bound {
		auth104.Write(w, auth104.Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the submission is over this topic's bound"))
		return
	}
	// The body is read under its own deadline, before a processing slot is
	// taken; at most maxBodies bodies are read or held at once, so the
	// memory they take is bounded. The slot is held until the body is done
	// with: by this handler, or by a feeder's takes once they are handed it
	// (below), so a body a pending take holds counts against the bound as
	// one being read does.
	select {
	case p.limits.bodies <- struct{}{}:
	case <-r.Context().Done():
		return
	}
	handed := false
	defer func() {
		if !handed {
			<-p.limits.bodies
		}
	}()
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyDeadline))
	beef, err := readBody(w, r, bound)
	if err == nil {
		select {
		case p.limits.ingest <- struct{}{}:
			defer func() { <-p.limits.ingest }()
		case <-r.Context().Done():
			return
		}
	}
	if err != nil {
		auth104.Write(w, auth104.Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the submission is over this topic's bound"))
		return
	}
	pieces := map[string]*storerec.PieceCarrier{}
	for _, t := range topics {
		bucket, ok := strings.CutPrefix(t, storerec.BlobPrefix)
		if !ok {
			continue
		}
		pc, err := storerec.CheckPieceCarrier(r.Context(), beef, bucket, p.hc)
		if err != nil {
			code, result := http.StatusBadRequest, "result=invalid"
			hdr := []string{}
			if errors.Is(err, storerec.ErrUnmined) {
				code, result, hdr = http.StatusServiceUnavailable, "result=unmined", []string{"Retry-After", "2"}
			}
			p.metrics.inc("bstore_provider_submits_total", result)
			writeEngineError(w, code, "piece: "+why(err), hdr...)
			return
		}
		pieces[t] = pc
	}
	if feeder {
		// A feeder (the local bridge) is answered once every piece has
		// passed its check, as an engine answers a piece it takes, and the
		// takes run after, on a goroutine that holds the body's slot until
		// they are done (takeLater). The bridge publishes on the answer and
		// reads no refusal of this provider's own, so nothing it acts on
		// waits for the store; a unicast writer reads the refusal, so it is
		// answered after its take (below). A piece is saved with the next
		// background pass either way: a crash between the answer and the
		// take loses what that pass would have.
		handed = true
		p.wg.Add(1)
		go p.takeLater(context.WithoutCancel(r.Context()), topics, beef, pieces)
		out := map[string]any{}
		for _, t := range topics {
			out[t] = map[string]any{"outputsToAdmit": []int{0}, "coinsToRetain": []int{}}
		}
		auth104.Write(w, auth104.JSON(http.StatusOK, out))
		return
	}
	out := map[string]any{}
	entries := false
	for _, t := range topics {
		admitted, why := p.take(r.Context(), t, beef, pieces[t])
		res := map[string]any{"outputsToAdmit": admitted, "coinsToRetain": []int{}}
		if why != "" {
			res["refused"] = why
			p.metrics.inc("bstore_provider_submits_total", "result=refused")
		} else {
			p.metrics.inc("bstore_provider_submits_total", "result=taken")
		}
		out[t] = res
		entries = entries || pieces[t] == nil
	}
	if entries {
		// A writer reads this answer as the entry taken, so it is on disk
		// first; one the plane delivered goes with the next pass.
		p.mu.Lock()
		err := p.save()
		p.mu.Unlock()
		if err != nil {
			auth104.Write(w, auth104.Error(http.StatusInternalServerError, "ERR_INTERNAL", "the entry could not be recorded"))
			return
		}
	}
	auth104.Write(w, auth104.JSON(http.StatusOK, out))
}

// takeLater is a feeder's take loop, run after its answer: at most maxTakes
// at once, so the takes queue here rather than on the store's lock, and the
// body's slot released once they are done. Each take's outcome is counted;
// a store error is logged where the synchronous path logs it (keepPiece),
// since no answer carries it.
func (p *Provider) takeLater(ctx context.Context, topics []string, beef []byte, pieces map[string]*storerec.PieceCarrier) {
	defer p.wg.Done()
	defer func() { <-p.limits.bodies }()
	p.limits.takes <- struct{}{}
	defer func() { <-p.limits.takes }()
	for _, t := range topics {
		_, why := p.take(ctx, t, beef, pieces[t])
		result := "taken"
		switch {
		case why == storeFailed:
			result = "error"
			p.metrics.inc("bstore_provider_submits_total", "result=refused")
		case why != "":
			result = "refused"
			p.metrics.inc("bstore_provider_submits_total", "result=refused")
		default:
			p.metrics.inc("bstore_provider_submits_total", "result=taken")
		}
		p.metrics.inc("bstore_provider_async_takes_total", "result="+result)
	}
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
	slices.Sort(ts)
	ts = slices.Compact(ts)
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
// and, when nothing was, why. pc is the piece carrier already checked on a
// blob topic (handleSubmit checks every piece first), or nil.
func (p *Provider) take(ctx context.Context, topic string, beef []byte, pc *storerec.PieceCarrier) ([]int, string) {
	b, blob := p.bucketOf(topic)
	if b == nil {
		return []int{}, "not a topic of this provider's"
	}
	if blob {
		if pc == nil {
			var err error
			if pc, err = storerec.CheckPieceCarrier(ctx, beef, b.Bucket, p.hc); err != nil {
				return []int{}, "piece: " + why(err)
			}
		}
		if !writerAllowed(b, pc.Piece.Writer) {
			return []int{}, "writer: not a writer this provider takes for the bucket"
		}
		if !p.fundingFree(pc.Funding, pc.Commitment) {
			return []int{}, "funding: a second carrier on one funding output"
		}
		if why := p.keepPiece(pc.Piece.Piece, hex.EncodeToString(pc.Piece.Root[:]), b.Bucket, hex.EncodeToString(pc.Piece.Writer)); why != "" {
			return []int{}, why
		}
		p.claimFunding(pc.Funding, pc.Commitment)
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
	if e.Kind != storerec.KindPut && e.Kind != storerec.KindDelete {
		return []int{}, ""
	}
	if !writerAllowed(b, e.Writer) {
		return []int{}, "writer: not a writer this provider takes for the bucket"
	}
	if !p.fundingFree(a.Carrier.Funding, a.Carrier.Commitment) {
		return []int{}, "funding: a second carrier on one funding output"
	}
	if e.Kind == storerec.KindDelete {
		p.noteDelete(e)
	} else if why := p.noteEntry(chainhash.Hash(a.Txid).String(), e, writerAllowList(b)); why != "" {
		return []int{}, why
	}
	p.claimFunding(a.Carrier.Funding, a.Carrier.Commitment)
	return []int{0}, ""
}

// claimFunding records the carrier a funding outpoint paid for: a writer
// makes at most one carrier per funding output (spec section 13.2), so a
// second, different one on the same outpoint is refused, and one mined
// output buys one piece or one entry, never a stream of them.
// It is checked before the carrier is taken and recorded only once it is,
// so a refused carrier leaves no record.
func (p *Provider) claimFunding(op transaction.Outpoint, c [32]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fundings.claim(fundingKey{txid: op.Txid, index: op.Index}, c, p.now().Unix())
}

// fundingFree reports whether a funding outpoint paid for no other carrier.
func (p *Provider) fundingFree(op transaction.Outpoint, c [32]byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fundings.free(fundingKey{txid: op.Txid, index: op.Index}, c)
}

func writerAllowList(b *BucketConfig) bool { return len(b.Writers) > 0 }

// The most undecided entries kept, from one writer and in all.
const (
	maxUndecidedPerWriter = 4096
	maxUndecided          = 10000
)

// noteEntry records a put entry, once, and asks for its root to be looked
// at.
func (p *Provider) noteEntry(c string, e *storerec.Entry, listed bool) string {
	rootHex := hex.EncodeToString(e.Root[:])
	p.mu.Lock()
	if _, ok := p.st.Entries[c]; !ok {
		w := hex.EncodeToString(e.Writer)
		mine, all := p.index.writers[w], p.index.total
		// A writer a bucket's allow-list names is never shut out by the
		// total that strangers fill.
		if mine >= maxUndecidedPerWriter || (!listed && all >= maxUndecided) {
			p.mu.Unlock()
			return "room: too many entries waiting for their closures"
		}
		rec := &EntryRec{C: c, Bucket: e.Bucket, Key: e.Key, Writer: hex.EncodeToString(e.Writer), Root: rootHex, Size: e.Size,
			ContentHash: hex.EncodeToString(e.ContentHash[:]), ProofRoot: hex.EncodeToString(e.ProofRoot[:]), Created: e.Created,
			At: p.now().UnixNano()}
		if p.deletedOnArrival(e) {
			// Its delete came first: ended on arrival, its pieces released,
			// never queued for a closure that nobody will ever claim.
			rec.Decision = decisionDeleted
			p.st.Entries[c] = rec
			p.index.add(rec)
			p.metrics.inc("bstore_provider_deletes_total")
			p.releasePiecesLocked(rootHex)
			_ = p.save()
			p.mu.Unlock()
			p.logf("entry %s: %s %s: deleted before it arrived", short(c), e.Bucket, e.Key)
			return ""
		}
		if e.Hold != nil {
			rec.Hold = &HoldRec{Txid: chainhash.Hash(e.Hold.Txid).String(), Vout: uint32(e.Hold.Vout), Sats: e.Hold.Satoshis, T: e.Hold.T,
				K: e.Hold.K, Retention: e.Hold.Retention, Deadline: e.Hold.Deadline, CheckC: e.Hold.CheckC}
		}
		p.st.Entries[c] = rec
		p.index.add(rec)
		p.pruneEntries()
		p.dirty = true
		p.logf("entry %s: %s %s, %d bytes, root %s", short(c), e.Bucket, e.Key, e.Size, short(rootHex))
	}
	p.mu.Unlock()
	p.enqueue(rootHex)
	return ""
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

// storeFailed is why a piece was not kept when the store could not write
// it: an error of this provider's, not a refusal of the piece.
const storeFailed = "store: the piece could not be stored"

// keepPiece stores a piece's bytes as unclaimed, if there is room.
func (p *Provider) keepPiece(data []byte, rootHex, bucket, writer string) string {
	h := sha256.Sum256(data)
	hh := hex.EncodeToString(h[:])
	if p.front != nil {
		// A front keeps no second copy of what it handed to the backend.
		p.mu.Lock()
		handed := p.handedObjectLocked(hh)
		p.mu.Unlock()
		if handed {
			return ""
		}
	}
	stored := false
	if !p.store.Has(h) {
		if why := p.room(uint64(len(data)), writer); why != "" {
			return why
		}
		if err := p.store.PutHashed(h, data); err != nil {
			p.logf("storing piece %s: %v", short(hh), err)
			return storeFailed
		}
		// Saved with the next background pass: a piece a crash forgets is
		// collected from the store on the next start, and sent again.
		p.mu.Lock()
		p.st.Unclaimed[hh] = &Unclaimed{Root: rootHex, Bucket: bucket, Writer: writer, Size: uint64(len(data)), At: p.now().Unix()}
		p.dirty = true
		p.mu.Unlock()
		stored = true
	}
	p.arrived(h, data, mustHash(rootHex), stored)
	p.mu.Lock()
	wanted := len(p.index.undecided[rootHex]) > 0
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
	id        string
	owner     string
	addr      string
	expires   time.Time
	objects   map[[32]byte]bool
	committed map[[32]byte]bool
}

func newUploads(c *Config, now func() time.Time) *uploads {
	return &uploads{max: c.Limits.UploadSessions, life: time.Duration(c.Limits.UploadSessionSeconds) * time.Second, now: now, byID: map[string]*upload{}}
}

// maxUploadsPerIdentity and maxUploadsPerAddress bound the sessions one
// identity, and one address, hold open.
const (
	maxUploadsPerIdentity = 4
	maxUploadsPerAddress  = 8
)

func (u *uploads) open(owner, addr string) (*upload, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	now := u.now()
	n, a := 0, 0
	for id, s := range u.byID {
		if now.After(s.expires) {
			delete(u.byID, id)
			continue
		}
		if s.owner == owner {
			n++
		}
		if s.addr == addr {
			a++
		}
	}
	if len(u.byID) >= u.max || n >= maxUploadsPerIdentity || a >= maxUploadsPerAddress {
		return nil, errors.New("too many upload sessions open; commit or let one expire")
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	s := &upload{id: hex.EncodeToString(b[:]), owner: owner, addr: addr, expires: now.Add(u.life), objects: map[[32]byte]bool{}, committed: map[[32]byte]bool{}}
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
func (p *Provider) uploadCreate(_ context.Context, r *http.Request, body []byte, who *auth104.Caller) auth104.Reply {
	if who == nil {
		return auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", "an upload session is opened over BRC-104 authentication")
	}
	addr := auth104.AddressKey(p.clientAddr(r))
	if ok, retry := p.limits.uploads.Take(addr); !ok {
		return auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many upload sessions from this address", "Retry-After", itoa(uint64(retry)))
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
	s, err := p.uploads.open(who.Hex, addr)
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
		addr := auth104.AddressKey(p.clientAddr(r))
		if !p.limits.inflight.enter(addr) {
			auth104.Write(w, auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many uploads in flight from this address", "Retry-After", "1"))
			return
		}
		defer p.limits.inflight.leave(addr)
		_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyDeadline))
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
		if why := p.room(uint64(len(body)), uploaderKey+who.Hex); why != "" {
			return auth104.Error(http.StatusInsufficientStorage, "ERR_NO_ROOM", why)
		}
		if err := p.store.Put(obj, body); err != nil {
			return auth104.Error(http.StatusInternalServerError, "ERR_STORE", "the object could not be stored")
		}
		p.mu.Lock()
		p.st.Unclaimed[hex.EncodeToString(obj[:])] = &Unclaimed{Writer: uploaderKey + who.Hex, Size: uint64(len(body)), At: p.now().Unix()}
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
	rootHex := hex.EncodeToString(root[:])
	p.mu.Lock()
	hh := p.st.Holdings[rootHex]
	held := hh != nil && hh.Verified && !hh.Damaged
	p.mu.Unlock()
	p.uploads.mu.Lock()
	staged, done := s.objects[root], s.committed[root]
	p.uploads.mu.Unlock()
	if !held && !done {
		if ok, retry := p.limits.commitsBy.Take(who.Hex); !ok {
			return auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many commits from this identity", "Retry-After", itoa(uint64(retry)))
		}
		if !staged {
			return auth104.Error(http.StatusUnprocessableEntity, "ERR_INCOMPLETE", "the root node was not uploaded in this session")
		}
		if why := p.commitCheck(ctx, s, root); why != "" {
			return auth104.JSON(http.StatusUnprocessableEntity, map[string]string{"status": "error", "code": "ERR_CLOSURE", "description": why})
		}
	}
	p.uploads.mu.Lock()
	s.committed[root] = true
	p.uploads.mu.Unlock()
	p.mu.Lock()
	for o := range s.objects {
		if u := p.st.Unclaimed[hex.EncodeToString(o[:])]; u != nil && u.Root == "" && u.Writer == uploaderKey+who.Hex {
			u.Root = rootHex
		}
	}
	p.dirty = true
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

// uploaderKey marks the unclaimed pieces an identity uploaded: identities
// cost nothing, so uploads have a share of their own.
const uploaderKey = "upload:"

// commitCheck is BRC-167's closure check at commit, bounded: the root's
// logical length within the provider's object bound before anything is
// walked, every object staged in this session or held already, and the
// walk under the verification limit with a deadline.
func (p *Provider) commitCheck(ctx context.Context, s *upload, root [32]byte) string {
	rb, err := p.fetch(root)
	if err != nil {
		return "the root node is not here"
	}
	r, err := chirp.DecodeRoot(rb)
	if err != nil {
		return "root: " + err.Error()
	}
	if r.Length > p.cfg.Policy.MaxObject {
		return "the object is larger than this provider takes"
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	select {
	case p.limits.commits <- struct{}{}:
		defer func() { <-p.limits.commits }()
	case <-ctx.Done():
		return "busy; commit again later"
	}
	fetch := func(h [32]byte) ([]byte, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		p.uploads.mu.Lock()
		ok := s.objects[h]
		p.uploads.mu.Unlock()
		if !ok && !p.heldAnywhere(h) {
			return nil, errors.New("not staged in this session")
		}
		return p.fetch(h)
	}
	if _, err := storerec.VerifyCHIRP(root, fetch); err != nil {
		return why(err)
	}
	return ""
}

func (p *Provider) heldAnywhere(h [32]byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heldObjectLocked(hex.EncodeToString(h[:]))
}

// writeEngineError answers as an overlay engine answers a refusal or a
// retry: the status, then {"error": msg}, with any header pairs given.
func writeEngineError(w http.ResponseWriter, code int, msg string, hdr ...string) {
	for i := 0; i+1 < len(hdr); i += 2 {
		w.Header().Set(hdr[i], hdr[i+1])
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// readBody reads a submit body of at most bound bytes. With a Content-Length
// it reads into one buffer of that size: io.ReadAll grows by doubling, so a
// 4 MiB piece cost about 16 MB of allocation, zeroing and copying before it
// was even parsed (memmove and memclr were a third of the provider's CPU on
// the lab edge). A body longer than its declared length is refused by the
// extra read; a shorter one is an error from ReadFull.
func readBody(w http.ResponseWriter, r *http.Request, bound int64) ([]byte, error) {
	lr := http.MaxBytesReader(w, r.Body, bound)
	n := r.ContentLength
	if n <= 0 || n > bound {
		return io.ReadAll(lr)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(lr, buf); err != nil {
		return nil, err
	}
	var extra [1]byte
	if m, _ := lr.Read(extra[:]); m > 0 {
		return nil, errors.New("body longer than its Content-Length")
	}
	return buf, nil
}
