package s3d

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/storerec"
)

// Multipart uploads (spec section 16.2) are the gateway's own state: the
// parts are kept on disk as they arrive, cut into 4 MiB blobs and hashed
// (partstage.go), and assembled into one closure at completion, with one
// entry. An upload belongs to the gateway and the access key that began
// it.

const minPart = 5 << 20

type uploads struct {
	dir     string
	now     func() time.Time
	writers chan struct{} // the slots part blobs are written in (limits.part_writers)
	mu      sync.Mutex
	m       map[string]*upload
}

type upload struct {
	ID          string              `json:"id"`
	Bucket      string              `json:"bucket"`
	Key         string              `json:"key"`
	AccessKey   string              `json:"accessKey"`
	ContentType string              `json:"contentType"`
	Meta        []storerec.MetaPair `json:"meta,omitempty"`
	Created     time.Time           `json:"created"`

	mu         sync.Mutex
	completing bool
	gone       bool
	parts      map[int]partRecord // read from disk once
	done       *completedRec      // the result, once its entry was made
	dir        string
	writers    chan struct{} // uploads.writers
	gens       uint64        // numbers each part record held, in turn
	arriving   map[int]int64 // the parts streaming in, by number: their sizes
	run        runHash
}

// completedRec is a completion whose entry was made, kept so that the
// same completion sent again is answered the same and publishes nothing.
type completedRec struct {
	Location string `json:"location"`
	ETag     string `json:"etag"`
	SHA256   string `json:"sha256"`
}

// partRecord is a part held. MD5 or SHA256 is the ETag it was given (hex;
// MD5 with etag_md5, and in every record written before SHA-256 ETags).
// Dir names the directory its blob files are in, Blobs and Segs their
// hashes and segment subtree roots (hex); File names the one file a part
// no object can be linked through is kept in; a record with neither is a
// part kept at partPath, as before blobs.
type partRecord struct {
	Size   int64    `json:"size"`
	MD5    string   `json:"md5,omitempty"`
	SHA256 string   `json:"sha256,omitempty"`
	Dir    string   `json:"dir,omitempty"`
	File   string   `json:"file,omitempty"`
	Blobs  []string `json:"blobs,omitempty"`
	Segs   []string `json:"segs,omitempty"`

	gen   uint64 // which record of the part this is, in this process
	fresh bool   // its blobs were hashed by this process
	state []byte // the content hash's state after the part, taken inline
}

// openUploads reopens the uploads under dir; writers is how many part
// blobs are written at once.
func openUploads(dir string, now func() time.Time, writers int) (*uploads, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	u := &uploads{dir: dir, now: now, writers: make(chan struct{}, max(writers, 1)), m: map[string]*upload{}}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name(), "upload.json"))
		if err != nil {
			_ = os.RemoveAll(filepath.Join(dir, e.Name()))
			continue
		}
		var up upload
		if json.Unmarshal(raw, &up) != nil || up.ID != e.Name() || !validUploadID(up.ID) {
			_ = os.RemoveAll(filepath.Join(dir, e.Name()))
			continue
		}
		up.dir = filepath.Join(dir, up.ID)
		up.writers = u.writers
		if raw, err := os.ReadFile(filepath.Join(up.dir, "completed.json")); err == nil {
			var c completedRec
			if json.Unmarshal(raw, &c) == nil {
				up.done = &c
			}
		}
		up.loadParts()
		up.tidy()
		u.m[up.ID] = &up
	}
	return u, nil
}

func validUploadID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}

func (u *uploads) get(id string) *upload {
	if !validUploadID(id) {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.m[id]
}

func (u *uploads) remove(up *upload) {
	u.mu.Lock()
	delete(u.m, up.ID)
	u.mu.Unlock()
	_ = os.RemoveAll(up.dir)
}

// expire drops uploads open longer than ttl.
func (u *uploads) expire(ttl time.Duration) {
	u.mu.Lock()
	var old []*upload
	for _, up := range u.m {
		if u.now().Sub(up.Created) > ttl {
			old = append(old, up)
		}
	}
	u.mu.Unlock()
	for _, up := range old {
		up.mu.Lock()
		busy := up.completing
		if !busy {
			up.gone = true
		}
		up.mu.Unlock()
		if !busy {
			u.remove(up)
		}
	}
}

func (up *upload) partPath(n int) string { return filepath.Join(up.dir, fmt.Sprintf("part-%05d", n)) }

func (up *upload) part(n int) (*partRecord, error) {
	raw, err := os.ReadFile(up.partPath(n) + ".json")
	if err != nil {
		return nil, err
	}
	var p partRecord
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// loadParts reads the part records once; the caller holds up.mu.
func (up *upload) loadParts() {
	if up.parts != nil {
		return
	}
	up.parts = map[int]partRecord{}
	ents, _ := os.ReadDir(up.dir)
	for _, e := range ents {
		name, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok || !strings.HasPrefix(name, "part-") {
			continue
		}
		k, err := strconv.Atoi(strings.TrimPrefix(name, "part-"))
		if err != nil {
			continue
		}
		if p, err := up.part(k); err == nil {
			up.gens++
			p.gen = up.gens
			up.parts[k] = *p
		}
	}
}

// tidy removes what a stop cut short: a part's files no record names, and
// temporary files. It runs at start, before any request.
func (up *upload) tidy() {
	ents, _ := os.ReadDir(up.dir)
	for _, e := range ents {
		name := e.Name()
		if strings.HasPrefix(name, "tmp-") {
			_ = os.Remove(filepath.Join(up.dir, name))
			continue
		}
		rest, ok := strings.CutPrefix(name, "part-")
		if !ok || strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".tmp") {
			continue
		}
		num, _, dotted := strings.Cut(rest, ".")
		k, err := strconv.Atoi(num)
		if err != nil {
			continue
		}
		p, held := up.parts[k]
		switch {
		case dotted && (!held || (p.Dir != name && p.File != name)):
			_ = os.RemoveAll(filepath.Join(up.dir, name))
		case !dotted && held && (p.Dir != "" || p.File != ""):
			_ = os.Remove(filepath.Join(up.dir, name))
		}
	}
}

// size is the bytes of every part held but skip; the caller holds up.mu.
func (up *upload) size(skip int) int64 {
	up.loadParts()
	var n int64
	for k, p := range up.parts {
		if k != skip {
			n += p.Size
		}
	}
	return n
}

// lookup is the upload id names, for this bucket, key and access key.
func (s *Server) lookupUpload(r *http.Request, a *Auth, b *bucket, key string) (*upload, *s3Error) {
	up := s.uploads.get(r.URL.Query().Get("uploadId"))
	if up == nil || up.Bucket != b.name || up.Key != key {
		return nil, errNoSuchUpload
	}
	if up.AccessKey != a.Key.ID {
		return nil, errAccessDenied
	}
	return up, nil
}

func (s *Server) createUpload(w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key string) *s3Error {
	if e := refuseUnsupported(r.Header); e != nil {
		return e
	}
	if r.Header.Get("X-Amz-Checksum-Type") == "COMPOSITE" {
		return notImplemented("composite checksums")
	}
	ct, meta, e := entryMeta(r.Header)
	if e != nil {
		return e
	}
	s.uploads.mu.Lock()
	total, mine := len(s.uploads.m), 0
	for _, up := range s.uploads.m {
		if up.AccessKey == a.Key.ID {
			mine++
		}
	}
	if total >= s.cfg.Limits.Uploads || mine >= s.cfg.Limits.UploadsPerKey {
		s.uploads.mu.Unlock()
		return &s3Error{"SlowDown", "Too many multipart uploads are open; complete or abort some.", http.StatusServiceUnavailable}
	}
	var idb [16]byte
	_, _ = rand.Read(idb[:])
	up := &upload{ID: hex.EncodeToString(idb[:]), Bucket: b.name, Key: key, AccessKey: a.Key.ID, ContentType: ct, Meta: meta,
		Created: s.opts.Now()}
	up.dir = filepath.Join(s.uploads.dir, up.ID)
	up.writers = s.uploads.writers
	s.uploads.m[up.ID] = up
	s.uploads.mu.Unlock()
	if err := os.MkdirAll(up.dir, 0o700); err != nil {
		s.uploads.remove(up)
		return errInternal
	}
	raw, _ := json.Marshal(up)
	if err := writeSynced(filepath.Join(up.dir, "upload.json"), raw); err != nil {
		s.uploads.remove(up)
		return errInternal
	}
	writeXML(w, http.StatusOK, initiateMultipart{NS: s3NS, Bucket: b.name, Key: key, UploadID: up.ID})
	return nil
}

func (s *Server) uploadPart(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key string) *s3Error {
	n, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || n < 1 || n > s.cfg.Limits.MaxParts {
		return invalidArgument(fmt.Sprintf("Part number must be an integer between 1 and %d, inclusive", s.cfg.Limits.MaxParts))
	}
	up, e := s.lookupUpload(r, a, b, key)
	if e != nil {
		return e
	}
	up.mu.Lock()
	closing := up.completing || up.gone || up.done != nil
	held := up.size(n)
	up.mu.Unlock()
	if closing {
		return errNoSuchUpload
	}
	release, e := acquire(s.parts)
	if e != nil {
		return e
	}
	defer release()
	limit := min(s.cfg.Limits.MaxPart, s.cfg.Limits.MaxUpload-held)
	if limit < 0 {
		limit = 0
	}
	bd, e := s.openBody(r, a, limit)
	if e != nil {
		return e
	}
	defer bd.hashes.sync()
	if !s.cfg.ETagMD5 {
		bd.hashes.needSHA256()
	}
	part, pw, closed, err := up.openPart(n, bd.length)
	if err != nil {
		return errNoSuchUpload
	}
	defer closed()
	kept := false
	defer func() {
		if !kept {
			up.discard(n, part)
		}
	}()
	// Blobs are read straight into the part writer's buffers, and every
	// digest takes each blob from there; a part kept in one file streams
	// through the digests instead.
	src := io.LimitReader(bd.r, bd.length+1)
	if w, ok := pw.(*partWriter); ok {
		w.tee = bd.hashes.feed
		src = io.LimitReader(bd.src, bd.length+1)
	}
	if _, err := io.Copy(pw, src); err != nil {
		pw.abort()
		return bodyError(err)
	}
	hashes, segs, err := pw.finish()
	if err != nil {
		s.opts.Logf("s3d: upload %s part %d: %v", up.ID, n, err)
		return errInternal
	}
	if e := bd.check(); e != nil {
		return e
	}
	if s.cfg.ETagMD5 {
		part.fill(bd.hashes.n, bd.hashes.md5Hex(), "", hashes, segs)
	} else {
		part.fill(bd.hashes.n, "", bd.hashes.sha256Hex(), hashes, segs)
	}
	// From here keep owns the part's files.
	kept = true
	if e := up.keep(n, part, pw, s.cfg.Limits.MaxUpload); e != nil {
		return e
	}
	s.metrics.add("bstore_s3d_part_bytes_total", bd.hashes.n)
	bd.hashes.responseHeaders(w.Header())
	w.Header().Set("ETag", `"`+part.etag()+`"`)
	w.WriteHeader(http.StatusOK)
	return nil
}

// keep records part n, its files written to sink, in place of any part n
// held before, unless the upload is closing or the part would take it past
// max bytes; a part refused is removed. The sink's syncs make the files
// durable, and its state is the content hash's after the part, when the
// part took the hash on inline.
//
// The record is written to a temporary file and synced at once with the
// syncs, before the lock is taken, and renamed into place under it: parts
// landing together do not wait on each other's syncs, nor the content
// hash on theirs.
func (up *upload) keep(n int, part partRecord, sink partSink, max int64) *s3Error {
	refuse := func(e *s3Error) *s3Error {
		up.discard(n, part)
		return e
	}
	part.state = sink.state()
	raw, _ := json.Marshal(part)
	var tmp string
	err := parallel(16, append(sink.syncs(), func() error {
		var err error
		tmp, err = writeSyncedTemp(up.dir, raw)
		return err
	}))
	if tmp != "" {
		defer os.Remove(tmp)
	}
	if err != nil {
		return refuse(errInternal)
	}
	up.mu.Lock()
	if up.completing || up.gone || up.done != nil {
		up.mu.Unlock()
		return refuse(errNoSuchUpload)
	}
	// Checked again under the lock: parts sent at once cannot together
	// pass the upload's bound.
	if up.size(n)+part.Size > max {
		up.mu.Unlock()
		return refuse(errTooLarge)
	}
	if err := os.Rename(tmp, up.partPath(n)+".json"); err != nil {
		up.mu.Unlock()
		return refuse(errInternal)
	}
	old, replaced := up.parts[n]
	up.gens++
	part.gen, part.fresh = up.gens, true
	up.parts[n] = part
	up.dropFrom(n)
	// The content hash the part took on inline follows parts 1 to n-1 as
	// they stand: nothing below was replaced while it ran, or the states
	// were dropped and the goroutine reads the part back.
	if part.state != nil && len(up.run.states) == n-1 {
		up.run.states = append(up.run.states, part.state)
	}
	up.kick()
	up.mu.Unlock()
	// The record's new name, and the part's directory, are durable before
	// the part is acknowledged.
	if err := syncDir(up.dir); err != nil {
		return errInternal
	}
	// The part this one replaced is no longer named.
	if replaced {
		up.discard(n, old)
	}
	return nil
}

func (s *Server) abortUpload(w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key string) *s3Error {
	up, e := s.lookupUpload(r, a, b, key)
	if e != nil {
		return e
	}
	up.mu.Lock()
	if up.completing || up.done != nil {
		up.mu.Unlock()
		return &s3Error{"OperationAborted", "The upload is being completed, or was.", http.StatusConflict}
	}
	up.gone = true
	up.mu.Unlock()
	s.uploads.remove(up)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// completeUpload assembles the parts named, in order, into one closure and
// publishes one entry whose ETag is S3's multipart form. The answer begins
// at once and is kept alive with whitespace while the gateway works, as S3
// does; a failure after that is an error document in a 200.
func (s *Server) completeUpload(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key, reqID string) *s3Error {
	if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Match") != "" {
		return errConditional
	}
	up, e := s.lookupUpload(r, a, b, key)
	if e != nil {
		return e
	}
	up.mu.Lock()
	prior := up.done
	up.mu.Unlock()
	if prior != nil {
		// Its entry was made: the same answer, nothing published again.
		writeXML(w, http.StatusOK, completeResult{NS: s3NS, Location: prior.Location, Bucket: b.name, Key: key,
			ETag: `"` + prior.ETag + `"`, ChecksumSHA256: prior.SHA256, ChecksumType: "FULL_OBJECT"})
		return nil
	}
	// A completion's x-amz-checksum-* headers are the whole object's
	// (FULL_OBJECT), checked against the assembled object below, never
	// against the request's XML. A composite value (one with a part count)
	// is not checked.
	whole := http.Header{}
	body := r.Header.Clone()
	for name := range checksumAlgos {
		hn := "X-Amz-Checksum-" + name
		if v := r.Header.Get(hn); v != "" {
			body.Del(hn)
			if !strings.Contains(v, "-") {
				whole.Set(hn, v)
			}
		}
	}
	body.Del("X-Amz-Sdk-Checksum-Algorithm")
	rb := *r
	rb.Header = body
	bd, e := s.openBody(&rb, a, 4<<20)
	if e != nil {
		return e
	}
	defer bd.hashes.sync()
	raw, err := io.ReadAll(io.LimitReader(bd.r, 4<<20+1))
	if err != nil {
		return bodyError(err)
	}
	if len(raw) > 4<<20 {
		return errMalformedXML
	}
	if e := bd.check(); e != nil {
		return e
	}
	var req completeMultipart
	if xml.Unmarshal(raw, &req) != nil || len(req.Parts) == 0 {
		return errMalformedXML
	}
	if len(req.Parts) > s.cfg.Limits.MaxParts {
		return errMalformedXML
	}
	up.mu.Lock()
	if up.completing || up.gone || up.done != nil {
		up.mu.Unlock()
		return &s3Error{"OperationAborted", "The upload is being completed.", http.StatusConflict}
	}
	up.completing = true
	up.mu.Unlock()
	done := false
	defer func() {
		if !done {
			up.mu.Lock()
			up.completing = false
			up.mu.Unlock()
		}
	}()
	up.mu.Lock()
	up.loadParts()
	held := maps.Clone(up.parts)
	up.mu.Unlock()
	names, recs, total, e := pickParts(req.Parts, held)
	if e != nil {
		return e
	}
	if total > s.cfg.Limits.MaxUpload {
		return errTooLarge
	}
	// The object is assembled under a stage slot and published under a
	// write slot, as a put is staged and published.
	stageSlot, e := acquire(s.stages)
	if e != nil {
		return e
	}
	stageDone := sync.OnceFunc(stageSlot)
	defer stageDone()

	// From here the answer is a 200 kept alive.
	var wmu sync.Mutex
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	rc := http.NewResponseController(w)
	_ = rc.Flush()
	stop := make(chan struct{})
	var kw sync.WaitGroup
	kw.Add(1)
	go func() {
		defer kw.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				wmu.Lock()
				_ = rc.SetWriteDeadline(time.Now().Add(time.Minute))
				_, _ = w.Write([]byte(" "))
				_ = rc.Flush()
				wmu.Unlock()
			}
		}
	}()
	finish := func(v any) {
		close(stop)
		kw.Wait()
		body, _ := xml.Marshal(v)
		_ = rc.SetWriteDeadline(time.Now().Add(time.Minute))
		_, _ = w.Write(body)
	}
	fail := func(e *s3Error) *s3Error {
		finish(xmlError{Code: e.Code, Message: e.Message, Resource: r.URL.Path, RequestID: reqID})
		return nil
	}
	staged, bad, err := s.assemble(context.WithoutCancel(ctx), up, names, recs, uint64(total), whole)
	if err != nil {
		s.opts.Logf("s3d: completing upload %s: %v", up.ID, err)
		return fail(errInternal)
	}
	if staged != nil {
		defer staged.Release()
	}
	if bad != nil {
		return fail(bad)
	}
	if staged.Size != uint64(total) {
		return fail(&s3Error{"InvalidPart", "A part changed on disk.", http.StatusBadRequest})
	}
	stageDone()
	release, e := acquire(s.writes)
	if e != nil {
		return fail(e)
	}
	defer release()
	etag := uploadETag(s.cfg.ETagMD5, recs, staged.ContentHash)
	rec, e := s.publishPut(ctx, a, b, key, up.ContentType, up.Meta, &etag, publish.Object{Root: staged.Root, Size: staged.Size,
		ContentHash: staged.ContentHash, ProofRoot: staged.ProofRoot, Closure: publish.StoreClosure{Store: s.store, Manifest: staged.Manifest, Staged: true}})
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	location := scheme + "://" + r.Host + "/" + b.name + "/" + urlEncodeKey(key)
	if rec != nil {
		// The entry is made (sent, or in the outbox): a completion sent
		// again answers this, and publishes nothing.
		c := &completedRec{Location: location, ETag: etag, SHA256: base64Std(rec.Entry.ContentHash[:])}
		raw, _ := json.Marshal(c)
		_ = writeSynced(filepath.Join(up.dir, "completed.json"), raw)
		_ = os.RemoveAll(filepath.Join(up.dir, "parts"))
		for i, n := range names {
			up.discard(n, recs[i])
		}
		up.mu.Lock()
		up.done, up.completing = c, false
		up.run.states = nil
		up.mu.Unlock()
		done = true
	}
	if e != nil {
		return fail(e)
	}
	s.metrics.add("bstore_s3d_bytes_in_total", int64(staged.Size))
	finish(completeResult{NS: s3NS, Location: location, Bucket: b.name, Key: key,
		ETag: `"` + etag + `"`, ChecksumSHA256: base64Std(rec.Entry.ContentHash[:]), ChecksumType: "FULL_OBJECT"})
	return nil
}

// pickParts matches a completion's parts, in order, to the parts held by
// the ETags they were given: their numbers, records and total size.
func pickParts(parts []completePart, held map[int]partRecord) (names []int, recs []partRecord, total int64, e *s3Error) {
	prev := 0
	for i, p := range parts {
		if p.PartNumber <= prev {
			return nil, nil, 0, &s3Error{"InvalidPartOrder", "The list of parts was not in ascending order. The parts list must be specified in order by part number.", http.StatusBadRequest}
		}
		prev = p.PartNumber
		rec, ok := held[p.PartNumber]
		if !ok || strings.Trim(p.ETag, `"`) != rec.etag() {
			return nil, nil, 0, &s3Error{"InvalidPart", "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.", http.StatusBadRequest}
		}
		if i < len(parts)-1 && rec.Size < minPart {
			return nil, nil, 0, &s3Error{"EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.", http.StatusBadRequest}
		}
		names = append(names, p.PartNumber)
		recs = append(recs, rec)
		total += rec.Size
	}
	return names, recs, total, nil
}

// assemble stages the parts named, in order, as one object: from the
// blobs kept as they arrived when every part but the last is a whole
// number of blobs, else by reading the parts. Either way the closure is
// the one a put of the same bytes stages. A staged object comes back
// even with an s3Error (a whole-object checksum that does not match), for
// the caller to release.
func (s *Server) assemble(ctx context.Context, up *upload, names []int, recs []partRecord, total uint64, whole http.Header) (*publish.Staged, *s3Error, error) {
	linked := true
	var files []string
	for i, n := range names {
		linked = linked && recs[i].staged(i == len(names)-1)
		files = append(files, up.files(n, recs[i])...)
	}
	if linked {
		staged, bad, err := s.stageLinked(ctx, up, names, recs, whole)
		if err == nil {
			return staged, bad, nil
		}
		// The same bytes either way: read them instead.
		s.opts.Logf("s3d: completing upload %s from its blobs: %v; reading the parts", up.ID, err)
	}
	return s.stageRead(ctx, files, total, whole)
}

// stageRead stages the content read from files in order, as a put stages
// its body, and checks the whole object's checksums the completion
// declared.
func (s *Server) stageRead(ctx context.Context, files []string, total uint64, whole http.Header) (*publish.Staged, *s3Error, error) {
	sums := newSums(whole)
	defer sums.sync()
	sums.sha256Staged()
	staged, err := s.store.StageTee(ctx, &partsReader{files: files}, total, sums.feed)
	if err != nil {
		return nil, nil, err
	}
	sums.setSHA256(staged.ContentHash)
	return staged, sums.check(whole, nil), nil
}

// stageLinked stages the content from its parts' blobs as they were kept
// and hashed when they arrived. It reads the content again only for what
// the running content hash has not covered, and for the whole object's
// checksums when the completion declares any. Every part but the last is
// a whole number of blobs.
func (s *Server) stageLinked(ctx context.Context, up *upload, names []int, recs []partRecord, whole http.Header) (*publish.Staged, *s3Error, error) {
	var blobs []publish.LinkedBlob
	for _, rec := range recs {
		b, err := up.linked(rec)
		if err != nil {
			return nil, nil, err
		}
		blobs = append(blobs, b...)
	}
	sums := newSums(whole)
	defer sums.sync()
	sums.sha256Staged()
	var serr error
	var wg sync.WaitGroup
	if sums.any() {
		var files []string
		for i, n := range names {
			files = append(files, up.files(n, recs[i])...)
		}
		wg.Go(func() { serr = hashFiles(sums, files) })
	}
	ch, err := up.contentHash(names, recs)
	wg.Wait()
	if err == nil {
		err = serr
	}
	if err != nil {
		return nil, nil, err
	}
	sums.setSHA256(ch)
	staged, err := s.store.StageLinked(ctx, blobs, ch)
	if err != nil {
		return nil, nil, err
	}
	return staged, sums.check(whole, nil), nil
}

// partsReader reads part files in order, one open at a time.
type partsReader struct {
	files []string
	cur   *os.File
}

func (p *partsReader) Read(b []byte) (int, error) {
	for {
		if p.cur == nil {
			if len(p.files) == 0 {
				return 0, io.EOF
			}
			f, err := os.Open(p.files[0])
			if err != nil {
				return 0, err
			}
			p.cur, p.files = f, p.files[1:]
		}
		n, err := p.cur.Read(b)
		if errors.Is(err, io.EOF) {
			p.cur.Close()
			p.cur = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

// writeSyncedTemp writes b to a new temporary file in dir, synced, and
// returns its path.
func writeSyncedTemp(dir string, b []byte) (string, error) {
	f, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeSynced(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
