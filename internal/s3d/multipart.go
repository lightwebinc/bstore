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
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/storerec"
)

// Multipart uploads (spec section 16.2) are the gateway's own state: the
// parts are kept on disk as they arrive and assembled into one closure at
// completion, re-chunked into 4 MiB blobs, with one entry. An upload
// belongs to the gateway and the access key that began it.

const minPart = 5 << 20

type uploads struct {
	dir string
	now func() time.Time
	mu  sync.Mutex
	m   map[string]*upload
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
}

// completedRec is a completion whose entry was made, kept so that the
// same completion sent again is answered the same and publishes nothing.
type completedRec struct {
	Location string `json:"location"`
	ETag     string `json:"etag"`
	SHA256   string `json:"sha256"`
}

type partRecord struct {
	Size int64  `json:"size"`
	MD5  string `json:"md5"`
}

func openUploads(dir string, now func() time.Time) (*uploads, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	u := &uploads{dir: dir, now: now, m: map[string]*upload{}}
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
		if raw, err := os.ReadFile(filepath.Join(up.dir, "completed.json")); err == nil {
			var c completedRec
			if json.Unmarshal(raw, &c) == nil {
				up.done = &c
			}
		}
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
			up.parts[k] = *p
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
	f, err := os.CreateTemp(up.dir, "tmp-*")
	if err != nil {
		return errNoSuchUpload
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_, err = io.Copy(f, io.LimitReader(bd.r, bd.length+1))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return bodyError(err)
	}
	if e := bd.check(); e != nil {
		return e
	}
	md5h := bd.hashes.md5Hex()
	rec, _ := json.Marshal(partRecord{Size: bd.hashes.n, MD5: md5h})
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.completing || up.gone || up.done != nil {
		return errNoSuchUpload
	}
	// Checked again under the lock: parts sent at once cannot together
	// pass the upload's bound.
	if up.size(n)+bd.hashes.n > s.cfg.Limits.MaxUpload {
		return errTooLarge
	}
	if err := os.Rename(tmp, up.partPath(n)); err != nil {
		return errInternal
	}
	if err := writeSynced(up.partPath(n)+".json", rec); err != nil {
		return errInternal
	}
	up.parts[n] = partRecord{Size: bd.hashes.n, MD5: md5h}
	s.metrics.add("bstore_s3d_part_bytes_total", bd.hashes.n)
	bd.hashes.responseHeaders(w.Header())
	w.Header().Set("ETag", `"`+md5h+`"`)
	w.WriteHeader(http.StatusOK)
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
	var md5s [][]byte
	var files []string
	var total int64
	prev := 0
	for i, p := range req.Parts {
		if p.PartNumber <= prev {
			return &s3Error{"InvalidPartOrder", "The list of parts was not in ascending order. The parts list must be specified in order by part number.", http.StatusBadRequest}
		}
		prev = p.PartNumber
		rec, err := up.part(p.PartNumber)
		if err != nil || strings.Trim(p.ETag, `"`) != rec.MD5 {
			return &s3Error{"InvalidPart", "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.", http.StatusBadRequest}
		}
		if i < len(req.Parts)-1 && rec.Size < minPart {
			return &s3Error{"EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.", http.StatusBadRequest}
		}
		m, _ := hex.DecodeString(rec.MD5)
		md5s = append(md5s, m)
		files = append(files, up.partPath(p.PartNumber))
		total += rec.Size
	}
	if total > s.cfg.Limits.MaxUpload {
		return errTooLarge
	}
	release, e := acquire(s.writes)
	if e != nil {
		return e
	}
	defer release()

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
	sums := newBodyHashes(whole, "")
	staged, err := s.store.Stage(context.WithoutCancel(ctx), sums.reader(&partsReader{files: files}), uint64(total))
	if err != nil {
		s.opts.Logf("s3d: completing upload %s: %v", up.ID, err)
		return fail(errInternal)
	}
	defer staged.Release()
	if staged.Size != uint64(total) {
		return fail(&s3Error{"InvalidPart", "A part changed on disk.", http.StatusBadRequest})
	}
	if e := sums.check(whole, nil); e != nil {
		return fail(e)
	}
	etag := partETag(md5s)
	rec, e := s.publishPut(ctx, a, b, key, up.ContentType, up.Meta, &etag, publish.Object{Root: staged.Root, Size: staged.Size,
		ContentHash: staged.ContentHash, ProofRoot: staged.ProofRoot, Closure: publish.StoreClosure{Store: s.store, Manifest: staged.Manifest}})
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
		for _, f := range files {
			_ = os.Remove(f)
		}
		up.mu.Lock()
		up.done, up.completing = c, false
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
