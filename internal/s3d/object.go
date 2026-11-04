package s3d

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

// resolved is what a key names for a reader at this gateway: an entry,
// anchored or the gateway's own pending one.
type resolved struct {
	entry   *storerec.Entry
	pending bool
}

// resolve proves what key names (spec section 15.2 steps 1 to 5) and
// applies the gateway's own pending entries over it (spec section 16.3): a
// pending put or delete newer than the anchored entry decides; one the
// anchored entry equals or follows is dropped.
func (s *Server) resolve(ctx context.Context, b *bucket, key string) (*resolved, *s3Error) {
	done, e := acquire(s.lookups)
	if e != nil {
		return nil, e
	}
	f, err := s.reader.Object(ctx, b.chain, key)
	done()
	p := s.st.newest(b.chain.Bucket, key)
	if err != nil {
		// No host proves anything. The gateway's own pending put is still
		// its own write to read; a pending delete is not NoSuchKey, which
		// needs a proven absence or an anchored entry the delete is newer
		// than (frozen.md section 14, row C16).
		if p != nil && p.Kind == storerec.KindPut && p.decoded() != nil {
			return &resolved{entry: p.decoded(), pending: true}, nil
		}
		return nil, readError(err)
	}
	if f.Result == storerec.ObjectFound {
		e := f.Entry
		c := hex.EncodeToString(e.C[:])
		if p != nil && newer(p.Created, p.C, e.Entry.Created, c) {
			if p.Kind == storerec.KindDelete {
				return nil, errNoSuchKey
			}
			if pe := p.decoded(); pe != nil {
				return &resolved{entry: pe, pending: true}, nil
			}
		}
		if p != nil {
			s.st.settle(b.chain.Bucket, key, e.Entry.Created, c)
		}
		return &resolved{entry: e.Entry}, nil
	}
	// Proven absent.
	if p != nil && p.Kind == storerec.KindPut {
		if pe := p.decoded(); pe != nil {
			return &resolved{entry: pe, pending: true}, nil
		}
	}
	return nil, errNoSuchKey
}

// readError maps a reader's failure to an S3 error: 503 SlowDown when no
// host answers or the map names an entry no host produces (never
// NoSuchKey, spec section 16.3); 500 when what verifies names the owner or
// the writer.
func readError(err error) *s3Error {
	switch {
	case errors.Is(err, reader.ErrRefused), errors.Is(err, reader.ErrContentHash):
		return &s3Error{"InternalError", "bstore refused the object: " + err.Error(), http.StatusInternalServerError}
	case errors.Is(err, reader.ErrNoSource):
		return &s3Error{"SlowDown", "No provider served the object's bytes; try again.", http.StatusServiceUnavailable}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return &s3Error{"RequestTimeout", "The request timed out.", http.StatusServiceUnavailable}
	}
	return &s3Error{"SlowDown", "No host proves the key now; try again.", http.StatusServiceUnavailable}
}

// etagOf is the entry's ETag (spec section 16.3), quoted.
func etagOf(e *storerec.Entry) string {
	if e.ETag != nil {
		return `"` + *e.ETag + `"`
	}
	return `"` + hex.EncodeToString(e.Root[:]) + `-1"`
}

func (s *Server) objectHeaders(h http.Header, e *storerec.Entry) {
	h.Set("ETag", etagOf(e))
	h.Set("Last-Modified", httpTime(e.Created))
	h.Set("Content-Type", e.ContentType)
	h.Set("Accept-Ranges", "bytes")
	// User metadata goes out in lower case, as S3 sends it: clients keep
	// the case of what follows the prefix.
	h["x-amz-meta-bstore-root"] = []string{hex.EncodeToString(e.Root[:])}
	for _, m := range e.Meta {
		h["x-amz-meta-"+m.Key] = []string{m.Value}
	}
}

// getObject is GetObject and HeadObject. A head checks steps 1 to 5 and
// never asks a provider; a get reads the blobs a range overlaps, each held
// to its hash before a byte of it is sent (spec section 15.2).
func (s *Server) getObject(ctx context.Context, w http.ResponseWriter, r *http.Request, b *bucket, key string) *s3Error {
	for name := range r.URL.Query() {
		if strings.HasPrefix(name, "response-") {
			switch name {
			case "response-content-type", "response-content-language", "response-expires", "response-cache-control",
				"response-content-disposition", "response-content-encoding":
			default:
				return invalidArgument("unknown response override " + name)
			}
		}
	}
	if r.Header.Get("X-Amz-Checksum-Mode") != "" && !strings.EqualFold(r.Header.Get("X-Amz-Checksum-Mode"), "ENABLED") {
		return invalidArgument("x-amz-checksum-mode is ENABLED or absent")
	}
	if r.URL.Query().Get("partNumber") != "" {
		return notImplemented("reading by part number")
	}
	res, e := s.resolve(ctx, b, key)
	if e != nil {
		return e
	}
	return s.serveObject(ctx, w, r, b, key, res.entry)
}

// serveObject answers a read of ent.
func (s *Server) serveObject(ctx context.Context, w http.ResponseWriter, r *http.Request, b *bucket, key string, ent *storerec.Entry) *s3Error {
	if e := conditional(r, ent); e != nil {
		if e.Status == http.StatusNotModified {
			w.Header().Set("ETag", etagOf(ent))
			w.Header().Set("Last-Modified", httpTime(ent.Created))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		return e
	}
	off, n, partial, rerr := parseRange(r.Header.Get("Range"), ent.Size)
	if rerr != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", ent.Size))
		return rerr
	}
	h := w.Header()
	// A read that fails from here on answers as an error, without the
	// object's headers: a client holds a checksum header to the body it came
	// with, and the AWS CLI dies on an error body that carries one where it
	// should retry.
	before := h.Clone()
	fail := func(e *s3Error) *s3Error {
		clear(h)
		maps.Copy(h, before)
		return e
	}
	s.objectHeaders(h, ent)
	for _, o := range []struct{ q, h string }{{"response-content-type", "Content-Type"}, {"response-content-language", "Content-Language"},
		{"response-expires", "Expires"}, {"response-cache-control", "Cache-Control"},
		{"response-content-disposition", "Content-Disposition"}, {"response-content-encoding", "Content-Encoding"}} {
		if v := r.URL.Query().Get(o.q); v != "" {
			h.Set(o.h, v)
		}
	}
	if !partial {
		// The full object's SHA-256 (spec section 16.3). A partial answer
		// carries none: it would describe bytes the client did not get.
		h.Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(ent.ContentHash[:]))
		h.Set("x-amz-checksum-type", "FULL_OBJECT")
	}
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, off+n-1, ent.Size))
	}
	h.Set("Content-Length", strconv.FormatUint(n, 10))
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return nil
	}
	release, e := acquire(s.reads)
	if e != nil {
		return fail(e)
	}
	defer release()
	obj, err := s.reader.Open(ctx, ent, s.store)
	if err != nil {
		return fail(readError(err))
	}
	if n == 0 {
		w.WriteHeader(status)
		return nil
	}
	iw := &idleWriter{w: w, rc: http.NewResponseController(w), idle: time.Duration(s.cfg.Limits.IdleSeconds) * time.Second,
		before: func() { w.WriteHeader(status) }}
	if err := obj.WriteRange(ctx, iw, off, n); err != nil {
		if !iw.started {
			return fail(readError(err))
		}
		s.opts.Logf("s3d: %s/%s: the answer was cut short: %v", b.name, key, err)
		panic(http.ErrAbortHandler)
	}
	s.metrics.add("bstore_s3d_bytes_out_total", int64(n))
	return nil
}

// idleWriter sends the status line before the first byte, and holds each
// write to the idle bound.
type idleWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	idle    time.Duration
	before  func()
	started bool
}

func (i *idleWriter) Write(p []byte) (int, error) {
	if !i.started {
		i.started = true
		i.before()
	}
	_ = i.rc.SetWriteDeadline(time.Now().Add(i.idle))
	return i.w.Write(p)
}

// conditional applies a read's If-Match, If-None-Match,
// If-Modified-Since and If-Unmodified-Since, as HTTP has them.
func conditional(r *http.Request, e *storerec.Entry) *s3Error {
	etag := etagOf(e)
	mod := time.UnixMilli(int64(e.Created)).UTC().Truncate(time.Second)
	match := func(list string) bool {
		for _, t := range strings.Split(list, ",") {
			t = strings.TrimSpace(t)
			if t == "*" || t == etag || strings.TrimPrefix(t, "W/") == etag {
				return true
			}
		}
		return false
	}
	if v := r.Header.Get("If-Match"); v != "" {
		if !match(v) {
			return &s3Error{"PreconditionFailed", "At least one of the pre-conditions you specified did not hold", http.StatusPreconditionFailed}
		}
	} else if v := r.Header.Get("If-Unmodified-Since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && mod.After(t) {
			return &s3Error{"PreconditionFailed", "At least one of the pre-conditions you specified did not hold", http.StatusPreconditionFailed}
		}
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		if match(v) {
			return &s3Error{"NotModified", "Not Modified", http.StatusNotModified}
		}
	} else if v := r.Header.Get("If-Modified-Since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && !mod.After(t) {
			return &s3Error{"NotModified", "Not Modified", http.StatusNotModified}
		}
	}
	return nil
}

var errRange = &s3Error{"InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable}

// parseRange reads one byte range of an object of size: off and n, and
// whether the answer is partial. A header that does not parse, or holds
// several ranges, is ignored, as HTTP lets a server do; one that parses
// and lies past the end is 416.
func parseRange(v string, size uint64) (uint64, uint64, bool, *s3Error) {
	if v == "" {
		return 0, size, false, nil
	}
	spec, ok := strings.CutPrefix(v, "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, size, false, nil
	}
	a, z, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return 0, size, false, nil
	}
	if a == "" {
		// A suffix: the last z bytes.
		k, err := strconv.ParseUint(z, 10, 64)
		if err != nil {
			return 0, size, false, nil
		}
		if k == 0 || size == 0 {
			return 0, 0, false, errRange
		}
		if k > size {
			k = size
		}
		return size - k, k, true, nil
	}
	first, err := strconv.ParseUint(a, 10, 64)
	if err != nil {
		return 0, size, false, nil
	}
	last := uint64(0)
	if z == "" {
		if size == 0 {
			return 0, 0, false, errRange
		}
		last = size - 1
	} else {
		if last, err = strconv.ParseUint(z, 10, 64); err != nil || last < first {
			return 0, size, false, nil
		}
	}
	if first >= size {
		return 0, 0, false, errRange
	}
	if last >= size {
		last = size - 1
	}
	return first, last - first + 1, true, nil
}

// refuseUnsupported answers NotImplemented for every write header this
// gateway cannot honour: a client that sets one must not believe it took.
func refuseUnsupported(h http.Header) *s3Error {
	if h.Get("If-None-Match") != "" || h.Get("If-Match") != "" {
		return errConditional
	}
	for name := range h {
		l := strings.ToLower(name)
		switch {
		case strings.HasPrefix(l, "x-amz-server-side-encryption"):
			return notImplemented("server-side encryption")
		case strings.HasPrefix(l, "x-amz-object-lock-"):
			return notImplemented("object lock")
		case l == "x-amz-tagging" || l == "x-amz-tagging-directive" && !strings.EqualFold(h.Get(name), "COPY"):
			return notImplemented("object tagging")
		case l == "x-amz-website-redirect-location":
			return notImplemented("website redirects")
		case strings.HasPrefix(l, "x-amz-grant-"):
			return notImplemented("ACLs")
		case l == "x-amz-acl" && h.Get(name) != "private":
			return notImplemented("ACLs other than private")
		case l == "x-amz-storage-class" && h.Get(name) != "STANDARD":
			return notImplemented("storage classes other than STANDARD")
		case l == "x-amz-write-offset-bytes":
			return notImplemented("appends")
		}
	}
	return nil
}

// entryMeta is a put's content type and user metadata (spec sections 4.1
// and 16.3): the content type lowercased without parameters, else
// application/octet-stream; metadata keys lowercased, bstore- reserved.
func entryMeta(h http.Header) (string, []storerec.MetaPair, *s3Error) {
	ct := strings.ToLower(strings.TrimSpace(h.Get("Content-Type")))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	if storerec.CheckContentType(ct) != nil {
		ct = "application/octet-stream"
	}
	var meta []storerec.MetaPair
	for name, vals := range h {
		l := strings.ToLower(name)
		k, ok := strings.CutPrefix(l, "x-amz-meta-")
		if !ok {
			continue
		}
		if strings.HasPrefix(k, "bstore-") {
			return "", nil, invalidArgument("user metadata keys starting bstore- are reserved")
		}
		if !storerec.CheckMetaKey(k) {
			return "", nil, invalidArgument("a user metadata key is 1 to 128 of a-z, 0-9 and -: " + k)
		}
		v := strings.Join(vals, ",")
		if len(v) > storerec.MaxMetaValue || !storerec.OneLine(v) {
			return "", nil, invalidArgument("a user metadata value is at most 1024 bytes on one line")
		}
		meta = append(meta, storerec.MetaPair{Key: k, Value: v})
	}
	if len(meta) > storerec.MaxMetaPairs {
		return "", nil, &s3Error{"MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.", http.StatusBadRequest}
	}
	sort.Slice(meta, func(i, j int) bool { return meta[i].Key < meta[j].Key })
	if len(meta) == 0 {
		meta = nil
	}
	return ct, meta, nil
}

// body is a write's body as it streams: decoded from aws-chunked when it is
// so encoded, every digest computed, and check to call at its end, before
// anything is published.
type body struct {
	r      io.Reader
	hashes *bodyHashes
	chunks *chunkReader
	length int64
	check  func() *s3Error
}

// openBody prepares a write's body of at most max bytes (spec section
// 16.1): Content-Length required; aws-chunked bodies decoded, their chunk
// signatures and trailing checksums checked; a signed payload hash
// checked; Content-MD5 checked.
func (s *Server) openBody(r *http.Request, a *Auth, max int64) (*body, *s3Error) {
	length := r.ContentLength
	enc := r.Header.Get("Content-Encoding")
	chunked := strings.HasPrefix(a.Payload, "STREAMING-") || hasToken(enc, "aws-chunked")
	if length < 0 && !chunked {
		return nil, errMissingLength
	}
	if chunked && !strings.HasPrefix(a.Payload, "STREAMING-") {
		return nil, invalidArgument("an aws-chunked body needs a streaming x-amz-content-sha256")
	}
	if chunked {
		n, err := decodedLength(r.Header.Get("X-Amz-Decoded-Content-Length"))
		if err != nil {
			return nil, &s3Error{"MissingContentLength", err.Error(), http.StatusLengthRequired}
		}
		length = n
	}
	if length > max {
		return nil, errTooLarge
	}
	var md5want []byte
	if v := r.Header.Get("Content-MD5"); v != "" {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil || len(b) != md5.Size {
			return nil, errInvalidDigest
		}
		md5want = b
	}
	idle := time.Duration(s.cfg.Limits.IdleSeconds) * time.Second
	var raw io.Reader = &idleReader{r: r.Body, rc: responseControllerOf(r), idle: idle, minRate: s.cfg.Limits.MinRate}
	bd := &body{length: length}
	trailer := r.Header.Get("X-Amz-Trailer")
	for _, t := range strings.Split(trailer, ",") {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" {
			continue
		}
		alg, ok := strings.CutPrefix(t, "x-amz-checksum-")
		if _, known := checksumAlgos[alg]; !ok || !known {
			return nil, invalidArgument("x-amz-trailer names a trailer this gateway does not check: " + t)
		}
	}
	if a := strings.ToLower(r.Header.Get("X-Amz-Sdk-Checksum-Algorithm")); a != "" {
		if _, known := checksumAlgos[a]; !known {
			return nil, invalidArgument("x-amz-sdk-checksum-algorithm is not one this gateway checks")
		}
	}
	bd.hashes = newBodyHashes(r.Header, trailer, s.cfg.ETagMD5 || md5want != nil)
	payloadSigned := false
	if chunked {
		signed := a.Payload == streamSigned || a.Payload == streamSignedT
		withTrailer := a.Payload == streamSignedT || a.Payload == streamUnsignT
		if withTrailer != (trailer != "") {
			return nil, invalidArgument("x-amz-trailer and the streaming payload type disagree")
		}
		src := raw
		if r.ContentLength >= 0 {
			src = io.LimitReader(raw, r.ContentLength)
		}
		bd.chunks = newChunkReader(src, a, signed, withTrailer)
		bd.chunks.max = length
		raw = bd.chunks
	} else {
		raw = io.LimitReader(raw, length)
		if payloadSigned = a.Payload != unsigned; payloadSigned {
			bd.hashes.sha256 = sha256.New()
		}
	}
	bd.r = bd.hashes.reader(raw)
	bd.check = func() *s3Error {
		bd.hashes.sync()
		if bd.hashes.n != length {
			return errIncomplete
		}
		if bd.chunks != nil {
			for _, t := range strings.Split(trailer, ",") {
				if t = strings.ToLower(strings.TrimSpace(t)); t != "" {
					if _, ok := bd.chunks.Trailers[t]; !ok {
						return invalidArgument("the trailer " + t + " was declared and not sent")
					}
				}
			}
		}
		if payloadSigned && hex.EncodeToString(bd.hashes.sha256.Sum(nil)) != a.Payload {
			return errSHA256Mismatch
		}
		if md5want != nil && !bytes.Equal(md5want, bd.hashes.md5.Sum(nil)) {
			return errBadDigest
		}
		var tr map[string]string
		if bd.chunks != nil {
			tr = bd.chunks.Trailers
		}
		return bd.hashes.check(r.Header, tr)
	}
	return bd, nil
}

func hasToken(list, tok string) bool {
	for _, t := range strings.Split(list, ",") {
		if strings.EqualFold(strings.TrimSpace(t), tok) {
			return true
		}
	}
	return false
}

// bodyError maps a failure reading a body.
func bodyError(err error) *s3Error {
	switch {
	case errors.Is(err, publish.ErrTooLarge):
		return errTooLarge
	case errors.Is(err, errChunkSig), errors.Is(err, errTrailerSg):
		return &s3Error{"SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", http.StatusForbidden}
	case errors.Is(err, errChunk), errors.Is(err, errTrailer):
		return &s3Error{"InvalidRequest", "The aws-chunked body is malformed.", http.StatusBadRequest}
	case errors.Is(err, io.ErrUnexpectedEOF):
		return errIncomplete
	case errors.Is(err, errTooSlow):
		return &s3Error{"RequestTimeout", "Your socket connection to the server was not read from or written to within the timeout period.", http.StatusBadRequest}
	}
	return &s3Error{"RequestTimeout", "Your socket connection to the server was not read from or written to within the timeout period.", http.StatusBadRequest}
}

// putObject is PutObject (spec section 16.2): the body staged into its
// closure while it streams, every digest checked, then published by the
// access key's identity; 200 once the entry is sent.
func (s *Server) putObject(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key string) *s3Error {
	if e := refuseUnsupported(r.Header); e != nil {
		return e
	}
	ct, meta, e := entryMeta(r.Header)
	if e != nil {
		return e
	}
	// The body streams in under a stage slot; a write slot is taken only
	// to publish, so slow bodies cannot hold up publishing.
	stageDone, e := acquire(s.stages)
	if e != nil {
		return e
	}
	bd, e := s.openBody(r, a, s.cfg.Limits.MaxObject)
	if e != nil {
		stageDone()
		return e
	}
	defer bd.hashes.sync()
	staged, err := s.store.Stage(ctx, bd.r, uint64(bd.length))
	stageDone()
	if err != nil {
		return bodyError(err)
	}
	defer staged.Release()
	if e := bd.check(); e != nil {
		return e
	}
	etag := hex.EncodeToString(staged.ContentHash[:])
	if s.cfg.ETagMD5 {
		etag = bd.hashes.md5Hex()
	}
	k := writeKey(b.name, a.Key.Identity, key, ct, meta, etag, staged.Root, staged.Size)
	entry, e := s.oneWrite(ctx, k, func() (*storerec.Entry, *s3Error) {
		release, e := acquire(s.writes)
		if e != nil {
			return nil, e
		}
		defer release()
		s.metrics.add("bstore_s3d_bytes_in_total", int64(staged.Size))
		if prev := s.retryOf(a, b, key, ct, meta, etag, staged.Root, staged.Size); prev != nil {
			// The same write sent again (a retry after a 503): the entry made
			// for it stands, and is still being delivered; no second entry.
			s.metrics.add("bstore_s3d_put_retries_total", 1)
			return prev, nil
		}
		rec, e := s.publishPut(ctx, a, b, key, ct, meta, &etag, publish.Object{Root: staged.Root, Size: staged.Size,
			ContentHash: staged.ContentHash, ProofRoot: staged.ProofRoot, Closure: publish.StoreClosure{Store: s.store, Manifest: staged.Manifest, Staged: true}})
		if e != nil {
			return nil, e
		}
		return rec.Entry, nil
	})
	if e != nil {
		return e
	}
	bd.hashes.responseHeaders(w.Header())
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header()["x-amz-meta-bstore-root"] = []string{hex.EncodeToString(entry.Root[:])}
	w.WriteHeader(http.StatusOK)
	return nil
}

// publishPut publishes a put entry by the key's identity and records it
// pending. Publishing goes on if the client goes away: its carriers are
// already signed.
func (s *Server) publishPut(ctx context.Context, a *Auth, b *bucket, key, ct string, meta []storerec.MetaPair, etag *string, obj publish.Object) (*publish.Receipt, *s3Error) {
	p := s.pubs[a.Key.Identity]
	var bounty *publish.Bounty
	if c := s.cfg.Bounty; c != nil {
		bounty = &publish.Bounty{Satoshis: c.Satoshis, T: c.T, K: c.K, Retention: c.RetentionSeconds, DeadlineBlocks: c.DeadlineBlocks, CheckC: c.CheckC}
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Minute)
	defer cancel()
	rec, err := p.Put(pctx, publish.Fields{Bucket: b.chain.Bucket, ID: b.chain.ID, Key: key, ContentType: ct, ETag: etag, Meta: meta,
		Created: s.opts.Now()}, obj, bounty)
	return s.afterPublish(b, rec, err)
}

func (s *Server) afterPublish(b *bucket, rec *publish.Receipt, err error) (*publish.Receipt, *s3Error) {
	if rec != nil {
		if aerr := s.st.add(rec.Entry, rec.Record, rec.C); aerr != nil {
			s.opts.Logf("s3d: pending entry %s: %v", rec.Txid, aerr)
		}
		s.metrics.add("bstore_s3d_entries_total", 1)
	}
	if err != nil {
		s.opts.Logf("s3d: publish to %s: %v", b.name, err)
		if errors.Is(err, publish.ErrNoHold) {
			return rec, &s3Error{"SlowDown", "No mined hold output is ready for the bounty yet; try again shortly.", http.StatusServiceUnavailable}
		}
		if errors.Is(err, publish.ErrOutboxFull) {
			return rec, &s3Error{"SlowDown", "A provider has not taken what this gateway sent it, and the gateway's outbox is at its bound; try again later.", http.StatusServiceUnavailable}
		}
		if errors.Is(err, publish.ErrNotSent) {
			if rec != nil {
				s.st.markUnsent(hex.EncodeToString(rec.C[:]))
			}
			return rec, &s3Error{"SlowDown", "The entry was made but no host took it yet; it is sent again.", http.StatusServiceUnavailable}
		}
		return nil, &s3Error{"InternalError", "The write could not be published: " + err.Error(), http.StatusInternalServerError}
	}
	return rec, nil
}

// copySource reads x-amz-copy-source: /bucket/key or bucket/key,
// percent-encoded, no version.
func (s *Server) copySource(v string) (*bucket, string, *s3Error) {
	if i := strings.IndexByte(v, '?'); i >= 0 {
		q, _ := url.ParseQuery(v[i+1:])
		if q.Get("versionId") != "" && q.Get("versionId") != "null" {
			return nil, "", notImplemented("copying a version")
		}
		v = v[:i]
	}
	d, err := url.PathUnescape(v)
	if err != nil {
		return nil, "", invalidArgument("x-amz-copy-source is not percent-encoded")
	}
	d = strings.TrimPrefix(d, "/")
	bn, key, ok := strings.Cut(d, "/")
	if !ok || key == "" {
		return nil, "", invalidArgument("x-amz-copy-source is <bucket>/<key>")
	}
	b := s.buckets[bn]
	if b == nil {
		return nil, "", errNoSuchBucket
	}
	if storerec.CheckObjectKey(key) != nil {
		return nil, "", invalidArgument("the copy source's key is not a valid key")
	}
	return b, key, nil
}

// copyObject is CopyObject (spec section 16.2): a new put entry naming the
// source's root, size, content hash and proof root; COPY keeps its content
// type, ETag and metadata, REPLACE takes the request's and keeps the ETag.
// Within one bucket no piece is published again; across buckets the
// closure is published on the destination's blob topic.
func (s *Server) copyObject(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key string) *s3Error {
	if e := refuseUnsupported(r.Header); e != nil {
		return e
	}
	for name := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-copy-source-if-") {
			return notImplemented("conditional copies")
		}
		if strings.HasPrefix(strings.ToLower(name), "x-amz-copy-source-server-side-encryption") {
			return notImplemented("server-side encryption")
		}
	}
	sb, skey, e := s.copySource(r.Header.Get("X-Amz-Copy-Source"))
	if e != nil {
		return e
	}
	if a.Key.Buckets != nil && !a.Key.Buckets[sb.name] {
		return errAccessDenied
	}
	dir := strings.ToUpper(r.Header.Get("X-Amz-Metadata-Directive"))
	if dir == "" {
		dir = "COPY"
	}
	if dir != "COPY" && dir != "REPLACE" {
		return invalidArgument("x-amz-metadata-directive is COPY or REPLACE")
	}
	if sb == b && skey == key && dir == "COPY" {
		return &s3Error{"InvalidRequest", "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata.", http.StatusBadRequest}
	}
	res, e := s.resolve(ctx, sb, skey)
	if e != nil {
		return e
	}
	src := res.entry
	ct, meta := src.ContentType, src.Meta
	if dir == "REPLACE" {
		if ct, meta, e = entryMeta(r.Header); e != nil {
			return e
		}
	}
	etag, et := src.ETag, ""
	if etag != nil {
		et = *etag
	}
	k := writeKey(b.name, a.Key.Identity, key, ct, meta, et, src.Root, src.Size)
	entry, e := s.oneWrite(ctx, k, func() (*storerec.Entry, *s3Error) {
		release, e := acquire(s.writes)
		if e != nil {
			return nil, e
		}
		defer release()
		obj := publish.Object{Root: src.Root, Size: src.Size, ContentHash: src.ContentHash, ProofRoot: src.ProofRoot}
		if sb != b {
			c, e := s.closureOf(ctx, src)
			if e != nil {
				return nil, e
			}
			obj.Closure = c
		}
		rec, e := s.publishPut(ctx, a, b, key, ct, meta, etag, obj)
		if e != nil {
			return nil, e
		}
		return rec.Entry, nil
	})
	if e != nil {
		return e
	}
	writeXML(w, http.StatusOK, copyResult{NS: s3NS, LastModified: isoTime(entry.Created), ETag: etagOf(entry),
		ChecksumSHA256: base64.StdEncoding.EncodeToString(src.ContentHash[:]), ChecksumType: "FULL_OBJECT"})
	return nil
}

// closureOf is an entry's closure for publishing it again: the local copy
// when the gateway holds it, else read from the advertisers, every object
// held to its hash.
func (s *Server) closureOf(ctx context.Context, e *storerec.Entry) (publish.Closure, *s3Error) {
	if m, err := s.store.Manifest(e.Root); err == nil {
		c := publish.StoreClosure{Store: s.store, Manifest: m}
		all := true
		for _, h := range c.Objects() {
			if !s.store.Has(h) {
				all = false
				break
			}
		}
		if all {
			return c, nil
		}
	}
	if e.Size == 0 {
		st, err := s.store.Stage(ctx, bytes.NewReader(nil), 0)
		if err != nil {
			return nil, errInternal
		}
		st.Release()
		return publish.StoreClosure{Store: s.store, Manifest: st.Manifest}, nil
	}
	obj, err := s.reader.Open(ctx, e, s.store)
	if err != nil {
		return nil, readError(err)
	}
	return obj, nil
}

// deleteObject is DeleteObject: a delete entry; 204 (spec section 16.2).
func (s *Server) deleteObject(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key string) *s3Error {
	if r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != "" ||
		r.Header.Get("X-Amz-If-Match-Last-Modified-Time") != "" || r.Header.Get("X-Amz-If-Match-Size") != "" {
		return errConditional
	}
	if r.URL.Query().Get("versionId") != "" {
		return notImplemented("deleting a version")
	}
	release, e := acquire(s.writes)
	if e != nil {
		return e
	}
	defer release()
	if e := s.publishDelete(ctx, a, b, key); e != nil {
		return e
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) publishDelete(ctx context.Context, a *Auth, b *bucket, key string) *s3Error {
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
	defer cancel()
	rec, err := s.pubs[a.Key.Identity].Delete(pctx, b.chain.Bucket, b.chain.ID, key, s.opts.Now())
	_, e := s.afterPublish(b, rec, err)
	return e
}

// deleteObjects is DeleteObjects: a delete entry per key.
func (s *Server) deleteObjects(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket) *s3Error {
	if r.ContentLength > 2<<20 {
		return errMalformedXML
	}
	bd, e := s.openBody(r, a, 2<<20)
	if e != nil {
		return e
	}
	defer bd.hashes.sync()
	raw, err := io.ReadAll(io.LimitReader(bd.r, 2<<20+1))
	if err != nil {
		return bodyError(err)
	}
	if len(raw) > 2<<20 {
		return errMalformedXML
	}
	if e := bd.check(); e != nil {
		return e
	}
	var req deleteRequest
	if xml.Unmarshal(raw, &req) != nil || len(req.Objects) == 0 {
		return errMalformedXML
	}
	if len(req.Objects) > s.cfg.Limits.DeleteKeys {
		return errMalformedXML
	}
	release, e := acquire(s.writes)
	if e != nil {
		return e
	}
	defer release()
	res := deleteResult{NS: s3NS}
	for _, o := range req.Objects {
		switch {
		case o.VersionID != "":
			res.Errors = append(res.Errors, deleteError{Key: o.Key, Code: "NotImplemented", Message: "deleting a version is not implemented"})
		case storerec.CheckObjectKey(o.Key) != nil:
			res.Errors = append(res.Errors, deleteError{Key: o.Key, Code: "InvalidArgument", Message: "not a valid key"})
		default:
			if e := s.publishDelete(ctx, a, b, o.Key); e != nil {
				res.Errors = append(res.Errors, deleteError{Key: o.Key, Code: e.Code, Message: e.Message})
			} else if !req.Quiet {
				res.Deleted = append(res.Deleted, deletedXML{Key: o.Key})
			}
		}
	}
	writeXML(w, http.StatusOK, res)
	return nil
}

// idleReader holds each read of a body to the idle bound, and the body as
// a whole to a least average rate once its first idle period has passed,
// so a body trickled in to hold a slot is cut off.
type idleReader struct {
	r       io.Reader
	rc      *http.ResponseController
	idle    time.Duration
	minRate int64
	start   time.Time
	n       int64
}

var errTooSlow = errors.New("the request body arrives too slowly")

func (i *idleReader) Read(p []byte) (int, error) {
	now := time.Now()
	if i.start.IsZero() {
		i.start = now
	}
	if el := now.Sub(i.start); i.minRate > 0 && el > i.idle && i.n < int64(el.Seconds()*float64(i.minRate)) {
		return 0, errTooSlow
	}
	if i.rc != nil {
		_ = i.rc.SetReadDeadline(now.Add(i.idle))
	}
	n, err := i.r.Read(p)
	i.n += int64(n)
	return n, err
}

type rcKey struct{}

func responseControllerOf(r *http.Request) *http.ResponseController {
	rc, _ := r.Context().Value(rcKey{}).(*http.ResponseController)
	return rc
}

// markUnsent records an entry a put answered 503 for.
func (s *state) markUnsent(c string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unsent == nil {
		s.unsent = map[string]time.Time{}
	}
	now := s.now()
	for k, at := range s.unsent {
		if now.Sub(at) > retryWindow {
			delete(s.unsent, k)
		}
	}
	s.unsent[c] = now
}

func (s *state) wasUnsent(c string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.unsent[c]
	return ok && now.Sub(at) <= retryWindow
}

// retryWindow is how long after a put's entry the same write, sent again,
// is answered by that entry instead of a second one.
const retryWindow = time.Hour

// retryOf is the entry a put made moments ago for exactly this write and
// answered 503 for (made, not yet taken by a quorum): the key's newest
// pending entry at this gateway, made within retryWindow by the same
// identity, of the same content (root and size), content type,
// ETag and metadata. A client that retries a put after a 503 SlowDown
// (whose entry stays in the outbox, to be delivered) gets that entry
// instead of a second one for the same content. Any other write to the key
// in between is newer, and a retry after it makes a new entry, as it must.
func (s *Server) retryOf(a *Auth, b *bucket, key, ct string, meta []storerec.MetaPair, etag string, root [32]byte, size uint64) *storerec.Entry {
	p := s.pubs[a.Key.Identity]
	if p == nil {
		return nil
	}
	return s.st.retryOf(b.chain.Bucket, b.chain.ID, p.Identity(), key, ct, meta, etag, root, size, s.opts.Now())
}

func (s *state) retryOf(bucket string, id [32]byte, writer []byte, key, ct string, meta []storerec.MetaPair, etag string, root [32]byte, size uint64, now time.Time) *storerec.Entry {
	pe := s.newest(bucket, key)
	if pe == nil || now.Sub(time.Unix(pe.Seen, 0)) > retryWindow || !s.wasUnsent(pe.C, now) {
		return nil
	}
	e := pe.decoded()
	if e == nil || e.Kind != storerec.KindPut || e.ID != id || !bytes.Equal(e.Writer, writer) || e.Root != root || e.Size != size ||
		e.ContentType != ct || e.ETag == nil || *e.ETag != etag || !sameMeta(e.Meta, meta) {
		return nil
	}
	return e
}

func sameMeta(a, b []storerec.MetaPair) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := slices.Clone(a), slices.Clone(b)
	cmp := func(p, q storerec.MetaPair) int { return strings.Compare(p.Key, q.Key) }
	slices.SortFunc(x, cmp)
	slices.SortFunc(y, cmp)
	return slices.Equal(x, y)
}
