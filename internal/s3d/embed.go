package s3d

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

// ErrShed is a read this gateway refused because it is shedding load or
// cannot prove or fetch the object now: what S3 answers as 503. The caller
// may try again.
var ErrShed = errors.New("gateway: the read was refused for now")

// ErrNoBucket is a read of a bucket the configuration does not name.
var ErrNoBucket = errors.New("gateway: no such bucket")

// ObjectAnswer is what a read answers to a caller in this process: what
// GetObject or HeadObject would have sent, without the HTTP exchange.
//
// Status is 200, 206, 304, 404, 412 or 416. Header carries the object's
// Content-Type, Content-Length, ETag, Last-Modified, Accept-Ranges and
// Content-Range as the status has them. A 200 or 206 of a read that was
// not a head has a body: WriteTo writes it, once. Close gives the read's
// slot back and must be called.
type ObjectAnswer struct {
	Status int
	Header http.Header
	Root   [32]byte // the object's closure root (200, 206, 304)

	ctx     context.Context
	s       *Server
	obj     *reader.Object
	off, n  uint64
	release func()
	wrote   bool
}

// WriteTo writes the answer's body to w: each blob the range overlaps is
// fetched whole and held to its hash before a byte of it is written, and a
// whole object is held to its content hash before its last blob is (spec
// section 15.2). An error before the first byte reached w means nothing
// was sent.
func (a *ObjectAnswer) WriteTo(w io.Writer) (int64, error) {
	if a.obj == nil || a.n == 0 || a.wrote {
		return 0, nil
	}
	a.wrote = true
	cw := &countWriter{w: w}
	if err := a.obj.WriteRange(a.ctx, cw, a.off, a.n); err != nil {
		if e := readError(err); e.Status == http.StatusServiceUnavailable {
			return cw.n, fmt.Errorf("%w: %s (%w)", ErrShed, e.Message, err)
		}
		return cw.n, err
	}
	a.s.metrics.add("bstore_s3d_bytes_out_total", int64(a.n))
	return cw.n, nil
}

// Close ends the read.
func (a *ObjectAnswer) Close() error {
	if a.release != nil {
		a.release()
		a.release = nil
	}
	return nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// ReadObject is GetObject (or HeadObject, when head) for a caller in this
// process: the key is proven and the object opened exactly as for an S3
// request, with no signature, no access key and no request rate bound,
// which are the S3 face's. bucket is the bucket's S3 name. cond carries
// Range and the conditional headers, or is nil.
//
// A key proven absent answers 404. A read refused for now is ErrShed.
func (s *Server) ReadObject(ctx context.Context, bucket, key string, head bool, cond http.Header) (*ObjectAnswer, error) {
	op := "GetObject"
	if head {
		op = "HeadObject"
	}
	a, e := s.readObject(ctx, bucket, key, head, cond)
	if e != nil {
		s.metrics.request(op, e.Status)
		switch {
		case e == errNoSuchBucket:
			return nil, fmt.Errorf("%w: %s", ErrNoBucket, bucket)
		case e.Status == http.StatusNotFound:
			return &ObjectAnswer{Status: http.StatusNotFound, Header: http.Header{}}, nil
		case e.Status == http.StatusServiceUnavailable:
			if e.Code == "SlowDown" {
				s.metrics.add(fmt.Sprintf("bstore_s3d_slowdown_total{reason=%q}", slowReason(e)), 1)
			}
			return nil, fmt.Errorf("%w: %s", ErrShed, e.Message)
		}
		return nil, errors.New("gateway: " + e.Error())
	}
	s.metrics.request(op, a.Status)
	return a, nil
}

func (s *Server) readObject(ctx context.Context, name, key string, head bool, cond http.Header) (*ObjectAnswer, *s3Error) {
	b := s.buckets[name]
	if b == nil {
		return nil, errNoSuchBucket
	}
	if err := storerec.CheckObjectKey(key); err != nil {
		return nil, errNoSuchKey // no object can have this key
	}
	res, e := s.resolve(ctx, b, key)
	if e != nil {
		return nil, e
	}
	return s.readEntry(ctx, res.entry, head, cond)
}

// readEntry answers a read of ent: serveObject's rules, for a caller in
// this process.
func (s *Server) readEntry(ctx context.Context, ent *storerec.Entry, head bool, cond http.Header) (*ObjectAnswer, *s3Error) {
	if cond == nil {
		cond = http.Header{}
	}
	a := &ObjectAnswer{Header: http.Header{}, Root: ent.Root, ctx: ctx, s: s}
	h := a.Header
	if e := conditional(&http.Request{Header: cond}, ent); e != nil {
		// 304 and 412 carry the validators and no body.
		h.Set("ETag", etagOf(ent))
		h.Set("Last-Modified", httpTime(ent.Created))
		a.Status = e.Status
		return a, nil
	}
	off, n, partial, rerr := parseRange(cond.Get("Range"), ent.Size)
	if rerr != nil {
		h.Set("Content-Range", fmt.Sprintf("bytes */%d", ent.Size))
		a.Status = rerr.Status
		return a, nil
	}
	h.Set("ETag", etagOf(ent))
	h.Set("Last-Modified", httpTime(ent.Created))
	h.Set("Content-Type", ent.ContentType)
	h.Set("Accept-Ranges", "bytes")
	a.Status = http.StatusOK
	if partial {
		a.Status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, off+n-1, ent.Size))
	}
	h.Set("Content-Length", strconv.FormatUint(n, 10))
	if head {
		return a, nil
	}
	release, e := acquire(s.reads)
	if e != nil {
		return nil, e
	}
	obj, err := s.reader.Open(ctx, ent, s.readLocal())
	if err != nil {
		release()
		return nil, readError(err)
	}
	a.obj, a.off, a.n, a.release = obj, off, n, release
	return a, nil
}

// BucketNames are the S3 names of the buckets served, sorted.
func (s *Server) BucketNames() []string { return append([]string(nil), s.order...) }
