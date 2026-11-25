// Package gateway runs a bstore gateway inside another program, so that
// program reads objects by a call instead of an S3 request to a gateway
// beside it.
//
// The gateway is the one `bstore s3d run` runs, from the same configuration
// file and data directory (docs/s3.md): every read proves the key against
// the chain's latest token and holds each blob to its hash before a byte of
// it is given, exactly as an S3 read does. What a call leaves out is the S3
// face: the signature, the access key, the request rate bound and the HTTP
// exchange. A configuration is used by one process at a time.
package gateway

import (
	"context"
	"net/http"
	"time"

	"github.com/lightwebinc/bstore/internal/s3d"
)

// ErrShed is a read the gateway refused for now: it is shedding load, or no
// host proves the key, or no provider serves the object's bytes. S3 answers
// these 503. The caller may try again.
var ErrShed = s3d.ErrShed

// ErrNoBucket is a read of a bucket the configuration does not name.
var ErrNoBucket = s3d.ErrNoBucket

// ErrRefused is an object that fails its checks (S3 answers 500): the
// owner's or the writer's fault, and trying again does not help.
var ErrRefused = s3d.ErrRefused

// Answer is a read's answer; see Gateway.Read.
type Answer = s3d.ObjectAnswer

// Options are what the program may set beyond the configuration file.
type Options struct {
	// Logf takes the gateway's log lines; log.Printf when nil.
	Logf func(format string, args ...any)
	// LockWait is how long an identity another process holds is waited for
	// at start.
	LockWait time.Duration
	// S3 also serves the S3 face at the configuration's `listen`. Without
	// it the program is the gateway's only reader; the metrics and profile
	// listeners are served as configured either way.
	S3 bool
}

// Gateway is a gateway running in this process.
type Gateway struct {
	s       *s3d.Server
	buckets []string
}

// Open loads the gateway configuration at path, opens the gateway and
// starts its background work. The identities it names must exist
// (`bstore s3d init`). ctx is the gateway's life, not the opening's: when
// it ends the background work stops, as it does at Close. Pass the
// program's own context, never one with a start-up timeout.
func Open(ctx context.Context, path string, o Options) (*Gateway, error) {
	cfg, err := s3d.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	s, err := s3d.New(ctx, cfg, s3d.Options{Logf: o.Logf, LockWait: o.LockWait, NoListen: !o.S3})
	if err != nil {
		return nil, err
	}
	if err := s.Start(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return &Gateway{s: s, buckets: s.BucketNames()}, nil
}

// Buckets are the S3 names of the buckets the gateway serves, sorted.
func (g *Gateway) Buckets() []string { return append([]string(nil), g.buckets...) }

// Read is GetObject, or HeadObject when head, of key in the bucket with
// the S3 name bucket. cond carries Range and the conditional headers
// (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since) under
// their canonical names, as a request's own Header has them, or is nil.
// ctx is the read's, and bounds WriteTo too.
//
// The answer's Status is 200, 206, 304, 404 (the key is proven absent, or
// no object can have it), 412 or 416. Its Header carries Content-Type,
// Content-Length, ETag, Last-Modified, Accept-Ranges and Content-Range as
// that status has them; user metadata and the S3 checksum headers are not
// given, and the closure root is the answer's Root. A 200 or 206 of a
// read that was not a head has a body and holds one of the gateway's read
// slots (limits.reads) until it is closed: WriteTo writes the body once,
// fetching and checking as it goes, and an error from it before its first
// byte means nothing was written. Close every answer, written or not: an
// answer with a body that is never closed keeps its slot for good.
//
// The error is ErrShed for a read refused for now (a bound, or nothing
// proves the key or serves its bytes at the moment: never a 404),
// ErrRefused for an object that fails its checks, ErrNoBucket for a bucket
// the configuration does not name. Reads past limits.reads or
// limits.lookups are refused at once, not queued: bound what is in flight
// on the caller's side to wait instead.
func (g *Gateway) Read(ctx context.Context, bucket, key string, head bool, cond http.Header) (*Answer, error) {
	return g.s.ReadObject(ctx, bucket, key, head, cond)
}

// Close stops the gateway and waits for its background work.
func (g *Gateway) Close() error { return g.s.Close() }
