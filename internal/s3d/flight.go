package s3d

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strings"
	"sync"

	"github.com/lightwebinc/bstore/storerec"
)

// writeFlights holds the writes being published now, by writeKey. An S3
// client that times out sends the same request again while the first is
// still publishing (the aws CLI gives up after 60 s, and a write can wait a
// block for a mined funding tree); without this, every resend opened its own
// entry and bounty race for the same root. A resend after the publish ended
// is retryOf's.
type writeFlights struct {
	mu sync.Mutex
	m  map[[32]byte]*writeFlight
}

type writeFlight struct {
	done  chan struct{}
	entry *storerec.Entry
	err   *s3Error
}

// join returns the flight for k and whether the caller leads it: a leader
// publishes and calls finish, anyone else waits on done.
func (wf *writeFlights) join(k [32]byte) (*writeFlight, bool) {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	if f := wf.m[k]; f != nil {
		return f, false
	}
	if wf.m == nil {
		wf.m = make(map[[32]byte]*writeFlight)
	}
	f := &writeFlight{done: make(chan struct{})}
	wf.m[k] = f
	return f, true
}

// finish records the leader's answer and releases every waiter.
func (wf *writeFlights) finish(k [32]byte, f *writeFlight, entry *storerec.Entry, err *s3Error) {
	wf.mu.Lock()
	delete(wf.m, k)
	wf.mu.Unlock()
	f.entry, f.err = entry, err
	close(f.done)
}

// writeKey names one write by what retryOf compares: the bucket, the
// writer, the key, the content (root and size), its type, ETag and
// metadata (in any order).
func writeKey(bucket, writer, key, ct string, meta []storerec.MetaPair, etag string, root [32]byte, size uint64) [32]byte {
	h := sha256.New()
	field := func(b []byte) {
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b))))
		h.Write(b)
	}
	for _, s := range []string{bucket, writer, key, ct, etag} {
		field([]byte(s))
	}
	field(root[:])
	field(binary.BigEndian.AppendUint64(nil, size))
	m := slices.Clone(meta)
	slices.SortFunc(m, func(p, q storerec.MetaPair) int { return strings.Compare(p.Key, q.Key) })
	for _, p := range m {
		field([]byte(p.Key))
		field([]byte(p.Value))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// oneWrite runs write once for all the requests naming the same write at
// the same time; each answers with the entry it made, or its error.
func (s *Server) oneWrite(ctx context.Context, k [32]byte, write func() (*storerec.Entry, *s3Error)) (entry *storerec.Entry, e *s3Error) {
	f, lead := s.flights.join(k)
	if !lead {
		s.metrics.add("bstore_s3d_put_retries_total", 1)
		select {
		case <-f.done:
			return f.entry, f.err
		case <-ctx.Done():
			return nil, errSlowDown
		}
	}
	e = errSlowDown // what the waiters get if write panics
	defer func() { s.flights.finish(k, f, entry, e) }()
	return write()
}
