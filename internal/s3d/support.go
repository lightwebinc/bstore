package s3d

import (
	"bytes"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// replayCache answers a signed write sent again byte for byte with the
// answer the first one got, without reading its body or writing anything:
// a captured request cannot publish twice, nor publish other bytes under
// a payload it did not sign. A write in flight under the same signature is
// answered 409. Signatures are kept for the clock skew S3 allows, or a
// presigned URL's life.
type replayCache struct {
	now func() time.Time
	mu  sync.Mutex
	m   map[string]*replayEntry
}

type replayEntry struct {
	until  time.Time
	done   bool
	status int
	header http.Header
	body   []byte
}

const maxReplay = 200_000

func newReplayCache(now func() time.Time) *replayCache {
	return &replayCache{now: now, m: map[string]*replayEntry{}}
}

func (c *replayCache) begin(sig string, until time.Time) (*replayEntry, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if e, ok := c.m[sig]; ok && now.Before(e.until) {
		return e, e.done, !e.done
	}
	if len(c.m) >= maxReplay {
		for k, e := range c.m {
			if !now.Before(e.until) {
				delete(c.m, k)
			}
		}
		if len(c.m) >= maxReplay {
			return nil, false, false
		}
	}
	e := &replayEntry{until: until}
	c.m[sig] = e
	return e, false, false
}

func (c *replayCache) finish(sig string, e *replayEntry, ok bool, status int, h http.Header, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !ok {
		delete(c.m, sig)
		return
	}
	e.done, e.status, e.header, e.body = true, status, h, body
}

// recorder captures a small answer for the replay cache.
type recorder struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
	wrote  bool
}

func (r *recorder) WriteHeader(code int) {
	if !r.wrote {
		r.status, r.wrote = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(p []byte) (int, error) {
	if !r.wrote {
		r.status, r.wrote = http.StatusOK, true
	}
	if r.buf.Len() < 64<<10 {
		r.buf.Write(p)
	}
	return r.ResponseWriter.Write(p)
}

func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// replayed runs a write once per signature.
func (s *Server) replayed(w http.ResponseWriter, r *http.Request, a *Auth, f func(http.ResponseWriter) *s3Error) *s3Error {
	until := a.Time.Add(maxSkew)
	if a.Presign {
		exp := r.URL.Query().Get("X-Amz-Expires")
		var secs int64
		fmt.Sscan(exp, &secs)
		until = a.Time.Add(time.Duration(secs) * time.Second)
	}
	e, done, busy := s.replay.begin(a.Seed, until)
	switch {
	case e == nil:
		return errSlowDown
	case busy:
		return &s3Error{"OperationAborted", "A request with this signature is in progress.", http.StatusConflict}
	case done:
		for k, v := range e.header {
			if k == "X-Amz-Request-Id" {
				continue
			}
			w.Header()[k] = v
		}
		w.WriteHeader(e.status)
		_, _ = w.Write(e.body)
		return nil
	}
	rec := &recorder{ResponseWriter: w}
	err := f(rec)
	ok := err == nil && rec.wrote && rec.status < 300 && rec.buf.Len() < 64<<10 && !bytes.Contains(rec.buf.Bytes(), []byte("<Error>"))
	s.replay.finish(a.Seed, e, ok, rec.status, w.Header().Clone(), bytes.Clone(rec.buf.Bytes()))
	return err
}

// keyLimits is a token bucket per access key.
type keyLimits struct {
	rate, burst float64
	now         func() time.Time
	mu          sync.Mutex
	m           map[string]*bucketState
}

type bucketState struct {
	tokens float64
	at     time.Time
}

func newKeyLimits(rate, burst float64, now func() time.Time) *keyLimits {
	return &keyLimits{rate: rate, burst: burst, now: now, m: map[string]*bucketState{}}
}

func (k *keyLimits) allow(id string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	now := k.now()
	b := k.m[id]
	if b == nil {
		b = &bucketState{tokens: k.burst, at: now}
		k.m[id] = b
	}
	b.tokens = min(k.burst, b.tokens+now.Sub(b.at).Seconds()*k.rate)
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// metrics are the gateway's counters, served as Prometheus text.
type metrics struct {
	mu sync.Mutex
	c  map[string]*atomic.Int64
}

func newMetrics() *metrics { return &metrics{c: map[string]*atomic.Int64{}} }

func (m *metrics) add(name string, v int64) {
	m.mu.Lock()
	c := m.c[name]
	if c == nil {
		c = &atomic.Int64{}
		m.c[name] = c
	}
	m.mu.Unlock()
	c.Add(v)
}

func (m *metrics) request(op string, status int) {
	m.add(fmt.Sprintf("bstore_s3d_requests_total{op=%q,status=\"%d\"}", op, status), 1)
}

func (m *metrics) serve(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	names := make([]string, 0, len(m.c))
	for n := range m.c {
		names = append(names, n)
	}
	m.mu.Unlock()
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		m.mu.Lock()
		v := m.c[n].Load()
		m.mu.Unlock()
		fmt.Fprintf(&b, "%s %d\n", n, v)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
