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
	now    func() time.Time
	perKey int
	mu     sync.Mutex
	m      map[string]*replayEntry
	byKey  map[string]int
}

type replayEntry struct {
	key    string
	until  time.Time
	done   bool
	status int
	header http.Header
	body   []byte
}

func newReplayCache(now func() time.Time, perKey int) *replayCache {
	return &replayCache{now: now, perKey: perKey, m: map[string]*replayEntry{}, byKey: map[string]int{}}
}

// sweep drops expired signatures; the caller holds c.mu.
func (c *replayCache) sweep(now time.Time) {
	for k, e := range c.m {
		if !now.Before(e.until) {
			delete(c.m, k)
			if c.byKey[e.key]--; c.byKey[e.key] <= 0 {
				delete(c.byKey, e.key)
			}
		}
	}
}

// begin claims a signature for key. It returns the entry, whether its
// write is done (answer from the cache) or busy (in progress), or nil when
// the key holds its bound of unexpired signatures: each key fills only its
// own share, so one key cannot stop another's writes.
func (c *replayCache) begin(sig, key string, until time.Time) (*replayEntry, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if e, ok := c.m[sig]; ok {
		if now.Before(e.until) {
			return e, e.done, !e.done
		}
		delete(c.m, sig)
		if c.byKey[e.key]--; c.byKey[e.key] <= 0 {
			delete(c.byKey, e.key)
		}
	}
	if c.perKey > 0 && c.byKey[key] >= c.perKey {
		c.sweep(now)
		if c.byKey[key] >= c.perKey {
			return nil, false, false
		}
	}
	e := &replayEntry{key: key, until: until}
	c.m[sig] = e
	c.byKey[key]++
	return e, false, false
}

func (c *replayCache) finish(sig string, e *replayEntry, ok bool, status int, h http.Header, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !ok {
		if c.m[sig] == e {
			delete(c.m, sig)
			if c.byKey[e.key]--; c.byKey[e.key] <= 0 {
				delete(c.byKey, e.key)
			}
		}
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
	e, done, busy := s.replay.begin(a.Seed, a.Key.ID, until)
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

// metrics are the gateway's counters and histograms, served as Prometheus
// text.
type metrics struct {
	mu sync.Mutex
	c  map[string]*atomic.Int64
	h  map[string]*hist
}

func newMetrics() *metrics { return &metrics{c: map[string]*atomic.Int64{}, h: map[string]*hist{}} }

// histBuckets are the upper bounds, in seconds, of every histogram.
var histBuckets = [...]float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// hist is a histogram of seconds: one count per bucket (the last past
// every bound) and the sum in nanoseconds.
type hist struct {
	counts [len(histBuckets) + 1]atomic.Int64
	sum    atomic.Int64
}

// observe adds one observation of seconds to the histogram name, which may
// carry labels (`name{k="v"}`).
func (m *metrics) observe(name string, seconds float64) {
	m.mu.Lock()
	h := m.h[name]
	if h == nil {
		h = &hist{}
		m.h[name] = h
	}
	m.mu.Unlock()
	h.counts[sort.SearchFloat64s(histBuckets[:], seconds)].Add(1)
	h.sum.Add(int64(seconds * 1e9))
}

// render writes the histogram as Prometheus text: its cumulative buckets,
// sum and count, with le beside the name's own labels.
func (h *hist) render(b *strings.Builder, name string) {
	base, labels := name, ""
	if i := strings.IndexByte(name, '{'); i >= 0 {
		base, labels = name[:i], name[i+1:len(name)-1]+","
	}
	var cum int64
	for i, le := range histBuckets {
		cum += h.counts[i].Load()
		fmt.Fprintf(b, "%s_bucket{%sle=\"%g\"} %d\n", base, labels, le, cum)
	}
	cum += h.counts[len(histBuckets)].Load()
	fmt.Fprintf(b, "%s_bucket{%sle=\"+Inf\"} %d\n", base, labels, cum)
	own := strings.TrimSuffix(labels, ",")
	fmt.Fprintf(b, "%s_sum{%s} %g\n", base, own, float64(h.sum.Load())/1e9)
	fmt.Fprintf(b, "%s_count{%s} %d\n", base, own, cum)
}

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

// set sets a gauge.
func (m *metrics) set(name string, v int64) {
	m.add(name, 0)
	m.mu.Lock()
	c := m.c[name]
	m.mu.Unlock()
	c.Store(v)
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
	m.mu.Lock()
	hnames := make([]string, 0, len(m.h))
	for n := range m.h {
		hnames = append(hnames, n)
	}
	m.mu.Unlock()
	sort.Strings(hnames)
	for _, n := range hnames {
		m.mu.Lock()
		h := m.h[n]
		m.mu.Unlock()
		h.render(&b, n)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
