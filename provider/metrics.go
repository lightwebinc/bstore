package provider

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// metrics are the provider's counters and gauges, served in Prometheus'
// text format on /metrics on the metrics listener only.
type metrics struct {
	mu       sync.Mutex
	counters map[string]float64
	gauges   map[string]float64
	hists    map[string]*histogram
}

// histogram is a Prometheus histogram of seconds, over histBuckets.
type histogram struct {
	counts []float64 // one a bucket, and one past the last
	sum    float64
	n      float64
}

// histBuckets are the upper bounds, in seconds, of every histogram here:
// from a sync to a wait on a slow leg.
var histBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

func newMetrics() *metrics {
	return &metrics{counters: map[string]float64{}, gauges: map[string]float64{}, hists: map[string]*histogram{}}
}

// observe adds one observation of seconds to the histogram name{labels}.
func (m *metrics) observe(name string, seconds float64, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := series(name, labels)
	h := m.hists[k]
	if h == nil {
		h = &histogram{counts: make([]float64, len(histBuckets)+1)}
		m.hists[k] = h
	}
	i := 0
	for i < len(histBuckets) && seconds > histBuckets[i] {
		i++
	}
	h.counts[i]++
	h.sum += seconds
	h.n++
}

// timed observes how long since at.
func (m *metrics) timed(name string, at time.Time, labels ...string) {
	m.observe(name, time.Since(at).Seconds(), labels...)
}

// sum is a histogram's sum and count, for a test.
func (m *metrics) sum(name string, labels ...string) (float64, float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h := m.hists[series(name, labels)]; h != nil {
		return h.sum, h.n
	}
	return 0, 0
}

// inc adds one to name{labels}; labels are "k=v" pairs.
func (m *metrics) inc(name string, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[series(name, labels)]++
}

// preset makes a counter series exist at zero, so a scrape sees it.
func (m *metrics) preset(name string, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.counters[series(name, labels)]; !ok {
		m.counters[series(name, labels)] = 0
	}
}

// add adds v to the counter name.
func (m *metrics) add(name string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] += v
}

// get reads a counter or gauge.
func (m *metrics) get(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.counters[name]; ok {
		return v
	}
	return m.gauges[name]
}

func (m *metrics) set(name string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = v
}

// count is auth104's outcome counter.
func (m *metrics) count(what, result string) {
	m.inc("bstore_provider_"+what+"_total", "result="+result)
}

func series(name string, labels []string) string {
	if len(labels) == 0 {
		return name
	}
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		k, v, _ := strings.Cut(l, "=")
		parts = append(parts, fmt.Sprintf("%s=%q", k, v))
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (m *metrics) serve(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	var lines []string
	for k, v := range m.counters {
		lines = append(lines, fmt.Sprintf("%s %v", k, v))
	}
	for k, v := range m.gauges {
		lines = append(lines, fmt.Sprintf("%s %v", k, v))
	}
	for k, h := range m.hists {
		// k is name{labels}; the buckets add le to the labels.
		name, labels, le := k, "", "{"
		if i := strings.IndexByte(k, '{'); i >= 0 {
			name, labels = k[:i], k[i:]
			le = labels[:len(labels)-1] + ","
		}
		cum := 0.0
		for i, b := range histBuckets {
			cum += h.counts[i]
			lines = append(lines, fmt.Sprintf("%s_bucket%sle=%q} %v", name, le, strconv.FormatFloat(b, 'g', -1, 64), cum))
		}
		cum += h.counts[len(histBuckets)]
		lines = append(lines, fmt.Sprintf("%s_bucket%sle=\"+Inf\"} %v", name, le, cum))
		lines = append(lines, fmt.Sprintf("%s_sum%s %s", name, labels, strconv.FormatFloat(h.sum, 'g', 9, 64)))
		lines = append(lines, fmt.Sprintf("%s_count%s %v", name, labels, h.n))
	}
	m.mu.Unlock()
	sort.Strings(lines)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}
