package provider

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// metrics are the provider's counters and gauges, served in Prometheus'
// text format on /metrics on the metrics listener only.
type metrics struct {
	mu       sync.Mutex
	counters map[string]float64
	gauges   map[string]float64
}

func newMetrics() *metrics {
	return &metrics{counters: map[string]float64{}, gauges: map[string]float64{}}
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
	m.mu.Unlock()
	sort.Strings(lines)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}
