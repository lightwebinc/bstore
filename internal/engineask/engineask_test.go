package engineask

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type engine struct {
	mu     sync.Mutex
	asks   []map[string]any
	refuse int // answer 400 to this many asks first
}

func (e *engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/admin/topics" {
		http.NotFound(w, r)
		return
	}
	var body map[string]any
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.asks = append(e.asks, body)
	if e.refuse > 0 {
		e.refuse--
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"BSTORE_BUCKETS is not a setting this host lets applications append to"}`))
		return
	}
	_, _ = w.Write([]byte(`{"entry":{}}`))
}

func (e *engine) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.asks)
}

func TestAskShape(t *testing.T) {
	e := &engine{}
	srv := httptest.NewServer(e)
	defer srv.Close()
	a := &Asker{URL: srv.URL + "/", ID: "bstore-provider", TTL: 10 * time.Minute,
		Settings: map[string][]string{"OVERLAY_TOPICS": {"tm_bstore_a", "tm_bstoreblob_a"}, "BSTORE_BUCKETS": {"a"}}}
	if err := a.Check(); err != nil {
		t.Fatal(err)
	}
	if err := a.Ask(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := e.asks[0]
	if got["id"] != "bstore-provider" || got["ttl_seconds"] != float64(600) {
		t.Fatalf("ask %v", got)
	}
	s := got["settings"].(map[string]any)
	if len(s["OVERLAY_TOPICS"].([]any)) != 2 || s["BSTORE_BUCKETS"].([]any)[0] != "a" {
		t.Fatalf("settings %v", s)
	}
}

func TestRefusalIsReportedAndRetried(t *testing.T) {
	e := &engine{refuse: 2}
	srv := httptest.NewServer(e)
	defer srv.Close()
	var logs []string
	var oks []bool
	var mu sync.Mutex
	a := &Asker{URL: srv.URL, ID: "x", TTL: time.Hour, Settings: map[string][]string{"OVERLAY_TOPICS": {"tm_x"}},
		Logf:     func(f string, args ...any) { mu.Lock(); logs = append(logs, f); mu.Unlock() },
		OnResult: func(ok bool) { mu.Lock(); oks = append(oks, ok); mu.Unlock() }}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	// Refused twice (retried after 1 s, then 2 s), then carried.
	deadline := time.Now().Add(10 * time.Second)
	for e.count() < 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(oks) != 3 || oks[0] || oks[1] || !oks[2] {
		t.Fatalf("outcomes %v", oks)
	}
	// One line for the refusal (the same twice), one for the success.
	if len(logs) != 2 || !strings.Contains(logs[0], "asking again") || !strings.Contains(logs[1], "carries") {
		t.Fatalf("logs %q", logs)
	}
}

func TestRefreshAtHalfTheLifetime(t *testing.T) {
	e := &engine{}
	srv := httptest.NewServer(e)
	defer srv.Close()
	a := &Asker{URL: srv.URL, ID: "x", TTL: 400 * time.Millisecond, Settings: map[string][]string{"OVERLAY_TOPICS": {"tm_x"}}}
	ctx, cancel := context.WithTimeout(t.Context(), 900*time.Millisecond)
	defer cancel()
	a.Run(ctx)
	// At 0, 200, 400, 600, 800 ms.
	if n := e.count(); n < 4 || n > 6 {
		t.Fatalf("%d asks in 900 ms at a 200 ms refresh", n)
	}
}

func TestCheck(t *testing.T) {
	for _, a := range []*Asker{
		{URL: "127.0.0.1:8280", ID: "x", Settings: map[string][]string{"OVERLAY_TOPICS": {"t"}}},
		{URL: "http://127.0.0.1:8280", ID: "a/b", Settings: map[string][]string{"OVERLAY_TOPICS": {"t"}}},
		{URL: "http://127.0.0.1:8280", ID: "x"},
	} {
		if a.Check() == nil {
			t.Fatalf("accepted %+v", a)
		}
	}
}
