package engineask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	fail   int // then answer 503 to this many
	status int // answer this to every ask
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
	if e.fail > 0 {
		e.fail--
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if e.status != 0 {
		w.WriteHeader(e.status)
		return
	}
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

// run runs a against e until e has seen n asks, and returns the outcomes
// and the log lines.
func run(t *testing.T, a *Asker, e *engine, n int) (oks []bool, logs []string) {
	t.Helper()
	var mu sync.Mutex
	a.Logf = func(f string, args ...any) { mu.Lock(); logs = append(logs, fmt.Sprintf(f, args...)); mu.Unlock() }
	a.OnResult = func(ok bool) { mu.Lock(); oks = append(oks, ok); mu.Unlock() }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		got := len(oks)
		mu.Unlock()
		if got >= n || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	return oks, logs
}

// A refusal is reported every time, and asked again at the refresh
// interval, not sooner.
func TestRefusalIsReportedEachTime(t *testing.T) {
	e := &engine{refuse: 2}
	srv := httptest.NewServer(e)
	defer srv.Close()
	a := &Asker{URL: srv.URL, ID: "x", TTL: 2 * time.Second, Settings: map[string][]string{"OVERLAY_TOPICS": {"tm_x"}}}
	start := time.Now()
	oks, logs := run(t, a, e, 3)
	if took := time.Since(start); took < 1900*time.Millisecond {
		t.Fatalf("three asks in %s: a refusal was asked again before the refresh interval (1 s)", took)
	}
	if len(oks) != 3 || oks[0] || oks[1] || !oks[2] {
		t.Fatalf("outcomes %v", oks)
	}
	if len(logs) != 3 || !strings.Contains(logs[0], "ENGINE REFUSED") || !strings.Contains(logs[1], "ENGINE REFUSED") ||
		!strings.Contains(logs[0], "BSTORE_BUCKETS is not a setting") || !strings.Contains(logs[2], "carries") {
		t.Fatalf("logs %q", logs)
	}
}

// An engine that cannot be reached is asked again sooner (1 s, 2 s), and
// reported once.
func TestFailureIsRetriedSooner(t *testing.T) {
	e := &engine{fail: 2}
	srv := httptest.NewServer(e)
	defer srv.Close()
	a := &Asker{URL: srv.URL, ID: "x", TTL: time.Hour, Settings: map[string][]string{"OVERLAY_TOPICS": {"tm_x"}}}
	oks, logs := run(t, a, e, 3)
	if len(oks) != 3 || oks[0] || oks[1] || !oks[2] {
		t.Fatalf("outcomes %v", oks)
	}
	if len(logs) != 2 || !strings.Contains(logs[0], "asking again") || strings.Contains(logs[0], "REFUSED") || !strings.Contains(logs[1], "carries") {
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
		{URL: "http://127.0.0.1:8280", ID: "x", TTL: 500 * time.Millisecond, Settings: map[string][]string{"OVERLAY_TOPICS": {"t"}}},
	} {
		if a.Check() == nil {
			t.Fatalf("accepted %+v", a)
		}
	}
}

func TestTransientAnswersAreNotRefusals(t *testing.T) {
	for _, code := range []int{408, 425, 429, 500, 503} {
		e := &engine{status: code}
		srv := httptest.NewServer(e)
		a := &Asker{URL: srv.URL, ID: "x", Settings: map[string][]string{"OVERLAY_TOPICS": {"t"}}}
		var ref *Refused
		if err := a.Ask(t.Context()); err == nil || errors.As(err, &ref) {
			t.Fatalf("%d: %v", code, err)
		}
		srv.Close()
	}
	for _, code := range []int{400, 403, 404, 405, 413} {
		e := &engine{status: code}
		srv := httptest.NewServer(e)
		a := &Asker{URL: srv.URL, ID: "x", Settings: map[string][]string{"OVERLAY_TOPICS": {"t"}}}
		var ref *Refused
		if err := a.Ask(t.Context()); !errors.As(err, &ref) || ref.Status != code {
			t.Fatalf("%d: %v", code, err)
		}
		srv.Close()
	}
}
