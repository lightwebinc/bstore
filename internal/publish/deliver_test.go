package publish

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
)

// A closure sent by upload session is only claimed by an entry: the entry
// goes to the origin of each public URL, once, and a provider named twice
// hears once.
func TestEntryReachesUploadProviders(t *testing.T) {
	d := &Delivery{Hosts: []string{"https://host.example.com"}, Providers: []string{"https://b.example.com/"},
		Uploads: []string{"https://a.example.com/store", "https://b.example.com/store"}}
	providers := func(entry bool) []string {
		var out []string
		for _, target := range d.recordTargets("b", entry) {
			if !target.Host {
				out = append(out, target.Base)
			}
		}
		return out
	}
	if got, want := providers(true), []string{"https://b.example.com", "https://a.example.com"}; !slices.Equal(got, want) {
		t.Fatalf("an entry goes to %q, want %q", got, want)
	}
	if got := providers(false); len(got) != 0 {
		t.Fatalf("a record that is no entry goes to %q", got)
	}
	d.Facade = "https://facade.example.com"
	if got, want := providers(true), []string{"https://facade.example.com", "https://b.example.com"}; !slices.Equal(got, want) {
		t.Fatalf("with a facade an entry goes to %q, want %q", got, want)
	}
}

// A facade whose publish queue is full answers 503 with Retry-After: the
// piece waits and is delivered, and a put does not fail on a moment's
// backpressure. A 503 with no Retry-After is a failure, as before.
func TestSubmitRidesOutBusyTarget(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, `{"error":"the publish queue is full; retry"}`, http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	d := &Delivery{}
	if err := d.submit(context.Background(), Target{Base: srv.URL, Topic: "t"}, []byte("beef")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("%d attempts, want 3", n)
	}
}

// A provider over its per-address budget answers 429 with Retry-After: the
// piece waits and is delivered, as on a busy facade.
func TestSubmitRidesOutRateLimit(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, `{"code":"ERR_RATE_LIMITED","description":"too many submissions in flight from this address"}`, http.StatusTooManyRequests)
			return
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	d := &Delivery{}
	if err := d.submit(context.Background(), Target{Base: srv.URL, Topic: "t"}, []byte("beef")); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("%d attempts, want 3", n)
	}
}

func TestSubmitFailsWithoutRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	d := &Delivery{}
	if err := d.submit(context.Background(), Target{Base: srv.URL, Topic: "t"}, []byte("beef")); err == nil {
		t.Fatal("a plain 503 was delivered")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d attempts, want 1", n)
	}
}
