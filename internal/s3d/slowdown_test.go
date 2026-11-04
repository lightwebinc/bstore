package s3d

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The default replay bound holds one replay window at the default request
// rate, so the rate, not the replay cache, is what holds a key back.
func TestReplayDefaultFollowsRequestRate(t *testing.T) {
	var l Limits
	l.defaults()
	if want := int(l.RequestsPerSec * maxSkew.Seconds()); l.ReplayPerKey != want {
		t.Fatalf("replay_per_key %d, want %d", l.ReplayPerKey, want)
	}
	if l.Writes < 10 {
		t.Fatalf("writes %d refuses an S3 client's default concurrency", l.Writes)
	}
	l = Limits{RequestsPerSec: 10}
	l.defaults()
	if l.ReplayPerKey != 9000 {
		t.Fatalf("replay_per_key %d at 10 requests a second", l.ReplayPerKey)
	}
	l = Limits{ReplayPerKey: 5}
	l.defaults()
	if l.ReplayPerKey != 5 {
		t.Fatal("a configured replay_per_key was replaced")
	}
}

// Each bound answers its own SlowDown, and the metric names it.
func TestSlowDownNamesItsBound(t *testing.T) {
	now := time.Now()
	s := &Server{replay: newReplayCache(func() time.Time { return now }, 1)}
	ok := func(w http.ResponseWriter) *s3Error { w.WriteHeader(200); return nil }
	a := &Auth{Seed: "one", Time: now, Key: &Key{ID: "k"}}
	if e := s.replayed(httptest.NewRecorder(), httptest.NewRequest("PUT", "/b/k", nil), a, ok); e != nil {
		t.Fatal(e)
	}
	a2 := &Auth{Seed: "two", Time: now, Key: &Key{ID: "k"}}
	e := s.replayed(httptest.NewRecorder(), httptest.NewRequest("PUT", "/b/k", nil), a2, ok)
	if e != errSlowReplay || slowReason(e) != "replay_per_key" {
		t.Fatalf("a full replay cache answered %v", e)
	}

	srv := &Server{writes: make(chan struct{}, 1), parts: make(chan struct{}, 1), reads: make(chan struct{}, 1),
		stages: make(chan struct{}, 1), lookups: make(chan struct{}, 1)}
	nameSlots(srv)
	for sem, want := range map[chan struct{}]string{srv.writes: "writes", srv.parts: "part_writes", srv.reads: "reads",
		srv.stages: "stages", srv.lookups: "lookups"} {
		release, e := acquire(sem)
		if e != nil {
			t.Fatal(e)
		}
		_, e = acquire(sem)
		if e == nil || e.Code != "SlowDown" || slowReason(e) != want {
			t.Fatalf("a full %s slot answered %v (%s)", want, e, slowReason(e))
		}
		release()
	}
	if slowReason(errSlowDown) != "other" || slowReason(errSlowKeyRate) != "requests_per_sec" {
		t.Fatal("reasons")
	}
}
