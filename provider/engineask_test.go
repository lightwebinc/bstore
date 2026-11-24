package provider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

// A provider with engine_topics asks the engine on its host for its buckets'
// records and blob topics as it starts, in one ask, and reports it carried.
func TestProviderAsksTheEngineForItsBuckets(t *testing.T) {
	type ask struct {
		ID       string              `json:"id"`
		TTL      int                 `json:"ttl_seconds"`
		Settings map[string][]string `json:"settings"`
	}
	got := make(chan ask, 4)
	eng := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var a ask
		if req.URL.Path != "/admin/topics" || json.NewDecoder(req.Body).Decode(&a) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		got <- a
		_, _ = w.Write([]byte(`{"entry":{}}`))
	}))
	defer eng.Close()

	r := newRig(t)
	cfg := r.config(t.TempDir())
	cfg.Buckets = append(cfg.Buckets, BucketConfig{Bucket: "second_qzvmkwerty"})
	cfg.EngineTopics = eng.URL
	cfg.EngineTopicsTTLSeconds = 120
	if err := cfg.Check(); err != nil {
		t.Fatal(err)
	}
	p := r.start(cfg, false)
	defer p.Close()

	select {
	case a := <-got:
		if a.ID != "bstore-provider" || a.TTL != 120 {
			t.Fatalf("ask %+v", a)
		}
		want := []string{"tm_bstore_" + testBucket, "tm_bstoreblob_" + testBucket, "tm_bstore_second_qzvmkwerty", "tm_bstoreblob_second_qzvmkwerty"}
		if !slices.Equal(a.Settings["OVERLAY_TOPICS"], want) {
			t.Fatalf("topics %v, want %v", a.Settings["OVERLAY_TOPICS"], want)
		}
		if !slices.Equal(a.Settings["BSTORE_BUCKETS"], []string{testBucket, "second_qzvmkwerty"}) || len(a.Settings) != 2 {
			t.Fatalf("settings %v", a.Settings)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the provider never asked the engine")
	}
	deadline := time.Now().Add(5 * time.Second)
	for p.metrics.get("bstore_provider_engine_ask_ok") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("bstore_provider_engine_ask_ok never 1")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestEngineTopicsChecked(t *testing.T) {
	r := newRig(t)
	cfg := r.config(t.TempDir())
	cfg.EngineTopics = "127.0.0.1:8280"
	if cfg.Check() == nil {
		t.Fatal("an engine address that is not a URL was accepted")
	}
}
