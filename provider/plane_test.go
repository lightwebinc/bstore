package provider

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

// post submits beef on topic to a provider and returns the status and body.
func (r *rig) post(base, topic string, beef []byte) (int, string) {
	r.t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/submit", bytes.NewReader(beef))
	if err != nil {
		r.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-topics", topic)
	res, err := r.client.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

func taken(t testing.TB, body, topic string) (bool, string) {
	t.Helper()
	var ans map[string]struct {
		Out     []int  `json:"outputsToAdmit"`
		Refused string `json:"refused"`
	}
	if err := json.Unmarshal([]byte(body), &ans); err != nil {
		t.Fatalf("answer %q: %v", body, err)
	}
	a := ans[topic]
	return len(a.Out) > 0, a.Refused
}

// A bridge feeding a provider from the plane, and a facade whose route
// points at one, read its answer as an engine's: a piece that breaks a rule
// is a 400 (refused, never retried, never published), and a valid piece is
// a 200 whether or not this provider keeps it.
func TestSubmitChecksPiecesBeforePolicy(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	o, err := r.writer.Build(ctx, bytes.Repeat([]byte("plane carriage "), 100))
	if err != nil {
		t.Fatal(err)
	}
	topic := storerec.BlobTopic(testBucket)

	// A provider for another bucket: the piece is valid, so 200, and not
	// kept, with the reason.
	other := r.config(t.TempDir())
	other.Buckets = []BucketConfig{{Bucket: "other_qzvmkwerty"}}
	po := r.start(other, false)
	code, body := r.post("https://"+po.Addr(), topic, o.Pieces[0])
	if code != http.StatusOK {
		t.Fatalf("valid piece, unserved bucket: %d %s", code, body)
	}
	if ok, why := taken(t, body, topic); ok || !strings.Contains(why, "not a topic") {
		t.Fatalf("taken=%v why=%q", ok, why)
	}

	// The bucket's provider: a piece with one byte of its signed record
	// changed is refused as an engine refuses, and nothing is kept.
	p := r.start(r.config(t.TempDir()), false)
	base := "https://" + p.Addr()
	var blob []byte
	for _, pc := range o.Pieces {
		if bytes.Contains(pc, []byte("plane carriage")) {
			blob = pc
		}
	}
	if blob == nil {
		t.Fatal("no piece carries the content")
	}
	bad := append([]byte(nil), blob...)
	bad[bytes.Index(bad, []byte("plane carriage"))] ^= 0x01
	code, body = r.post(base, topic, bad)
	if code != http.StatusBadRequest || !strings.Contains(body, `"error":"piece:`) {
		t.Fatalf("broken piece: %d %s", code, body)
	}
	// The same piece intact is taken.
	code, body = r.post(base, topic, blob)
	if ok, why := taken(t, body, topic); code != http.StatusOK || !ok {
		t.Fatalf("intact piece: %d taken=%v why=%q", code, ok, why)
	}
}

// A piece whose funding tree is not in the provider's headers is a retry
// (503): its headers may only be behind, and a feed retries a 5xx.
func TestSubmitUnminedPieceIsARetry(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	o, err := r.writer.Build(ctx, []byte("funded on another chain view"))
	if err != nil {
		t.Fatal(err)
	}
	elsewhere := httptest.NewServer(testchain.New(700))
	t.Cleanup(elsewhere.Close)
	cfg := r.config(t.TempDir())
	cfg.Headers = elsewhere.URL
	p := r.start(cfg, false)
	code, body := r.post("https://"+p.Addr(), storerec.BlobTopic(testBucket), o.Pieces[0])
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "piece:") {
		t.Fatalf("unmined piece: %d %s", code, body)
	}
}

// A configured feeder skips the per-address submission budget, which a
// feed at plane speed would trip and lose pieces to; anyone else meets it.
func TestFeederSkipsThePerAddressBudget(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	o, err := r.writer.Build(ctx, bytes.Repeat([]byte("x"), 3*4096))
	if err != nil {
		t.Fatal(err)
	}
	topic := storerec.BlobTopic(testBucket)
	limited := func(feeders []string) []int {
		cfg := r.config(t.TempDir())
		cfg.Limits.SubmitsPerAddrPerSec, cfg.Limits.SubmitBurst = 0.001, 1
		cfg.Feeders = feeders
		p := r.start(cfg, false)
		var codes []int
		for n := 0; n < 3; n++ {
			code, _ := r.post("https://"+p.Addr(), topic, o.Pieces[0])
			codes = append(codes, code)
		}
		return codes
	}
	if codes := limited(nil); codes[1] != http.StatusTooManyRequests {
		t.Fatalf("without a feeder: %v, want the second limited", codes)
	}
	for _, c := range limited([]string{"127.0.0.0/8"}) {
		if c != http.StatusOK {
			t.Fatalf("a feeder was limited: %d", c)
		}
	}
	bad := r.config(t.TempDir())
	bad.Feeders = []string{"127.0.0.1"}
	if err := bad.Check(); err == nil || !strings.Contains(err.Error(), "feeders") {
		t.Fatalf("a feeder that is not a CIDR: %v", err)
	}
}

// The feed listener takes the local bridge's submissions in plain HTTP,
// without the per-address budgets, and serves nothing else; it must be a
// loopback address.
func TestFeedListener(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	o, err := r.writer.Build(ctx, bytes.Repeat([]byte("y"), 2*4096))
	if err != nil {
		t.Fatal(err)
	}
	cfg := r.config(t.TempDir())
	cfg.Limits.SubmitsPerAddrPerSec, cfg.Limits.SubmitBurst = 0.001, 1
	feed := r.config(t.TempDir()).Listen // a free loopback port
	cfg.FeedListen = feed
	r.start(cfg, false)
	topic := storerec.BlobTopic(testBucket)
	plain := &http.Client{}
	for n := 0; n < 3; n++ {
		req, _ := http.NewRequest(http.MethodPost, "http://"+feed+"/submit", bytes.NewReader(o.Pieces[0]))
		req.Header.Set("x-topics", topic)
		res, err := plain.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if ok, why := taken(t, string(b), topic); res.StatusCode != http.StatusOK || !ok {
			t.Fatalf("feed submission %d: %d taken=%v %q", n, res.StatusCode, ok, why)
		}
	}
	res, err := plain.Get("http://" + feed + "/bstore/v1/terms")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("a public route on the feed listener: %d", res.StatusCode)
	}
	bad := r.config(t.TempDir())
	bad.FeedListen = "0.0.0.0:8095"
	if err := bad.Check(); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("a public feed listener: %v", err)
	}
}

// A feeder's piece is answered once it has passed its check and taken
// after: the answer is an engine's for a taken piece, the piece is in the
// store soon after, a body a pending take holds counts against the bodies
// bound as one being read does, and a stop waits for the takes, so the
// state it saves names the piece.
func TestFeederIsAnsweredBeforeTheTake(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	var pieces [][]byte
	for _, s := range []string{"a", "b", "c"} {
		o, err := r.writer.Build(ctx, bytes.Repeat([]byte(s), 2*4096))
		if err != nil {
			t.Fatal(err)
		}
		pieces = append(pieces, o.Pieces[0])
	}
	topic := storerec.BlobTopic(testBucket)
	hashOf := func(beef []byte) [32]byte {
		pc, err := storerec.CheckPieceCarrier(ctx, beef, testBucket, nil)
		if err != nil {
			t.Fatal(err)
		}
		return pc.Piece.Hash()
	}
	cfg := r.config(t.TempDir())
	feed := r.config(t.TempDir()).Listen // a free loopback port
	cfg.FeedListen = feed
	p := r.start(cfg, false)
	// One body at a time, and the takes held.
	p.limits.bodies = make(chan struct{}, 1)
	p.limits.takes = make(chan struct{}, 1)
	p.limits.takes <- struct{}{}
	plain := &http.Client{}
	post := func(beef []byte) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, "http://"+feed+"/submit", bytes.NewReader(beef))
		req.Header.Set("x-topics", topic)
		res, err := plain.Do(req)
		if err != nil {
			t.Error(err)
			return 0, ""
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, string(b)
	}
	asyncTaken := func() float64 {
		p.metrics.mu.Lock()
		defer p.metrics.mu.Unlock()
		return p.metrics.counters[series("bstore_provider_async_takes_total", []string{"result=taken"})]
	}
	code, body := post(pieces[0])
	if ok, why := taken(t, body, topic); code != http.StatusOK || !ok || why != "" {
		t.Fatalf("a feeder's piece: %d taken=%v %q", code, ok, why)
	}
	if p.store.Has(hashOf(pieces[0])) {
		t.Fatal("the piece was stored before its take")
	}
	// The slot the pending take holds is the one body allowed: the next
	// submission waits for it, as it would for a body being read.
	answered := make(chan int, 1)
	go func() { code, _ := post(pieces[1]); answered <- code }()
	select {
	case code := <-answered:
		t.Fatalf("a second body was read past the bound: %d", code)
	case <-time.After(300 * time.Millisecond):
	}
	<-p.limits.takes
	select {
	case code := <-answered:
		if code != http.StatusOK {
			t.Fatalf("the second piece: %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second piece was not answered once the takes ran")
	}
	r.eventually("both pieces stored and counted", 10*time.Second, func() bool {
		return p.store.Has(hashOf(pieces[0])) && p.store.Has(hashOf(pieces[1])) && asyncTaken() == 2
	})
	// A stop waits for a take pending, and saves what it kept.
	p.limits.takes <- struct{}{}
	if code, _ := post(pieces[2]); code != http.StatusOK {
		t.Fatalf("the third piece: %d", code)
	}
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("stopped with a take pending: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	<-p.limits.takes
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stop did not finish once the take ran")
	}
	h := hashOf(pieces[2])
	st, err := loadState(cfg.Data)
	if err != nil {
		t.Fatal(err)
	}
	if st.Unclaimed[hex.EncodeToString(h[:])] == nil || !p.store.Has(h) {
		t.Fatal("the state saved at the stop does not name the piece taken after its answer")
	}
}
