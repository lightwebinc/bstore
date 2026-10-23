package module

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

// The topic managers on the golden transaction vectors (tm_bstore.test.ts):
// through the manager directly and through the engine's admission path,
// every refusal raised and counted by its label and nothing refused
// recorded; a worse copy cannot suppress a genuine object; a token whose
// block the headers lack is admitted once they catch up; pieces and the
// blob topic admit nothing.

func TestVectorsThroughTheManager(t *testing.T) {
	v, h := readTxVectors(t)
	if v.Bucket != bucket {
		t.Fatalf("vectors for %s", v.Bucket)
	}
	host := newFakeHost()
	tm, err := NewRecordsTopicManager(topic, host, headersOf(h), storerec.DefaultMaxBEEF, storerec.DefaultMaxSweepInputs)
	if err != nil {
		t.Fatal(err)
	}
	refused := map[string]float64{}
	for _, c := range v.Cases {
		got, err := tm.Admit(context.Background(), beefOf(t, c), c.PreviousCoins)
		if c.Verdict == "admit" {
			if err != nil {
				t.Fatalf("%s: %v", c.Name, err)
			}
			equalU32(t, got.OutputsToAdmit, c.Admits.OutputsToAdmit, c.Name+" outputs")
			equalU32(t, got.CoinsToRetain, c.Admits.CoinsToRetain, c.Name+" coins")
			continue
		}
		var r *Refused
		if !errors.As(err, &r) || r.Reason != c.Reason {
			t.Fatalf("%s: want refused %s, got %v", c.Name, c.Reason, err)
		}
		refused[c.Reason]++
	}
	for reason, n := range refused {
		if got := host.count("bstore_refused_total", map[string]string{"reason": reason}); got != n {
			t.Errorf("%s counted %v, want %v", reason, got, n)
		}
		if !slices.Contains(Reasons, reason) {
			t.Errorf("%s is not a preset label", reason)
		}
	}
	if refused["piece"] == 0 || refused["not-bstore"] == 0 {
		t.Error("pieces and transactions of nothing bstore are among the refusals")
	}
	// Counted once per transaction, however often it is shown again; through
	// the contract's method, with the submission's bytes in ctx.
	put := v.byName(t, "entry-put")
	before := []float64{host.count("bstore_admitted_total", map[string]string{"kind": "entry"}), host.count("bstore_admitted_repeats_total", map[string]string{"kind": "entry"})}
	raw := beefOf(t, put)
	if _, err := tm.IdentifyAdmissibleOutputs(context.Background(), nil, nil, nil); !errors.Is(err, ErrNoSubmission) {
		t.Fatalf("no submission: %v", err)
	}
	if _, err := tm.IdentifyAdmissibleOutputs(bmodule.WithSubmission(context.Background(), raw), nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if a, r := host.count("bstore_admitted_total", map[string]string{"kind": "entry"}), host.count("bstore_admitted_repeats_total", map[string]string{"kind": "entry"}); a != before[0] || r != before[1]+1 {
		t.Fatalf("admitted %v repeats %v", a, r)
	}
}

func TestVectorsThroughTheEngine(t *testing.T) {
	v, h := readTxVectors(t)
	r := setup(t, setupOpts{chain: newTestChain(900000, h)})
	admitted := map[string]bool{}
	for _, c := range v.Cases {
		if c.Verdict == "admit" {
			admitted[c.Txid] = true
		}
	}
	done := map[string]bool{}
	for _, c := range v.Cases {
		got := r.submit(beefOf(t, c), "")
		switch {
		case c.Verdict == "admit":
			// The same transaction in another admissible BEEF is a duplicate
			// the engine does not show the manager again.
			if !done[c.Txid] {
				equalU32(t, got, c.Admits.OutputsToAdmit, c.Name)
			}
			done[c.Txid] = true
		case !admitted[c.Txid]:
			equalU32(t, got, nil, c.Name)
			if r.storage.isApplied(c.Txid, topic) {
				t.Fatalf("%s: a refusal was recorded", c.Name)
			}
		}
	}
	if s := r.status(v.byName(t, "token-first").Txid); s != "rooted" {
		t.Fatalf("token-first is %s", s)
	}
}

func TestSuppression(t *testing.T) {
	v, h := readTxVectors(t)
	for _, p := range [][2]string{
		{"carrier-beef-tree-unproven", "entry-beef-v1"},
		{"carrier-beef-extra-transaction", "entry-beef-v1"},
		{"token-beef-atomic", "token-v2"},
		{"token-beef-no-parent", "token-v2"},
		{"advertisement-beef-with-parent", "advertisement"},
		{"sweep-beef-extra-transaction", "sweep"},
	} {
		worse, genuine := v.byName(t, p[0]), v.byName(t, p[1])
		if worse.Txid != genuine.Txid {
			t.Fatalf("%s and %s are different transactions", p[0], p[1])
		}
		naive := setup(t, setupOpts{chain: newTestChain(900000, h), naive: true})
		equalU32(t, naive.submit(beefOf(t, worse), ""), nil, p[0])
		if !naive.storage.isApplied(genuine.Txid, topic) {
			t.Fatalf("%s: the naive refusal is recorded as applied", p[0])
		}
		calls := naive.calls
		equalU32(t, naive.submit(beefOf(t, genuine), ""), nil, p[1])
		if naive.calls != calls {
			t.Fatalf("%s: the genuine object reached the manager", p[0])
		}
		real := setup(t, setupOpts{chain: newTestChain(900000, h)})
		equalU32(t, real.submit(beefOf(t, worse), ""), nil, p[0])
		if real.storage.isApplied(genuine.Txid, topic) {
			t.Fatalf("%s: a refusal was recorded", p[0])
		}
		equalU32(t, real.submit(beefOf(t, genuine), ""), genuine.Admits.OutputsToAdmit, p[0]+": the genuine object is admitted")
	}
}

func TestLaggingHeaders(t *testing.T) {
	ch := newTestChain(71000, nil)
	owner := newOwner(bucket, "lagging owner", ch)
	t0 := owner.token(tokenOpts{})
	t1 := owner.token(tokenOpts{})
	height := t0.tx.MerklePath.BlockHeight
	root := ch.roots[height]
	r := setup(t, setupOpts{chain: ch, config: func(c *Config) { c.Served = []string{owner.served()} }})
	ch.orphan(height)
	equalU32(t, r.submit(t1.beef, ""), nil, "refused unmined")
	if r.host.count("bstore_refused_total", map[string]string{"reason": "unmined"}) != 1 {
		t.Fatal("unmined not counted")
	}
	if r.storage.isApplied(t1.txid, topic) {
		t.Fatal("recorded")
	}
	ch.mu.Lock()
	ch.roots[height] = root
	ch.mu.Unlock()
	equalU32(t, r.submit(t1.beef, ""), []uint32{0}, "decided again and admitted")
	if r.status(t1.txid) != "pending" {
		t.Fatal(r.status(t1.txid))
	}
	equalU32(t, r.submit(t0.beef, ""), []uint32{0}, "t0")
	if r.status(t1.txid) != "consistent" {
		t.Fatal(r.status(t1.txid))
	}
	equalStrings(t, r.answer(map[string]string{"bucket": bucket, "owner": owner.hex(), "id": owner.idHex()}), []string{t1.txid}, "latest")
}

func TestBlobTopic(t *testing.T) {
	v, h := readTxVectors(t)
	host := newFakeHost()
	blob, err := NewBlobTopicManager("tm_bstoreblob_"+bucket, host)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"piece", "entry-put", "token-first", "advertisement"} {
		_, err := blob.IdentifyAdmissibleOutputs(bmodule.WithSubmission(context.Background(), beefOf(t, v.byName(t, name))), nil, nil, nil)
		var r *Refused
		if !errors.As(err, &r) || r.Reason != "piece" {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if host.count("bstore_refused_total", map[string]string{"reason": "piece"}) != 4 {
		t.Fatal("piece not counted")
	}
	records, _ := NewRecordsTopicManager(topic, host, headersOf(h), storerec.DefaultMaxBEEF, storerec.DefaultMaxSweepInputs)
	if _, err := records.Admit(context.Background(), beefOf(t, v.byName(t, "piece")), nil); err == nil || err.(*Refused).Reason != "piece" {
		t.Fatalf("records topic: %v", err)
	}
	// Mounted only where OVERLAY_TOPICS names the blob topic.
	r := setup(t, setupOpts{chain: newTestChain(900000, h), overlayTopics: []string{topic, "tm_bstoreblob_" + bucket}})
	if r.m.Module.Topics["tm_bstoreblob_"+bucket] == nil {
		t.Fatal("the blob topic is not mounted")
	}
	equalU32(t, r.submit(beefOf(t, v.byName(t, "piece")), "tm_bstoreblob_"+bucket), nil, "piece")
	equalU32(t, r.submit(beefOf(t, v.byName(t, "entry-put")), "tm_bstoreblob_"+bucket), nil, "entry")
	if len(r.storage.applied) != 0 || len(r.storage.outputs) != 0 {
		t.Fatal("something was stored")
	}
}

func TestManagerRefusesItsTopic(t *testing.T) {
	host := newFakeHost()
	h := headersOf{}
	for _, c := range []struct {
		topic      string
		beef, ins  int
		want, blob string
	}{
		{"tm_bstoreblob_" + bucket, 262144, 1024, "not a tm_bstore_ records topic", ""},
		{"tm_bchat_photos_qzvmkwerty", 262144, 1024, "not a tm_bstore_ records topic", ""},
		{"tm_bstore_Photos_qzvmkwerty", 262144, 1024, "not a bucket identifier", ""},
		{topic, 262143, 1024, "BEEF bound", ""},
		{topic, 262144, 1023, "sweep input bound", ""},
	} {
		if _, err := NewRecordsTopicManager(c.topic, host, h, c.beef, c.ins); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s %d %d: %v", c.topic, c.beef, c.ins, err)
		}
	}
	if _, err := NewBlobTopicManager(topic, host); err == nil || !strings.Contains(err.Error(), "not a tm_bstoreblob_ topic") {
		t.Error(err)
	}
}
