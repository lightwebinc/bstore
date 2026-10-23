package module

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/lightwebinc/bstore/storerec"
)

// The independent review's findings, each held by a test (review.test.ts):
// a history page never splits a seq; a hold output keeps its floor as long
// as its entry keeps the pending one; a mined output the engine marked
// spent before the spender was journaled is restored and pruned; a sweep is
// kept only in the bucket whose carrier it matched; the latest tokens
// follow the branch whose proofs verify, a longer orphaned branch
// notwithstanding; chain walks only seqs that hold a rooted token.

func TestHistoryNeverSplitsASeq(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	f := w.owner.fork()
	eB := w.owned.put(w.owner.id, "k", putOpts{label: "fork", created: T0 + 999})
	w.r.submitTx(eB.tx, "")
	b1 := f.token(tokenOpts{anchor: []*posted{eB}})
	one(t, w.r.submit(b1.beef, ""), "fork")
	tokens := map[string]uint64{b1.txid: b1.rec.Seq}
	for i := 1; i <= 64; i++ {
		e := w.owned.put(w.owner.id, "k", putOpts{label: fmt.Sprintf("v%d", i), created: T0 + uint64(i)})
		w.r.submitTx(e.tx, "")
		m := w.owner.token(tokenOpts{anchor: []*posted{e}})
		one(t, w.r.submit(m.beef, ""), "token")
		tokens[m.txid] = m.rec.Seq
	}
	seen := map[string]bool{}
	before := "1000"
	for {
		var low uint64 = 1 << 62
		n := 0
		for _, x := range w.r.answer(with(w.chainQ, "history", "k", "before", before)) {
			if seq, ok := tokens[x]; ok {
				seen[x] = true
				n++
				low = min(low, seq)
			}
		}
		if n == 0 {
			break
		}
		before = strconv.FormatUint(low, 10)
	}
	if len(seen) != len(tokens) {
		t.Fatalf("%d of %d tokens", len(seen), len(tokens))
	}
}

func TestHoldOfAnUnrootedNamedEntry(t *testing.T) {
	w := newWorld(t, worldOpts{})
	t0 := w.owner.token(tokenOpts{})
	h := w.alice.hold(1000)
	e := w.alice.put(w.owner.id, "k", putOpts{hold: h.terms})
	w.r.submitTx(h.tx, "")
	w.r.submitTx(e.tx, "")
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e}}).beef, "")
	w.r.prune(25 * hourDur)
	if !w.entry(e.txid) {
		t.Fatal("the entry went")
	}
	w.r.submit(t0.beef, "")
	if len(w.r.answer(map[string]string{"bucket": bucket, "hold": h.op})) != 1 {
		t.Fatal("the hold went")
	}
	w.r.prune(29 * dayDur)
	if len(w.r.answer(map[string]string{"bucket": bucket, "hold": h.op})) != 1 {
		t.Fatal("anchored now: 31 days")
	}
}

func TestSpentBeforeJournaled(t *testing.T) {
	w := newWorld(t, worldOpts{})
	root := sha("r")
	ad := w.prov.advertise([][32]byte{root}, uint64((w.clock.ms()+400*dayMs)/1000))
	w.r.submitTx(ad, "")
	w.r.storage.markSpent(ad.TxID().String(), 0, topic)
	r2 := w.r.restart()
	if r2.ls.Counts().Ads != 1 {
		t.Fatal("not restored, though storage hands only unspent rows")
	}
	equalStrings(t, r2.answer(map[string]string{"bucket": bucket, "root": hex.EncodeToString(root[:]), "after": ""}), nil, "and known spent")
	r2.prune(someDays)
	if r2.storage.held(ad.TxID().String(), 0, topic) {
		t.Fatal("not pruned")
	}
}

func TestSweepKeptInItsBucket(t *testing.T) {
	other := "archive_qzvmkwerty"
	w := newWorld(t, worldOpts{config: func(c *Config) { c.Buckets = []string{bucket, other} }})
	there := newAuthor(other, "w alice", w.chain)
	e := there.put(w.owner.id, "x", putOpts{})
	one(t, w.r.submitTx(e.tx, "tm_bstore_"+other), "carrier")
	sw := mintSweep(e.tree, []uint32{e.vout}, there.party, storerec.EntryDerivation, w.chain)
	one(t, w.r.submitTx(sw, ""), "admitted in this bucket first")
	equalStrings(t, w.r.answer(map[string]string{"bucket": other, "entry": e.txid}), nil, "it retracts the carrier of the other")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "spent": e.op}), nil, "but is not kept here, where nothing matched")
	one(t, w.r.submitTx(sw, "tm_bstore_"+other), "the other bucket")
	equalStrings(t, w.r.answer(map[string]string{"bucket": other, "spent": e.op}), []string{sw.TxID().String()}, "kept in the bucket whose carrier it retracted")
	again := w.r.restart()
	equalStrings(t, again.answer(map[string]string{"bucket": other, "spent": e.op}), []string{sw.TxID().String()}, "after a restart")
}

func TestLatestFollowsTheVerifiedBranch(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.alice.put(w.owner.id, "a", putOpts{})
	b := w.alice.put(w.owner.id, "b", putOpts{})
	for _, e := range []*posted{a, b} {
		w.r.submitTx(e.tx, "")
	}
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	other := w.owner.fork()
	a1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{a}})
	a2 := w.owner.token(tokenOpts{})
	b1 := other.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{b}})
	for _, m := range []*minted{a1, a2, b1} {
		one(t, w.r.submit(m.beef, ""), "token")
	}
	equalStrings(t, w.r.answer(w.chainQ), []string{a2.txid}, "both branches on the chain: the longer")
	// A reorganisation takes branch A's blocks out of the best chain.
	for _, m := range []*minted{a1, a2} {
		w.chain.orphan(m.tx.MerklePath.BlockHeight)
	}
	again := w.r.restart()
	equalStrings(t, again.answer(w.chainQ), []string{b1.txid}, "the branch on the chain")
	ww := *w
	ww.r = again
	if o, err := ww.object(t, "b"); err != nil || o == nil || *o != (objectRead{result: storerec.ObjectFound, entry: b.txid, token: b1.txid}) {
		t.Fatalf("%+v %v", o, err)
	}
	// Retention follows the branch on the chain: b stays anchored, a is no
	// longer named by it.
	again.prune(someDays)
	equalStrings(t, again.answer(map[string]string{"bucket": bucket, "entry": b.txid}), txids(b), "b anchored")
}

func TestChainWalksRootedSeqsOnly(t *testing.T) {
	u := newWorld(t, worldOpts{unserved: true, seed: "unrooted"})
	u.owner.token(tokenOpts{})
	var ts []*minted
	for range 200 {
		ts = append(ts, u.owner.token(tokenOpts{}))
	}
	for _, m := range ts {
		u.r.submit(m.beef, "")
	}
	u.r.ls.mu.Lock()
	v := u.r.ls.chains[bucket+"|"+u.owner.hex()+"|"+u.owner.idHex()]
	bySeq, rooted := len(v.bySeq), v.rootedSeqs.sorted.size()
	u.r.ls.mu.Unlock()
	if bySeq != 200 || rooted != 0 {
		t.Fatal(bySeq, rooted)
	}
	if got := u.r.outputs(map[string]string{"bucket": bucket, "owner": u.owner.hex(), "chain": u.owner.idHex(), "after": "0"}); len(got) != 0 {
		t.Fatal(len(got))
	}
}
