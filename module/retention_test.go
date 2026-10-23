package module

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bstore/storerec"
)

// Retention (spec section 12.3) at the host's clock (retention.test.ts):
// each floor holds, and what passes its floor is dropped: deleted from the
// topic and evicted, so the same object offered again is decided afresh.

func (w *world) heldOut(txid string) bool { return w.r.storage.held(txid, 0, topic) }

func (w *world) entry(txid string) bool {
	return len(w.r.answer(map[string]string{"bucket": bucket, "entry": txid})) == 1
}

func TestRetentionEntries(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	v1 := w.alice.put(w.owner.id, "k", putOpts{label: "one"})
	v2 := w.alice.put(w.owner.id, "k", putOpts{label: "two", created: T0 + 1})
	pending := w.owned.put(w.owner.id, "p", putOpts{})
	stranger := w.bob.put(w.owner.id, "s", putOpts{})
	for _, e := range []*posted{v1, v2, pending, stranger} {
		w.r.submitTx(e.tx, "")
	}
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{v1}}).beef, "")
	w.r.prune(23 * hourDur)
	if !w.entry(stranger.txid) || !w.entry(pending.txid) {
		t.Fatal("within 24 hours, everything")
	}
	w.r.prune(2 * hourDur)
	if w.entry(stranger.txid) || w.heldOut(stranger.txid) {
		t.Fatal("a stranger's entry goes at 24 hours")
	}
	w.r.prune(5 * dayDur)
	// v1 is replaced by v2 now: 31 days from this token's first sight.
	w.r.submit(w.owner.token(tokenOpts{anchor: []*posted{v2}}).beef, "")
	w.r.prune(2 * dayDur)
	if w.entry(pending.txid) || w.heldOut(pending.txid) {
		t.Fatal("the owner's unnamed entry goes at 7 days")
	}
	w.r.prune(28 * dayDur)
	if !w.entry(v1.txid) {
		t.Fatal("replaced 30 days ago: kept")
	}
	w.r.prune(dayDur + hourDur)
	if w.entry(v1.txid) {
		t.Fatal("replaced over 31 days ago: dropped")
	}
	w.r.prune(someDays)
	if o, _ := w.object(t, "k"); o == nil || o.entry != v2.txid {
		t.Fatal("the anchored entry is kept")
	}
	// Dropped means evicted: offered again, the stranger's entry is admitted afresh and answered.
	one(t, w.r.submitTx(stranger.tx, ""), "again")
	if !w.entry(stranger.txid) {
		t.Fatal("not answered again")
	}
	if w.r.host.count("bstore_dropped_total", map[string]string{"why": "retention"}) < 3 {
		t.Fatal("drops not counted")
	}
}

func TestRetentionNamedByUnrooted(t *testing.T) {
	w := newWorld(t, worldOpts{})
	t0 := w.owner.token(tokenOpts{})
	e := w.bob.put(w.owner.id, "k", putOpts{})
	w.r.submitTx(e.tx, "")
	t1 := w.owner.token(tokenOpts{anchor: []*posted{e}})
	w.r.submit(t1.beef, "")
	if w.r.status(t1.txid) != "pending" {
		t.Fatal(w.r.status(t1.txid))
	}
	w.r.prune(6 * dayDur)
	if !w.entry(e.txid) {
		t.Fatal("bob is no writer, but the owner named his entry")
	}
	w.r.submit(t0.beef, "")
	w.r.prune(someDays)
	if !w.entry(e.txid) {
		t.Fatal("anchored once the chain is rooted, and kept")
	}
	// A reader refuses it, since bob is no writer of the token it is proven
	// against (spec section 15.2 step 4).
	if _, err := w.object(t, "k"); err == nil || storerec.Reason(err) != "writer" {
		t.Fatalf("a reader takes it: %v", err)
	}
}

func TestRetentionTokens(t *testing.T) {
	w := newWorld(t, worldOpts{})
	u := newWorld(t, worldOpts{unserved: true, seed: "stranger", chain: w.chain})
	s0 := w.owner.token(tokenOpts{})
	s1 := w.owner.token(tokenOpts{})
	x0 := u.owner.token(tokenOpts{})
	x1 := u.owner.token(tokenOpts{})
	for _, x := range []*minted{s0, s1, x0, x1} {
		w.r.submit(x.beef, "")
	}
	w.r.prune(25 * hourDur)
	equalStrings(t, w.r.answer(w.chainQ), []string{s1.txid}, "served")
	equalStrings(t, w.r.answer(u.chainQ), nil, "other")
	if w.r.storage.held(x1.txid, 0, topic) {
		t.Fatal("the other chain's token is still stored")
	}
	w.r.prune(someDays)
	equalStrings(t, w.r.answer(with(w.chainQ, "seq", "0")), []string{s0.txid}, "for the host's life")
}

func TestRetentionOutcomes(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	e := w.alice.put(w.owner.id, "k", putOpts{})
	w.r.submitTx(e.tx, "")
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e}}).beef, "")
	asOwner := newAuthor(bucket, "w owner", w.chain)
	kept := asOwner.outcome(outcomeOpts{root: e.e.Root, provider: w.prov.party})
	otherRoot := asOwner.outcome(outcomeOpts{root: [32]byte{9, 9}, provider: w.prov.party})
	stranger := w.buyer.outcome(outcomeOpts{root: e.e.Root, provider: w.prov.party})
	for _, o := range []*posted{kept, otherRoot, stranger} {
		w.r.submitTx(o.tx, "")
	}
	w.r.prune(25 * hourDur)
	if !w.heldOut(kept.txid) || w.heldOut(otherRoot.txid) || w.heldOut(stranger.txid) {
		t.Fatal(w.heldOut(kept.txid), w.heldOut(otherRoot.txid), w.heldOut(stranger.txid))
	}
	w.r.prune(30 * dayDur)
	if w.heldOut(kept.txid) {
		t.Fatal("kept past 31 days")
	}
}

func TestRetentionFronts(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	e := w.alice.put(w.owner.id, "k", putOpts{})
	w.r.submitTx(e.tx, "")
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e}}).beef, "")
	w.r.submitTx(w.prov.advertise([][32]byte{e.e.Root}, uint64(w.clock.ms()/1000)+90*86400), "")
	var fronts []*posted
	for i := range 17 {
		f := newAuthor(bucket, fmt.Sprintf("front %d", i), w.chain)
		s := f.statement(w.prov.party.id, 0)
		w.r.submitTx(s.tx, "")
		fronts = append(fronts, s)
		w.clock.advance(time.Second)
	}
	otherAdvertiser := w.front.statement(w.prov2.party.id, 0)
	w.r.submitTx(otherAdvertiser.tx, "")
	w.r.prune(25 * hourDur)
	for i, f := range fronts {
		if w.heldOut(f.txid) != (i < 16) {
			t.Fatalf("front %d held %v", i, w.heldOut(f.txid))
		}
	}
	if w.heldOut(otherAdvertiser.txid) {
		t.Fatal("another advertiser's statement is kept")
	}
	w.r.prune(31 * dayDur)
	if w.heldOut(fronts[0].txid) {
		t.Fatal("kept past 31 days")
	}
}

func TestRetentionAdvertisements(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	e := w.alice.put(w.owner.id, "k", putOpts{})
	w.r.submitTx(e.tx, "")
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e}}).beef, "")
	now := uint64(w.clock.ms() / 1000)
	soon := w.prov.advertise([][32]byte{e.e.Root}, now+10*86400)
	long := w.prov2.advertise([][32]byte{e.e.Root}, now+90*86400)
	other := w.prov.advertise([][32]byte{{3, 3}}, now+90*86400)
	for _, tx := range []*transaction.Transaction{soon, long, other} {
		w.r.submitTx(tx, "")
	}
	held := func(tx *transaction.Transaction) bool { return w.r.storage.held(tx.TxID().String(), 0, topic) }
	w.r.prune(25 * hourDur)
	if !held(soon) || !held(long) || held(other) {
		t.Fatal(held(soon), held(long), held(other))
	}
	w.r.prune(10 * dayDur)
	if held(soon) {
		t.Fatal("expired over 24 hours ago")
	}
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "root": hex.EncodeToString(e.e.Root[:]), "after": ""}), []string{long.TxID().String()}, "live")
	w.r.prune(20 * dayDur)
	if !held(long) {
		t.Fatal("31 days after first sight, plus 24 hours, has not passed")
	}
	w.r.prune(2 * dayDur)
	if held(long) {
		t.Fatal("kept too long")
	}
}

func TestRetentionHolds(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	hA, hP, hX := w.alice.hold(1000), w.owned.hold(1000), w.bob.hold(1000)
	a := w.alice.put(w.owner.id, "a", putOpts{hold: hA.terms})
	p := w.owned.put(w.owner.id, "p", putOpts{hold: hP.terms})
	x := w.bob.put(w.owner.id, "x", putOpts{hold: hX.terms})
	for _, tx := range []*transaction.Transaction{hA.tx, hP.tx, hX.tx, a.tx, p.tx, x.tx} {
		w.r.submitTx(tx, "")
	}
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{a}}).beef, "")
	w.r.prune(25 * hourDur)
	holds := func() string {
		return fmt.Sprint(len(w.r.answer(map[string]string{"bucket": bucket, "hold": hA.op})), len(w.r.answer(map[string]string{"bucket": bucket, "hold": hP.op})),
			len(w.r.answer(map[string]string{"bucket": bucket, "hold": hX.op})))
	}
	if holds() != "1 1 0" {
		t.Fatal(holds())
	}
	w.r.prune(30 * dayDur)
	if holds() != "0 0 0" {
		t.Fatal(holds())
	}
}

func TestRetentionMarks(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}}).beef, "")
	mine := w.alice.put(w.owner.id, "m", putOpts{})
	theirs := w.bob.put(w.owner.id, "s", putOpts{})
	for _, e := range []*posted{mine, theirs} {
		w.r.submitTx(e.tx, "")
	}
	s1 := mintSweep(mine.tree, []uint32{mine.vout}, w.alice.party, storerec.EntryDerivation, w.chain)
	s2 := mintSweep(theirs.tree, []uint32{theirs.vout}, w.bob.party, storerec.EntryDerivation, w.chain)
	for _, s := range []*transaction.Transaction{s1, s2} {
		one(t, w.r.submitTx(s, ""), "sweep")
	}
	_, _, lifeMine, _ := w.r.ls.Mark(mine.op)
	_, _, lifeTheirs, _ := w.r.ls.Mark(theirs.op)
	if !lifeMine || lifeTheirs {
		t.Fatal(lifeMine, lifeTheirs)
	}
	w.r.prune(30 * dayDur)
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "spent": mine.op}), []string{s1.TxID().String()}, "kept 30 days")
	w.r.prune(2 * dayDur)
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "spent": mine.op}), nil, "the sweep's transaction went with its outpoints")
	_, _, _, okMine := w.r.ls.Mark(mine.op)
	_, _, _, okTheirs := w.r.ls.Mark(theirs.op)
	if !okMine || okTheirs || w.r.ls.Swept(theirs.op) {
		t.Fatal(okMine, okTheirs, w.r.ls.Swept(theirs.op))
	}
	// The party's carrier stays retracted whenever it is offered; the
	// stranger's, once both have aged out, is a carrier like any other.
	for _, e := range []*posted{mine, theirs} {
		one(t, w.r.submitTx(e.tx, ""), "again")
	}
	if w.entry(mine.txid) || !w.entry(theirs.txid) {
		t.Fatal(w.entry(mine.txid), w.entry(theirs.txid))
	}
}

func TestRetentionRows(t *testing.T) {
	w := newWorld(t, worldOpts{})
	first := w.owned.put(w.owner.id, "a", putOpts{})
	second := w.owned.put(w.owner.id, "a", putOpts{vout: ptr(first.vout), created: T0 + 5})
	if second.op != first.op {
		t.Fatal("not one outpoint")
	}
	w.r.submitTx(first.tx, "")
	w.r.prune(6 * dayDur)
	if c, _ := w.r.ls.FirstOn(first.op); c != first.c {
		t.Fatal("held: its row stays")
	}
	one(t, w.r.submitTx(second.tx, ""), "second")
	if w.entry(second.txid) {
		t.Fatal("a later carrier on the outpoint has no floor")
	}
	w.r.prune(2 * dayDur)
	if w.entry(first.txid) {
		t.Fatal("the first is past its floor")
	}
	if _, ok := w.r.ls.FirstOn(first.op); ok {
		t.Fatal("nothing held on it and past 24 hours: forgotten")
	}
}
