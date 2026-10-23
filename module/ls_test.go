package module

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bstore/storerec"
)

// ls_bstore's classes (spec section 11.1) through the engine's admission
// path (ls_bstore.test.ts): tokens in any order, the fold of a served chain,
// object answers and listing pages checked as a reader checks them,
// pending and unanchored, entries, advertisements, fronts, holds, outcomes,
// sweeps, history and chain, and the questions it refuses.

func one(t *testing.T, got []uint32, what string) {
	t.Helper()
	equalU32(t, got, []uint32{0}, what)
}

func TestTokensInAnyOrder(t *testing.T) {
	w := newWorld(t, worldOpts{})
	e1 := w.alice.put(w.owner.id, "a.txt", putOpts{})
	t0 := w.owner.token(tokenOpts{})
	t1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e1}})
	t2 := w.owner.token(tokenOpts{})
	one(t, w.r.submitTx(e1.tx, ""), "e1")
	one(t, w.r.submit(t2.beef, ""), "t2")
	one(t, w.r.submit(t1.beef, ""), "t1")
	if w.r.status(t2.txid) != "pending" {
		t.Fatal(w.r.status(t2.txid))
	}
	equalStrings(t, w.r.answer(w.chainQ), nil, "nothing is rooted yet")
	one(t, w.r.submit(t0.beef, ""), "t0")
	for _, x := range []*minted{t0, t1, t2} {
		if w.r.status(x.txid) != "consistent" {
			t.Fatalf("%s %s", x.txid, w.r.status(x.txid))
		}
	}
	equalStrings(t, w.r.answer(w.chainQ), []string{t2.txid}, "latest")
	equalStrings(t, w.r.answer(with(w.chainQ, "seq", "1")), []string{t1.txid}, "seq 1")
	cq := map[string]string{"bucket": bucket, "owner": w.owner.hex(), "chain": w.owner.idHex(), "after": "0"}
	equalStrings(t, w.r.answer(cq), []string{t1.txid, t2.txid}, "chain after 0")
	equalStrings(t, w.r.answer(with(cq, "after", "2")), nil, "chain after 2")
	if w.r.host.count("bstore_token_status_total", map[string]string{"status": "consistent"}) != 3 {
		t.Fatal("statuses not counted")
	}
}

func TestMapMismatchIsSticky(t *testing.T) {
	w := newWorld(t, worldOpts{})
	e1 := w.alice.put(w.owner.id, "a.txt", putOpts{})
	e2 := w.alice.put(w.owner.id, "b.txt", putOpts{})
	t0 := w.owner.token(tokenOpts{})
	t1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e1}})
	// The next token claims a root its changes do not reproduce.
	bad := w.owner.token(tokenOpts{anchor: []*posted{e2}, mutate: func(b *storerec.Bucket) { b.MapRoot = [32]byte{7, 7, 7} }})
	after := w.owner.token(tokenOpts{})
	for _, x := range []*posted{e1, e2} {
		w.r.submitTx(x.tx, "")
	}
	for _, x := range []*minted{t0, t1, bad, after} {
		one(t, w.r.submit(x.beef, ""), x.txid)
	}
	if w.r.status(bad.txid) != "map-mismatch" || w.r.status(after.txid) != "map-mismatch" {
		t.Fatal(w.r.status(bad.txid), w.r.status(after.txid))
	}
	equalStrings(t, w.r.answer(w.chainQ), []string{t1.txid}, "latest")
	o, err := w.object(t, "b.txt")
	if err != nil || o == nil || o.result != storerec.ObjectAbsent || o.token != t1.txid {
		t.Fatalf("b.txt is absent from the map of the last consistent token: %+v %v", o, err)
	}
	if !w.r.host.logged("ls_bstore token status", func(m map[string]any) bool { return m["reason"] == "map-root" }) {
		t.Fatal("the break is not logged")
	}
	// An unserved chain is rooted, never folded.
	u := newWorld(t, worldOpts{unserved: true, seed: "u"})
	u0 := u.owner.token(tokenOpts{})
	u1 := u.owner.token(tokenOpts{mutate: func(b *storerec.Bucket) { b.Count = 5 }})
	for _, x := range []*minted{u0, u1} {
		u.r.submit(x.beef, "")
	}
	if u.r.status(u1.txid) != "rooted" {
		t.Fatal(u.r.status(u1.txid))
	}
	equalStrings(t, u.r.answer(u.chainQ), []string{u1.txid}, "unserved latest")
	if msg := u.r.refused(with(u.chainQ, "key", "a")); !strings.Contains(msg, "does not serve") {
		t.Fatal(msg)
	}
}

func TestObject(t *testing.T) {
	w := newWorld(t, worldOpts{})
	var es []*posted
	for _, k := range []string{"a.txt", "c.txt", "é/x", "z"} {
		es = append(es, w.alice.put(w.owner.id, k, putOpts{}))
	}
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	for _, e := range es[:3] {
		w.r.submitTx(e.tx, "")
	}
	t1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: es})
	w.r.submit(t1.beef, "")
	check := func(key string, want objectRead) {
		t.Helper()
		o, err := w.object(t, key)
		if err != nil || o == nil || *o != want {
			t.Fatalf("%s: %+v %v, want %+v", key, o, err, want)
		}
	}
	check("c.txt", objectRead{result: storerec.ObjectFound, entry: es[1].txid, token: t1.txid})
	check("b.txt", objectRead{result: storerec.ObjectAbsent, token: t1.txid})
	check("0", objectRead{result: storerec.ObjectAbsent, token: t1.txid})
	check("zz", objectRead{result: storerec.ObjectAbsent, token: t1.txid})
	// z's entry was never submitted here: the key exists and this host cannot produce it.
	check("z", objectRead{result: storerec.ObjectUnheld, token: t1.txid})
	w.r.submitTx(es[3].tx, "")
	check("z", objectRead{result: storerec.ObjectFound, entry: es[3].txid, token: t1.txid})
	// A delete removes the key at the next token.
	d := w.alice.del(w.owner.id, "c.txt", T0+1)
	w.r.submitTx(d.tx, "")
	t2 := w.owner.token(tokenOpts{anchor: []*posted{d}})
	w.r.submit(t2.beef, "")
	check("c.txt", objectRead{result: storerec.ObjectAbsent, token: t2.txid})
}

func TestListAndExport(t *testing.T) {
	w := newWorld(t, worldOpts{})
	keys := make([]string, 300)
	for i := range keys {
		keys[i] = fmt.Sprintf("k/%03d", i)
	}
	var es []*posted
	for _, k := range keys {
		es = append(es, w.alice.put(w.owner.id, k, putOpts{}))
	}
	es = append(es, w.alice.put(w.owner.id, "j", putOpts{}), w.alice.put(w.owner.id, "l", putOpts{}))
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	missing := es[10]
	for _, e := range es {
		if e != missing {
			w.r.submitTx(e.tx, "")
		}
	}
	t1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: es})
	w.r.submit(t1.beef, "")
	// The first page stops at the entry this host cannot produce, k/010.
	p1 := w.list(t, "k/", "")
	equalStrings(t, p1.keys, keys[:10], "page 1 keys")
	equalStrings(t, p1.leaves, []string{"0 j", "2 k/010"}, "page 1 leaves")
	// Asked after it: a full page of 256.
	p2 := w.list(t, "k/", "k/010")
	equalStrings(t, p2.keys, keys[11:11+256], "page 2 keys")
	equalStrings(t, p2.leaves, []string{"0 k/010"}, "page 2 leaves")
	// The last page: role 1 strictly after the range.
	p3 := w.list(t, "k/", keys[266])
	equalStrings(t, p3.keys, keys[267:], "page 3 keys")
	equalStrings(t, p3.leaves, []string{"0 " + keys[266], "1 l"}, "page 3 leaves")
	// An empty range brackets nothing.
	empty := w.list(t, "m/", "")
	equalStrings(t, empty.keys, nil, "empty keys")
	equalStrings(t, empty.leaves, []string{"0 l"}, "empty leaves")
	// export: as list with no prefix, 512 a page, priced classes answered
	// through the engine where the host prices nothing.
	w.r.submitTx(missing.tx, "")
	ex := readPage(t, w.r.outputs(map[string]string{"bucket": bucket, "owner": w.owner.hex(), "export": w.owner.idHex(), "after": ""}), "", "", true)
	equalStrings(t, ex.keys, append(append([]string{"j"}, keys...), "l"), "export keys")
	equalStrings(t, ex.leaves, nil, "export leaves")
}

func TestPending(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	a1 := w.alice.put(w.owner.id, "p", putOpts{created: T0})
	a2 := w.alice.put(w.owner.id, "p", putOpts{created: T0 + 2})
	o1 := w.owned.put(w.owner.id, "p", putOpts{created: T0 + 1})
	s1 := w.bob.put(w.owner.id, "p", putOpts{created: T0 + 3})
	future := w.owned.put(w.owner.id, "p", putOpts{created: T0 + 2*uint64(dayMs)})
	other := w.alice.put(w.owner.id, "q", putOpts{})
	for _, e := range []*posted{a1, a2, o1, s1, future, other} {
		w.r.submitTx(e.tx, "")
	}
	q := with(w.chainQ, "pending", "p")
	equalStrings(t, w.r.answer(q), txids(o1), "alice is not a writer yet; bob never; the future one never")
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}}).beef, "")
	equalStrings(t, w.r.answer(q), txids(a2, o1, a1), "alice's too, now the latest token names her")
	w.r.submit(w.owner.token(tokenOpts{anchor: []*posted{a1, o1}}).beef, "")
	equalStrings(t, w.r.answer(q), txids(a2), "named entries are not pending, whatever the token did with them")
	if o, _ := w.object(t, "p"); o == nil || o.entry != o1.txid {
		t.Fatalf("the last by created won the batch: %+v", o)
	}
}

func TestUnanchored(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	var es []*posted
	for i := range 70 {
		es = append(es, w.alice.put(w.owner.id, fmt.Sprintf("u/%d", i), putOpts{}))
	}
	for _, e := range es {
		w.r.submitTx(e.tx, "")
	}
	all := sorted(txids(es...))
	q := with(w.chainQ, "writer", w.alice.hex(), "after", "")
	equalStrings(t, w.r.answer(q), all[:64], "page 1")
	equalStrings(t, w.r.answer(with(q, "after", all[63])), all[64:], "page 2")
	// Anchoring names them.
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: es[:60]}).beef, "")
	equalStrings(t, w.r.answer(q), sorted(txids(es[60:]...)), "the rest")
	// Admitted more than 7 days ago: no longer answered.
	w.clock.advance(8 * dayDur)
	equalStrings(t, w.r.answer(q), nil, "aged")
	equalStrings(t, w.r.answer(with(w.chainQ, "writer", w.bob.hex(), "after", "")), nil, "bob")
}

func TestEntryHoldAds(t *testing.T) {
	w := newWorld(t, worldOpts{})
	h := w.alice.hold(5000)
	e := w.alice.put(w.owner.id, "held", putOpts{hold: h.terms})
	w.r.submitTx(e.tx, "")
	one(t, w.r.submitTx(h.tx, ""), "hold")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "entry": e.txid}), txids(e), "entry")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "hold": h.op}), []string{h.tx.TxID().String()}, "hold")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "hold": h.tx.TxID().String() + ".1"}), nil, "not a hold")
	root := e.e.Root
	r := hex.EncodeToString(root[:])
	exp := uint64(w.clock.ms()/1000) + 3600
	var ads []*transaction.Transaction
	for i := range 5 {
		roots := make([][32]byte, 16)
		for j := range roots {
			roots[j] = root
		}
		ads = append(ads, w.prov.advertise(roots, exp+uint64(i)))
	}
	for _, tx := range ads {
		if got := w.r.submitTx(tx, ""); len(got) != 16 {
			t.Fatalf("admitted %v", got)
		}
	}
	var byTxid []string
	for _, tx := range ads {
		byTxid = append(byTxid, tx.TxID().String())
	}
	byTxid = sorted(byTxid)
	page := w.r.outputs(map[string]string{"bucket": bucket, "root": r, "after": ""})
	// 64 outputs a page, four whole transactions.
	if len(page) != 64 {
		t.Fatalf("%d outputs", len(page))
	}
	equalStrings(t, distinct(page), byTxid[:4], "page 1")
	equalStrings(t, distinctS(w.r.answer(map[string]string{"bucket": bucket, "root": r, "after": byTxid[3]})), byTxid[4:], "page 2")
	// ad: by advertiser, at most 8; another advertiser's are not answered.
	other := w.prov2.advertise([][32]byte{root}, exp)
	w.r.submitTx(other, "")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "root": r, "advertiser": w.prov2.hex()}), []string{other.TxID().String()}, "prov2")
	want := make([]string, 8)
	for i := range want {
		want[i] = byTxid[0]
	}
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "root": r, "advertiser": w.prov.hex()}), want, "the first eight, at most")
	// A renewal spends the advertisement it renews: the old one is no longer answered.
	renewed := w.prov2.renew(other, 0, root, exp+7200)
	one(t, w.r.submitTx(renewed, ""), "renewal")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "root": r, "advertiser": w.prov2.hex()}), []string{renewed.TxID().String()}, "renewed")
	// Expired by the host's clock: not answered.
	w.clock.advance(3 * hourDur)
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "root": r, "after": ""}), nil, "expired")
}

func distinct(a []answered) []string {
	var out []string
	for _, x := range a {
		if len(out) == 0 || out[len(out)-1] != x.txid {
			out = append(out, x.txid)
		}
	}
	return out
}

func distinctS(a []string) []string {
	var out []string
	for _, x := range a {
		if len(out) == 0 || out[len(out)-1] != x {
			out = append(out, x)
		}
	}
	return out
}

func TestAnyoneAdvertisement(t *testing.T) {
	v, h := readTxVectors(t)
	w := newWorld(t, worldOpts{chain: newTestChain(900000, h)})
	c := v.byName(t, "advertisement-anyone-key")
	equalU32(t, w.r.submit(beefOf(t, c), ""), c.Admits.OutputsToAdmit, "admitted")
	ad, err := storerec.DecodeAdvertisement(*txOf(t, c).Outputs[c.Admits.OutputsToAdmit[0]].LockingScript)
	if err != nil {
		t.Fatal(err)
	}
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "root": hex.EncodeToString(ad.Hash[:]), "after": ""}), nil, "never answered")
}

func TestFronts(t *testing.T) {
	w := newWorld(t, worldOpts{})
	var fs []*posted
	for i := range 10 {
		fs = append(fs, w.front.statement(w.prov.party.id, T0+uint64(i)))
	}
	later := w.front.statement(w.prov.party.id, T0+2*uint64(dayMs))
	for _, f := range append(fs, later) {
		one(t, w.r.submitTx(f.tx, ""), "front")
	}
	all := sorted(txids(append(fs, later)...))
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "advertiser": w.prov.hex(), "after": ""}), all[:8], "fronts 1")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "advertiser": w.prov.hex(), "after": all[7]}), all[8:], "fronts 2")
	var newest []string
	for i := len(fs) - 1; i >= 2; i-- {
		newest = append(newest, fs[i].txid)
	}
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "advertiser": w.prov.hex(), "front": w.front.hex()}), newest, "newest first, never one dated after the host time")
}

func TestOutcomes(t *testing.T) {
	w := newWorld(t, worldOpts{})
	root := [32]byte{1, 1, 1}
	var os []*posted
	for i := range 3 {
		os = append(os, w.buyer.outcome(outcomeOpts{root: root, provider: w.prov.party, created: T0 + uint64(i)}))
	}
	pass := w.buyer.outcome(outcomeOpts{root: root, provider: w.prov.party, answerer: w.front.party, outcome: storerec.OutcomePass, created: T0 + 10})
	future := w.buyer.outcome(outcomeOpts{root: root, provider: w.prov.party, created: T0 + 2*uint64(dayMs)})
	byBob := newAuthor(bucket, "outcome bob", w.chain).outcome(outcomeOpts{root: root, provider: w.prov.party, created: T0 + 5})
	for _, o := range append(os, pass, future, byBob) {
		one(t, w.r.submitTx(o.tx, ""), "outcome")
	}
	q := func(kv ...string) []string { return w.r.answer(with(map[string]string{"bucket": bucket}, kv...)) }
	equalStrings(t, q("answerer", w.prov.hex()), txids(byBob, os[2], os[1], os[0]), "outcomes")
	equalStrings(t, q("answerer", w.front.party.hex), txids(pass), "outcomes front")
	equalStrings(t, q("provider", w.prov.hex()), txids(pass, byBob, os[2], os[1], os[0]), "outcomes-of")
	equalStrings(t, q("answerer", w.prov.hex(), "buyer", w.buyer.hex()), txids(os[2], os[1], os[0]), "outcomes-by")
	equalStrings(t, q("provider", w.prov.hex(), "buyer", w.buyer.hex()), txids(pass, os[2], os[1], os[0]), "outcomes-of-by")
	// outcome-history: before a cursor, no window.
	cursor := fmt.Sprintf("%d:%s", T0+2, os[2].txid)
	equalStrings(t, q("outcome-history", w.prov.hex(), "before", cursor), txids(os[1], os[0]), "outcome-history")
	equalStrings(t, q("outcome-history-of", w.prov.hex(), "before", fmt.Sprintf("%d:%s", T0+3*uint64(dayMs), strings.Repeat("0", 64))), txids(pass, byBob, os[2], os[1], os[0]), "outcome-history-of")
	w.clock.advance(32 * dayDur)
	equalStrings(t, q("answerer", w.prov.hex()), nil, "admitted more than 31 days ago")
	equalStrings(t, q("outcome-history", w.prov.hex(), "before", cursor), txids(os[1], os[0]), "history has no window")
}

func TestHistory(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	v1 := w.alice.put(w.owner.id, "h", putOpts{label: "one"})
	v2 := w.alice.put(w.owner.id, "h", putOpts{label: "two", created: T0 + 1})
	d := w.alice.del(w.owner.id, "h", T0+2)
	other := w.alice.put(w.owner.id, "x", putOpts{})
	for _, e := range []*posted{v1, v2, d, other} {
		w.r.submitTx(e.tx, "")
	}
	t1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{v1}})
	t2 := w.owner.token(tokenOpts{anchor: []*posted{other}})
	t3 := w.owner.token(tokenOpts{anchor: []*posted{v2}})
	t4 := w.owner.token(tokenOpts{anchor: []*posted{d}})
	for _, x := range []*minted{t1, t2, t3, t4} {
		w.r.submit(x.beef, "")
	}
	q := with(w.chainQ, "history", "h", "before", "9")
	equalStrings(t, w.r.answer(q), []string{t4.txid, d.txid, t3.txid, v2.txid, t1.txid, v1.txid}, "history")
	equalStrings(t, w.r.answer(with(q, "before", "4")), []string{t3.txid, v2.txid, t1.txid, v1.txid}, "history before 4")
	_ = t2
}

func TestSweep(t *testing.T) {
	w := newWorld(t, worldOpts{})
	e := w.alice.put(w.owner.id, "s", putOpts{})
	w.r.submitTx(e.tx, "")
	sw := mintSweep(e.tree, []uint32{e.vout, 40}, w.alice.party, storerec.EntryDerivation, w.chain)
	one(t, w.r.submitTx(sw, ""), "sweep")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "entry": e.txid}), nil, "retracted")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "spent": e.op}), []string{sw.TxID().String()}, "by the carrier's outpoint")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "spent": e.tree.TxID().String() + ".40"}), []string{sw.TxID().String()}, "by the other outpoint")
	if w.r.host.count("bstore_retractions_total", nil) != 1 {
		t.Fatal("retraction not counted")
	}
	// The replay: admitted by the manager, which reads nothing the topic
	// holds, and dropped by the index at once.
	one(t, w.r.submitTx(e.tx, ""), "replay")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "entry": e.txid}), nil, "still retracted")
	if w.r.storage.held(e.txid, 0, "") {
		t.Fatal("not deleted again")
	}
	if w.r.storage.isApplied(e.txid, topic) {
		t.Fatal("not evicted, so a replay would not be decided afresh")
	}
	// A sweep that matched nothing: its transaction is dropped, its
	// outpoints kept, and a carrier that comes later is retracted.
	late := w.buyer.outcome(outcomeOpts{provider: w.prov.party})
	sw2 := mintSweep(late.tree, []uint32{late.vout}, w.buyer.party, storerec.OutcomeDerivation, w.chain)
	one(t, w.r.submitTx(sw2, ""), "unmatched sweep")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "spent": late.op}), nil, "its transaction is not kept")
	if !w.r.ls.Swept(late.op) {
		t.Fatal("its outpoints are not kept")
	}
	one(t, w.r.submitTx(late.tx, ""), "late carrier")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "answerer": w.prov.hex()}), nil, "retracted on arrival")
	if _, _, _, ok := w.r.ls.Mark(late.op); !ok {
		t.Fatal("no mark")
	}
}

func TestOneCarrierPerOutpoint(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	first := w.alice.put(w.owner.id, "k", putOpts{created: T0})
	second := w.alice.put(w.owner.id, "k", putOpts{created: T0 + 1, vout: ptr(first.vout)})
	third := w.alice.put(w.owner.id, "k", putOpts{created: T0 + 2, vout: ptr(first.vout)})
	if second.op != first.op {
		t.Fatal("not one outpoint")
	}
	for _, e := range []*posted{first, second} {
		one(t, w.r.submitTx(e.tx, ""), "carrier")
	}
	if c, _ := w.r.ls.FirstOn(first.op); c != first.c {
		t.Fatal("first carrier")
	}
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "entry": second.txid}), nil, "second dropped")
	if w.r.host.count("bstore_dropped_total", map[string]string{"why": "second-carrier"}) != 1 {
		t.Fatal("not counted")
	}
	// Once a served chain names it, the same carrier offered again is held.
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{third}}).beef, "")
	one(t, w.r.submitTx(third.tx, ""), "third")
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "entry": third.txid}), txids(third), "third held")
	if o, _ := w.object(t, "k"); o == nil || o.entry != third.txid {
		t.Fatalf("%+v", o)
	}
}

func TestQuestionsRefused(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a64 := strings.Repeat("a", 64)
	for _, q := range []map[string]string{
		{},
		{"bucket": bucket},
		{"bucket": "other_qzvmkwerty", "entry": a64},
		{"bucket": bucket, "entry": strings.Repeat("A", 64)},
		{"bucket": bucket, "entry": a64, "extra": "1"},
		{"bucket": bucket, "root": a64, "after": "x"},
		with(w.chainQ, "owner", "02"+strings.Repeat("ab", 32), "key", "a"),
		with(w.chainQ, "key", "a\n"),
		{"bucket": bucket, "owner": w.owner.hex(), "export": strings.Repeat("0", 64), "after": ""},
		{"bucket": bucket, "hold": a64 + ".01"},
		{"bucket": bucket, "answerer": "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"},
	} {
		if msg := w.r.refused(q); !strings.Contains(msg, "ls_bstore: refused") {
			t.Fatalf("%v: %s", q, msg)
		}
	}
	q := question(map[string]string{"bucket": bucket})
	q.Service = "ls_bchat"
	if _, err := w.r.ls.Lookup(t.Context(), q); err == nil || !strings.Contains(err.Error(), "this service is ls_bstore") {
		t.Fatal(err)
	}
	// A question that is not a JSON object of strings.
	q = question(nil)
	q.Query = []byte(`{"bucket":1}`)
	if _, err := w.r.ls.Lookup(t.Context(), q); err == nil || !strings.Contains(err.Error(), "ls_bstore: refused") {
		t.Fatal(err)
	}
}
