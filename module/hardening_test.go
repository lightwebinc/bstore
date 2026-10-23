package module

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

// What keeps the index honest across crashes, restarts and races
// (hardening.test.ts): the journal file, restore in any order to the same
// answers, retention never against a restore in flight, drops that wait
// for the engine's record and never delete what was offered again, the
// first carrier of an outpoint and the retraction marks across a
// compaction and a restart, reorganisations, and the bound on tokens at
// one seq.

func TestJournalFile(t *testing.T) {
	dir := t.TempDir()
	j, err := NewFileJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := strings.Repeat("a", 64)
	if err := j.Append(Line{K: "c", Op: a + ".0", C: strings.Repeat("b", 64), At: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.Append(Line{K: "x", Op: a + ".1", Key: "02" + strings.Repeat("c", 64), At: 2, Life: true}); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	path := filepath.Join(dir, JournalFile)
	text, _ := os.ReadFile(path)
	want := `{"k":"c","op":"` + a + `.0","c":"` + strings.Repeat("b", 64) + `","at":1}` + "\n" + `{"k":"x","op":"` + a + `.1","key":"02` + strings.Repeat("c", 64) + `","at":2,"life":true}` + "\n"
	if string(text) != want {
		t.Fatalf("bytes:\n%s", text)
	}
	_ = os.WriteFile(path, append(text, []byte(`{"k":"c","op":"`)...), 0o600)
	lines, err := (&FileJournal{Dir: dir, Path: path}).Load()
	if err != nil || len(lines) != 2 {
		t.Fatalf("%d %v", len(lines), err)
	}
	if text, _ = os.ReadFile(path); !bytes.HasSuffix(text, []byte("}\n")) {
		t.Fatal("the cut line is still in the file")
	}
	_ = os.WriteFile(path, append(text, []byte("{\"k\":\"zz\"}\n{\"k\":\"c\"}\n")...), 0o600)
	if _, err := (&FileJournal{Dir: dir, Path: path}).Load(); err == nil || !strings.Contains(err.Error(), "line 3 is not a bstore row") {
		t.Fatal(err)
	}
	_ = os.WriteFile(path, []byte(`{"k":"x","op":"nope","key":"02","at":1,"life":true}`+"\n"), 0o600)
	if _, err := (&FileJournal{Dir: dir, Path: path}).Load(); err == nil || !strings.Contains(err.Error(), "line 1 is not a bstore row") {
		t.Fatal(err)
	}
	_ = os.WriteFile(path, []byte("not json\n"), 0o600)
	if _, err := (&FileJournal{Dir: dir, Path: path}).Load(); err == nil || !strings.Contains(err.Error(), "line 1 is not JSON") {
		t.Fatal(err)
	}
	k, _ := NewFileJournal(dir)
	p := Line{K: "p", Op: strings.Repeat("d", 64) + ".7", S: strings.Repeat("e", 64), At: 3}
	if err := k.Rewrite([]Line{p}); err != nil {
		t.Fatal(err)
	}
	if got, err := k.Load(); err != nil || len(got) != 1 || got[0] != p {
		t.Fatalf("%+v %v", got, err)
	}
}

// TestJournalBytes holds every line kind to the TypeScript module's bytes
// (JSON.stringify of the object journal.ts builds, keys in its order), and
// each read back as journal.ts's isLine reads it.
func TestJournalBytes(t *testing.T) {
	op, c, txid, key := strings.Repeat("a", 64)+".0", strings.Repeat("b", 64), strings.Repeat("c", 64), "02"+strings.Repeat("d", 64)
	for _, x := range []struct {
		l    Line
		want string
	}{
		{Line{K: "c", Op: op, C: c, At: 1798761600000}, `{"k":"c","op":"` + op + `","c":"` + c + `","at":1798761600000}`},
		{Line{K: "cf", Op: op}, `{"k":"cf","op":"` + op + `"}`},
		{Line{K: "a", B: bucket, C: c, At: 5}, `{"k":"a","b":"` + bucket + `","c":"` + c + `","at":5}`},
		{Line{K: "af", C: c}, `{"k":"af","c":"` + c + `"}`},
		{Line{K: "x", Op: op, Key: key, At: 6, Life: false, B: bucket}, `{"k":"x","op":"` + op + `","key":"` + key + `","at":6,"life":false,"b":"` + bucket + `"}`},
		{Line{K: "x", Op: op, Key: key, At: 6, Life: true}, `{"k":"x","op":"` + op + `","key":"` + key + `","at":6,"life":true}`},
		{Line{K: "xl", Op: op}, `{"k":"xl","op":"` + op + `"}`},
		{Line{K: "xf", Op: op}, `{"k":"xf","op":"` + op + `"}`},
		{Line{K: "p", Op: op, S: txid, At: 7}, `{"k":"p","op":"` + op + `","s":"` + txid + `","at":7}`},
		{Line{K: "pf", Op: op}, `{"k":"pf","op":"` + op + `"}`},
		{Line{K: "t", B: bucket, Txid: txid, Vout: 0, At: 8}, `{"k":"t","b":"` + bucket + `","txid":"` + txid + `","vout":0,"at":8}`},
		{Line{K: "tf", Txid: txid}, `{"k":"tf","txid":"` + txid + `"}`},
		{Line{K: "o", B: bucket, Txid: txid, Vout: 3, At: 9}, `{"k":"o","b":"` + bucket + `","txid":"` + txid + `","vout":3,"at":9}`},
		{Line{K: "w", B: bucket, Txid: txid, Vout: 3}, `{"k":"w","b":"` + bucket + `","txid":"` + txid + `","vout":3}`},
		{Line{K: "of", B: bucket, Txid: txid, Vout: 3}, `{"k":"of","b":"` + bucket + `","txid":"` + txid + `","vout":3}`},
	} {
		got, err := x.l.encode()
		if err != nil || string(got) != x.want+"\n" {
			t.Fatalf("%s: %s %v", x.l.K, got, err)
		}
		back, ok := parseLine(got)
		if !ok || *back != x.l {
			t.Fatalf("%s: read back %+v %v", x.l.K, back, ok)
		}
	}
	// isLine's refusals: a wrong shape in any member.
	for _, bad := range []string{
		`{"k":"c","op":"` + op + `","c":"` + c + `"}`,
		`{"k":"c","op":"` + op + `","c":"` + c + `","at":-1}`,
		`{"k":"c","op":"` + op + `","c":"` + c + `","at":1.5}`,
		`{"k":"c","op":"` + strings.Repeat("a", 64) + `.01","c":"` + c + `","at":1}`,
		`{"k":"a","b":"Photos","c":"` + c + `","at":1}`,
		`{"k":"x","op":"` + op + `","key":"` + key + `","at":1,"life":"yes"}`,
		`{"k":"t","b":"` + bucket + `","txid":"` + strings.ToUpper(txid) + `","vout":0,"at":1}`,
		`[1]`, `{"k":"q"}`,
	} {
		if _, ok := parseLine([]byte(bad)); ok {
			t.Errorf("read %s", bad)
		}
	}
}

type busyWorld struct {
	w        *world
	snapshot func(r *rig) string
}

// busy is a world with a bit of everything: tokens spent by their
// successors, entries anchored, replaced and pending, a renewed
// advertisement, fronts, outcomes, a hold, and a sweep.
func busy(t *testing.T) busyWorld {
	w := newWorld(t, worldOpts{})
	h := w.alice.hold(5000)
	e1 := w.alice.put(w.owner.id, "a", putOpts{hold: h.terms})
	e2 := w.alice.put(w.owner.id, "b", putOpts{})
	e3 := w.alice.put(w.owner.id, "a", putOpts{label: "again", created: T0 + 1})
	p := w.owned.put(w.owner.id, "p", putOpts{})
	doomed := w.bob.put(w.owner.id, "doomed", putOpts{})
	now := uint64(w.clock.ms() / 1000)
	ad := w.prov.advertise([][32]byte{e1.e.Root}, now+30*86400)
	renewed := w.prov.renew(ad, 0, e1.e.Root, now+60*86400)
	f := w.front.statement(w.prov.party.id, 0)
	o := w.buyer.outcome(outcomeOpts{root: e1.e.Root, provider: w.prov.party})
	for _, x := range []*posted{e1, e2, e3, p, doomed, f, o} {
		w.r.submitTx(x.tx, "")
	}
	w.r.submitTx(h.tx, "")
	w.r.submitTx(ad, "")
	w.r.submitTx(renewed, "")
	t0 := w.owner.token(tokenOpts{})
	t1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e1, e2}})
	t2 := w.owner.token(tokenOpts{anchor: []*posted{e3}})
	for _, x := range []*minted{t0, t1, t2} {
		w.r.submit(x.beef, "")
	}
	sw := mintSweep(doomed.tree, []uint32{doomed.vout}, w.bob.party, storerec.EntryDerivation, w.chain)
	w.r.submitTx(sw, "")
	q := w.chainQ
	root := hex.EncodeToString(e1.e.Root[:])
	snapshot := func(r *rig) string {
		ctxOf := func(qq map[string]string) []string {
			var out []string
			for _, x := range r.outputs(qq) {
				out = append(out, x.txid+" "+hex.EncodeToString(x.context))
			}
			return out
		}
		var st []string
		for _, x := range []*minted{t0, t1, t2} {
			st = append(st, r.status(x.txid))
		}
		parts := [][]string{
			r.answer(q), r.answer(with(q, "seq", "0")), ctxOf(with(q, "key", "a")), ctxOf(with(q, "prefix", "", "after", "")),
			r.answer(with(q, "pending", "p")), r.answer(with(q, "writer", w.alice.hex(), "after", "")),
			r.answer(map[string]string{"bucket": bucket, "entry": e1.txid}), r.answer(map[string]string{"bucket": bucket, "entry": doomed.txid}),
			r.answer(map[string]string{"bucket": bucket, "root": root, "after": ""}), r.answer(map[string]string{"bucket": bucket, "advertiser": w.prov.hex(), "after": ""}),
			r.answer(map[string]string{"bucket": bucket, "provider": w.prov.hex()}), r.answer(map[string]string{"bucket": bucket, "hold": h.op}),
			r.answer(map[string]string{"bucket": bucket, "spent": doomed.op}), r.answer(with(q, "history", "a", "before", "9")),
			r.answer(map[string]string{"bucket": bucket, "owner": w.owner.hex(), "chain": w.owner.idHex(), "after": "0"}), st,
		}
		return fmt.Sprint(parts)
	}
	return busyWorld{w: w, snapshot: snapshot}
}

func TestRestoreInAnyOrder(t *testing.T) {
	b := busy(t)
	before := b.snapshot(b.w.r)
	if !strings.Contains(before, "] [") {
		t.Fatal(before)
	}
	again := b.w.r.restart()
	if got := b.snapshot(again); got != before {
		t.Fatalf("after a restart:\n%s\nwant\n%s", got, before)
	}
	// Storage's rows in reverse, and shuffled: the same.
	rows := again.storage.utxos(topic)
	rev := make([]bmodule.Output, len(rows))
	for i, o := range rows {
		rev[len(rows)-1-i] = o
	}
	shuffled := append([]bmodule.Output(nil), rows...)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	noBEEF := append([]bmodule.Output(nil), rows...)
	for i := range noBEEF {
		noBEEF[i].Beef = nil
	}
	for name, order := range map[string][]bmodule.Output{"reverse": rev, "shuffled": shuffled, "without BEEF": noBEEF, "none": nil} {
		if _, err := again.ls.Restore(context.Background(), order, again.storage); err != nil {
			t.Fatal(err)
		}
		if got := b.snapshot(again); got != before && name != "none" {
			t.Fatalf("%s:\n%s\nwant\n%s", name, got, before)
		}
	}
	if _, err := again.ls.Restore(context.Background(), rows, again.storage); err != nil {
		t.Fatal(err)
	}
	// The renewed advertisement, though spent, is restored, and goes at its floor.
	b.w.clock.advance(70 * dayDur)
	again.ls.Prune(context.Background())
	if again.ls.Counts().Ads != 0 {
		t.Fatal(again.ls.Counts().Ads)
	}
}

func TestRetentionWaitsForRestore(t *testing.T) {
	b := busy(t)
	w := b.w
	rows := w.r.storage.utxos(topic)
	gate := make(chan struct{})
	w.r.storage.slow = gate
	restored := make(chan error, 1)
	go func() {
		_, err := w.r.ls.Restore(context.Background(), rows, w.r.storage)
		restored <- err
	}()
	time.Sleep(20 * time.Millisecond)
	w.clock.advance(someDays)
	pruned := make(chan int, 1)
	go func() { pruned <- w.r.ls.Prune(context.Background()) }()
	late := w.alice.put(w.owner.id, "late", putOpts{})
	admitted := make(chan []uint32, 1)
	go func() { got, _ := w.r.admit(atomicOf(t, late.tx), topic); admitted <- got }()
	time.Sleep(50 * time.Millisecond)
	// The restore holds the index; retention and the admission wait for it.
	select {
	case <-pruned:
		t.Fatal("retention ran against a restore in flight")
	case <-admitted:
		t.Fatal("an admission ran against a restore in flight")
	default:
	}
	close(gate)
	if err := <-restored; err != nil {
		t.Fatal(err)
	}
	if n := <-pruned; n <= 0 {
		t.Fatalf("retention ran once the restore was done: %d", n)
	}
	<-admitted
	w.r.ls.Flush(context.Background())
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "entry": late.txid}), txids(late), "the late entry")
}

func TestDropWaitsForTheEngine(t *testing.T) {
	w := newWorld(t, worldOpts{})
	e := w.alice.put(w.owner.id, "s", putOpts{})
	w.r.submitTx(e.tx, "")
	sw := mintSweep(e.tree, []uint32{e.vout}, w.alice.party, storerec.EntryDerivation, w.chain)
	w.r.submitTx(sw, "")
	// The replay: the index drops it while the engine admits it; the drop
	// waits for the engine's record, then deletes and evicts.
	st := w.r.storage
	beforeApplied := func() {
		// Told before the applied record exists, as the engine tells it.
		w.r.ls.Flush(context.Background())
		if w.r.ls.Counts().PendingDrops != 1 {
			t.Fatal("the drop did not wait for the engine's record")
		}
	}
	got, err := w.r.admitWith(atomicOf(t, e.tx), topic, beforeApplied)
	if err != nil || len(got) != 1 {
		t.Fatal(got, err)
	}
	w.r.ls.Flush(context.Background())
	if w.r.ls.Counts().PendingDrops != 0 || st.isApplied(e.txid, topic) {
		t.Fatal("not dropped once recorded")
	}
	// A stranger's entry dropped by retention, offered again before the drop
	// runs: it is held, and stays in storage.
	s := w.bob.put(w.owner.id, "x", putOpts{})
	w.r.submitTx(s.tx, "")
	w.clock.advance(25 * hourDur)
	w.r.ls.holdDrops = true
	w.r.ls.Prune(context.Background())
	w.r.ls.holdDrops = false
	if w.r.ls.Counts().PendingDrops != 1 {
		t.Fatal(w.r.ls.Counts().PendingDrops)
	}
	op := outpointOf(s.txid, 0)
	_ = st.DeleteAppliedTransaction(context.Background(), &op.Txid, topic)
	_ = st.DeleteOutput(context.Background(), &op, topic)
	if _, err := w.r.admit(atomicOf(t, s.tx), topic); err != nil {
		t.Fatal(err)
	}
	w.r.ls.Flush(context.Background())
	if !st.held(s.txid, 0, topic) {
		t.Fatal("deleted what was offered again")
	}
	equalStrings(t, w.r.answer(map[string]string{"bucket": bucket, "entry": s.txid}), txids(s), "held again")
}

func TestUnmatchedSweepDroppedByRestore(t *testing.T) {
	w := newWorld(t, worldOpts{})
	e := w.alice.put(w.owner.id, "m", putOpts{})
	w.r.submitTx(e.tx, "")
	matched := mintSweep(e.tree, []uint32{e.vout}, w.alice.party, storerec.EntryDerivation, w.chain)
	w.r.submitTx(matched, "")
	lone := mintSweep(e.tree, []uint32{100}, w.alice.party, storerec.EntryDerivation, w.chain)
	// Admitted, and the host stops before the drop runs.
	if _, err := w.r.admit(atomicOf(t, lone), topic); err != nil {
		t.Fatal(err)
	}
	if !w.r.storage.held(lone.TxID().String(), 0, topic) {
		t.Fatal("not stored")
	}
	again := w.r.restart()
	if again.storage.held(lone.TxID().String(), 0, topic) {
		t.Fatal("the restore did not drop it")
	}
	lonely := e.tree.TxID().String() + ".100"
	equalStrings(t, again.answer(map[string]string{"bucket": bucket, "spent": lonely}), nil, "lone")
	equalStrings(t, again.answer(map[string]string{"bucket": bucket, "spent": e.op}), []string{matched.TxID().String()}, "matched")
	if !again.ls.Swept(lonely) {
		t.Fatal("its outpoints are not kept")
	}
}

func TestRowsAndMarksAcrossCompaction(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	w.r.submit(w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}}).beef, "")
	first := w.alice.put(w.owner.id, "k", putOpts{})
	second := w.alice.put(w.owner.id, "k", putOpts{vout: ptr(first.vout), created: T0 + 1})
	swept := w.alice.put(w.owner.id, "s", putOpts{})
	for _, e := range []*posted{first, swept} {
		w.r.submitTx(e.tx, "")
	}
	w.r.submitTx(mintSweep(swept.tree, []uint32{swept.vout}, w.alice.party, storerec.EntryDerivation, w.chain), "")
	w.r.ls.Prune(context.Background())
	// The compacted journal is the state, in the TypeScript module's order:
	// rows, marks, outpoints, first sights, tokens, mined outputs.
	order := map[string]int{"c": 0, "x": 1, "p": 2, "a": 3, "t": 4, "o": 5, "w": 5}
	last := 0
	for _, l := range w.r.journal.Lines {
		k, ok := order[l.K]
		if !ok || k < last {
			t.Fatalf("compacted journal out of order at %s", l.K)
		}
		last = k
	}
	again := w.r.restart()
	one(t, again.submitTx(second.tx, ""), "second")
	equalStrings(t, again.answer(map[string]string{"bucket": bucket, "entry": second.txid}), nil, "second dropped")
	one(t, again.submitTx(swept.tx, ""), "swept")
	equalStrings(t, again.answer(map[string]string{"bucket": bucket, "entry": swept.txid}), nil, "swept retracted")
	equalStrings(t, again.answer(map[string]string{"bucket": bucket, "entry": first.txid}), txids(first), "first kept")
}

func TestReorganisation(t *testing.T) {
	w := newWorld(t, worldOpts{})
	a := w.alice.put(w.owner.id, "a", putOpts{})
	b := w.alice.put(w.owner.id, "b", putOpts{})
	for _, e := range []*posted{a, b} {
		w.r.submitTx(e.tx, "")
	}
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	other := w.owner.fork()
	x := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{a}})
	y := other.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{b}})
	for _, m := range []*minted{x, y} {
		one(t, w.r.submit(m.beef, ""), "fork")
	}
	equalStrings(t, w.r.answer(w.chainQ), sorted([]string{x.txid, y.txid}), "both latest")
	low, high := x, y
	if y.txid < x.txid {
		low, high = y, x
	}
	// Both verify: the lowest txid.
	if o, _ := w.object(t, "a"); o == nil || o.token != low.txid {
		t.Fatalf("%+v", o)
	}
	// The lower one's block leaves the best chain: the other, once the choice is made again.
	w.chain.orphan(low.tx.MerklePath.BlockHeight)
	again := w.r.restart()
	if got := again.outputs(with(w.chainQ, "key", "a"))[0].txid; got != high.txid {
		t.Fatalf("%s, want %s", got, high.txid)
	}
}

func TestPerSeqBound(t *testing.T) {
	w := newWorld(t, worldOpts{})
	w.r.submit(w.owner.token(tokenOpts{}).beef, "")
	var forks []string
	for i := range 9 {
		m := w.owner.fork().token(tokenOpts{created: T0 + 600_000 + uint64(i)})
		one(t, w.r.submit(m.beef, ""), "fork")
		forks = append(forks, m.txid)
	}
	equalStrings(t, w.r.answer(with(w.chainQ, "seq", "1")), sorted(forks)[:8], "the lowest eight")
	if !w.r.host.logged("ls_bstore kept no more tokens at this seq", nil) {
		t.Fatal("not logged")
	}
}

func outpointOf(txid string, vout uint32) transaction.Outpoint {
	h, _ := chainhash.NewHashFromHex(txid)
	return transaction.Outpoint{Txid: *h, Index: vout}
}
