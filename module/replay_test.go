package module

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chaintoken"
	bmodule "github.com/lightwebinc/bcommon/module"
	"github.com/lightwebinc/bcommon/replay"

	"github.com/lightwebinc/bstore/storerec"
)

// A replay world: one bucket with a bit of everything, published in full to
// host A and in part to host B, over one chain and one clock.
type replayWorld struct {
	w        *world
	a        *rig
	objects  []replayed
	tokens   []*minted
	snapshot func(r *rig) string
}

// replayed is one published object: its submission, and whether host B
// received it.
type replayed struct {
	label string
	beef  []byte
	txid  string
	toB   bool
}

func newReplayWorld(t *testing.T, missing ...string) *replayWorld {
	t.Helper()
	w := newWorld(t, worldOpts{})
	now := uint64(w.clock.ms())
	h := w.alice.hold(5000)
	e1 := w.alice.put(w.owner.id, "a", putOpts{hold: h.terms})
	e2 := w.alice.put(w.owner.id, "b", putOpts{})
	e3 := w.alice.put(w.owner.id, "a", putOpts{label: "again", created: T0 + 1})
	p := w.owned.put(w.owner.id, "p", putOpts{})
	pend := w.alice.put(w.owner.id, "z", putOpts{created: now - 3600_000})
	doomed := w.bob.put(w.owner.id, "doomed", putOpts{})
	secs := now / 1000
	ad := w.prov.advertise([][32]byte{e1.e.Root}, secs+30*86400)
	renewed := w.prov.renew(ad, 0, e1.e.Root, secs+60*86400)
	ad2 := w.prov2.advertise([][32]byte{e2.e.Root}, secs+30*86400)
	f := w.front.statement(w.prov.party.id, now-60_000)
	o := w.buyer.outcome(outcomeOpts{root: e1.e.Root, provider: w.prov.party, created: now - 60_000})
	t0 := w.owner.token(tokenOpts{})
	t1 := w.owner.token(tokenOpts{writers: [][]byte{w.alice.id()}, anchor: []*posted{e1, e2}})
	t2 := w.owner.token(tokenOpts{anchor: []*posted{e3}})
	t3 := w.owner.token(tokenOpts{anchor: []*posted{p}})
	sw := mintSweep(doomed.tree, []uint32{doomed.vout}, w.bob.party, storerec.EntryDerivation, w.chain)
	rw := &replayWorld{w: w, a: w.r, tokens: []*minted{t0, t1, t2, t3}}
	miss := map[string]bool{}
	for _, m := range missing {
		miss[m] = true
	}
	add := func(label string, beef []byte, txid string) {
		rw.objects = append(rw.objects, replayed{label: label, beef: beef, txid: txid, toB: !miss[label]})
	}
	for _, x := range []struct {
		label string
		p     *posted
	}{{"e1", e1}, {"e2", e2}, {"e3", e3}, {"p", p}, {"pend", pend}, {"doomed", doomed}, {"front", f}, {"outcome", o}} {
		add(x.label, atomicOf(t, x.p.tx), x.p.txid)
	}
	for _, x := range []struct {
		label string
		tx    *transaction.Transaction
	}{{"hold", h.tx}, {"ad", ad}, {"renewed", renewed}, {"ad2", ad2}} {
		add(x.label, atomicOf(t, x.tx), x.tx.TxID().String())
	}
	for i, x := range rw.tokens {
		add(fmt.Sprintf("t%d", i), x.beef, x.txid)
	}
	add("sweep", atomicOf(t, sw), sw.TxID().String())
	for _, x := range rw.objects {
		if got := rw.a.submit(x.beef, ""); got == nil {
			t.Fatalf("host A refused %s", x.label)
		}
	}
	q := w.chainQ
	root1, root2 := hex.EncodeToString(e1.e.Root[:]), hex.EncodeToString(e2.e.Root[:])
	rw.snapshot = func(r *rig) string {
		ctxOf := func(qq map[string]string) []string {
			var out []string
			for _, x := range r.outputs(qq) {
				out = append(out, x.txid+" "+hex.EncodeToString(x.context))
			}
			return out
		}
		var st []string
		for _, x := range rw.tokens {
			st = append(st, r.status(x.txid))
		}
		b := func(kv ...string) map[string]string { return with(map[string]string{"bucket": bucket}, kv...) }
		parts := [][]string{
			ctxOf(q), ctxOf(with(q, "seq", "0")), ctxOf(with(q, "seq", "1")), ctxOf(with(q, "seq", "2")), ctxOf(with(q, "seq", "3")),
			ctxOf(with(q, "key", "a")), ctxOf(with(q, "key", "b")), ctxOf(with(q, "key", "p")), ctxOf(with(q, "key", "z")),
			ctxOf(with(q, "prefix", "", "after", "")), ctxOf(map[string]string{"bucket": bucket, "owner": w.owner.hex(), "export": w.owner.idHex(), "after": ""}),
			r.answer(with(q, "pending", "z")), r.answer(with(q, "pending", "a")), r.answer(with(q, "pending", "doomed")),
			r.answer(with(q, "writer", w.alice.hex(), "after", "")), r.answer(with(q, "writer", w.owner.hex(), "after", "")),
			r.answer(with(q, "writer", w.bob.hex(), "after", "")),
			r.answer(b("entry", e1.txid)), r.answer(b("entry", e2.txid)), r.answer(b("entry", e3.txid)), r.answer(b("entry", p.txid)),
			r.answer(b("entry", pend.txid)), r.answer(b("entry", doomed.txid)),
			r.answer(b("root", root1, "after", "")), r.answer(b("root", root2, "after", "")),
			r.answer(b("root", root1, "advertiser", w.prov.hex())), r.answer(b("root", root2, "advertiser", w.prov2.hex())),
			r.answer(b("advertiser", w.prov.hex(), "after", "")), r.answer(b("advertiser", w.prov.hex(), "front", w.front.hex())),
			r.answer(b("hold", h.op)),
			r.answer(b("answerer", w.prov.hex())), r.answer(b("provider", w.prov.hex())),
			r.answer(b("answerer", w.prov.hex(), "buyer", w.buyer.hex())), r.answer(b("provider", w.prov.hex(), "buyer", w.buyer.hex())),
			r.answer(b("outcome-history", w.prov.hex(), "before", fmt.Sprintf("%d:%s", now+1, strings.Repeat("f", 64)))),
			r.answer(b("spent", doomed.op)), r.answer(b("spent", e1.op)),
			r.answer(with(q, "history", "a", "before", "9")),
			r.answer(map[string]string{"bucket": bucket, "owner": w.owner.hex(), "chain": w.owner.idHex(), "after": "0"}),
			st,
		}
		return fmt.Sprint(parts)
	}
	return rw
}

// hostB is a second host over the world's chain and clock, serving the same
// chain, given what the world published to it.
func (rw *replayWorld) hostB(t *testing.T) *rig {
	t.Helper()
	b := setup(t, setupOpts{chain: rw.w.chain, clock: rw.w.clock, config: func(c *Config) { c.Served = []string{rw.w.owner.served()} }})
	for _, x := range rw.objects {
		if x.toB {
			b.submit(x.beef, "")
		}
	}
	return b
}

// peerOf asks a rig's lookup service as a peer's route answers: an error
// is the peer's refusal.
func peerOf(r *rig) bmodule.Ask {
	return func(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
		a, err := r.ls.Lookup(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", bmodule.ErrRefused, err)
		}
		return a, nil
	}
}

type walkResult struct {
	calls, questions, gaps int
	kinds                  map[string]int
	notes                  []string
}

// replayFrom runs whole walks of host into from until one is Done,
// submitting each answer's objects in order as the engine does and holding
// every token to its BEEF and its parent: the token and exactly its
// parent, the parent held or admitted earlier in the answer.
func replayFrom(t *testing.T, into, from *rig, maxQ, maxO int) walkResult {
	t.Helper()
	res := walkResult{kinds: map[string]int{}}
	var cursor any
	for {
		res.calls++
		if res.calls > 2000 {
			t.Fatal("the walk does not end")
		}
		a, err := into.ls.Replay(context.Background(), &bmodule.ReplayRequest{Topic: topic, Peer: "a", Ask: peerOf(from), Cursor: cursor,
			MaxQuestions: maxQ, MaxObjects: maxO,
			Applied: func(_ context.Context, txid chainhash.Hash) (bool, error) {
				return into.storage.isApplied(txid.String(), topic), nil
			}})
		if err != nil {
			t.Fatal(err)
		}
		if maxQ > 0 && a.Questions > maxQ || maxO > 0 && len(a.Objects) > maxO {
			t.Fatalf("a call asked %d and answered %d, bounds %d and %d", a.Questions, len(a.Objects), maxQ, maxO)
		}
		res.questions += a.Questions
		res.gaps += a.Gaps
		res.notes = append(res.notes, a.Notes...)
		admitted := map[chainhash.Hash]bool{}
		for _, o := range a.Objects {
			for _, dep := range o.After {
				if !admitted[dep] {
					t.Fatalf("%s %s submitted before %s, its After", o.Kind, o.Txid, dep)
				}
			}
			if o.Kind == replay.KindToken {
				wire, err := chaintoken.ReadWire(o.BEEF, 0)
				if err != nil {
					t.Fatal(err)
				}
				tx := wire.SubjectTx()
				parent, err := wire.Token(tx)
				if err != nil {
					t.Fatalf("token %s: %v", o.Txid, err)
				}
				tok, _ := storerec.ReadToken(tx)
				if tok.Bucket.Seq > 0 && !into.storage.isApplied(parent.Txid.String(), topic) && !admitted[parent.Txid] {
					t.Fatalf("token %s (seq %d) submitted before its parent is held", o.Txid, tok.Bucket.Seq)
				}
			}
			if into.storage.isApplied(o.Txid.String(), topic) {
				t.Fatalf("%s %s answered though applied", o.Kind, o.Txid)
			}
			if got := into.submit(o.BEEF, ""); got != nil {
				admitted[o.Txid] = true
			}
			res.kinds[o.Kind]++
		}
		if a.Done {
			if a.Cursor != nil {
				t.Fatal("a Done answer carries a cursor")
			}
			return res
		}
		cursor = a.Cursor
	}
}

func utxoSet(r *rig) []string {
	var out []string
	for _, o := range r.storage.utxos(topic) {
		out = append(out, o.Outpoint.String())
	}
	sort.Strings(out)
	return out
}

// TestReplayLevelsAHost: host B missed two tokens, entries anchored,
// replaced and pending, two advertisements and a hold output, a front
// statement, an outcome and the sweep that retracts an entry it holds. One
// walk from host A, asking only the free classes, leaves both with the same
// unspent outputs and every class answered alike, tokens in order on their
// parents; a second walk answers nothing.
func TestReplayLevelsAHost(t *testing.T) {
	rw := newReplayWorld(t, "t2", "t3", "e2", "e3", "p", "pend", "hold", "ad", "renewed", "ad2", "front", "outcome", "sweep")
	b := rw.hostB(t)
	want := rw.snapshot(rw.a)
	if got := rw.snapshot(b); got == want {
		t.Fatal("host B is level before any replay")
	}
	res := replayFrom(t, b, rw.a, 0, 0)
	if got := rw.snapshot(b); got != want {
		t.Fatalf("after the walk:\n%s\nwant\n%s\nnotes %v", got, want, res.notes)
	}
	if fmt.Sprint(utxoSet(b)) != fmt.Sprint(utxoSet(rw.a)) {
		t.Fatalf("unspent outputs:\n%v\nwant\n%v", utxoSet(b), utxoSet(rw.a))
	}
	if res.kinds["token"] != 2 || res.kinds["entry"] != 4 || res.kinds["ad"] != 2 || res.kinds["hold"] != 1 || res.kinds["front"] != 1 ||
		res.kinds["outcome"] != 1 || res.kinds["sweep"] != 1 || res.gaps != 0 {
		t.Fatalf("replayed %v, gaps %d, notes %v", res.kinds, res.gaps, res.notes)
	}
	// The walk is done: the next one answers nothing.
	again := replayFrom(t, b, rw.a, 0, 0)
	if len(again.kinds) != 0 {
		t.Fatalf("a second walk replayed %v", again.kinds)
	}
	// And A replaying from B finds nothing either.
	if back := replayFrom(t, rw.a, b, 0, 0); len(back.kinds) != 0 {
		t.Fatalf("A replayed %v from B", back.kinds)
	}
}

// TestReplayBounded: a call asks at most two questions and answers at
// most one object, and the walks together level the host all the same.
func TestReplayBounded(t *testing.T) {
	rw := newReplayWorld(t, "t2", "t3", "e2", "e3", "p", "pend", "hold", "ad", "renewed", "ad2", "front", "outcome", "sweep")
	b := rw.hostB(t)
	res := replayFrom(t, b, rw.a, 2, 1)
	if got, want := rw.snapshot(b), rw.snapshot(rw.a); got != want {
		t.Fatalf("after the bounded walk (%d calls):\n%s\nwant\n%s", res.calls, got, want)
	}
	if res.calls < 13 {
		t.Fatalf("%d calls for 13 objects at one a call", res.calls)
	}
}

// TestReplayFirstToken: a host that holds no token of the chain needs the
// first token's parent, the chain tree, which no host keeps: without a
// chain view the chain is a gap and no token of it is answered; with one,
// the whole bucket is levelled. The host holds one carrier, the one a
// sweep retracted: a sweep is found through the carriers the host holds,
// and one of a carrier it never held is not replayed (no class lists
// sweeps; it would retract nothing here).
func TestReplayFirstToken(t *testing.T) {
	rw := newReplayWorld(t)
	b := setup(t, setupOpts{chain: rw.w.chain, clock: rw.w.clock, config: func(c *Config) { c.Served = []string{rw.w.owner.served()} }})
	for _, x := range rw.objects {
		if x.label == "doomed" {
			b.submit(x.beef, "")
		}
	}
	res := replayFrom(t, b, rw.a, 0, 0)
	if res.kinds["token"] != 0 || res.gaps == 0 {
		t.Fatalf("with no chain view: %v, gaps %d", res.kinds, res.gaps)
	}
	b.ls.trees = treesOf{rw.w.owner.chainTree}
	replayFrom(t, b, rw.a, 0, 0)
	if got, want := rw.snapshot(b), rw.snapshot(rw.a); got != want {
		t.Fatalf("with a chain view:\n%s\nwant\n%s", got, want)
	}
}

// TestReplayKeepsRetention: an object this host dropped by its floor is
// not handed back by a peer that still holds it; nor is a carrier on an
// outpoint this host retracted.
func TestReplayKeepsRetention(t *testing.T) {
	rw := newReplayWorld(t, "pend")
	b := rw.hostB(t)
	rw.w.clock.advance(7 * dayDur)
	// B dropped nothing it never had; A still holds pend (its floor is
	// counted from A's own sight, and A has not pruned). pend is dated six
	// days and a few hours ago: past the replay window.
	res := replayFrom(t, b, rw.a, 0, 0)
	if res.kinds["entry"] != 0 {
		t.Fatalf("replayed %v", res.kinds)
	}
}

// TestReplayRetraction: host B swept an entry host A still answers. B's
// walk does not offer the entry back (the outpoint carries B's retraction
// mark); A's walk finds the sweep through the entry it holds and retracts
// it, after which the two answer alike.
func TestReplayRetraction(t *testing.T) {
	rw := newReplayWorld(t)
	w := rw.w
	x := w.alice.put(w.owner.id, "x", putOpts{created: uint64(w.clock.ms()) - 60_000})
	sw := mintSweep(x.tree, []uint32{x.vout}, w.alice.party, storerec.EntryDerivation, w.chain)
	b := rw.hostB(t)
	for _, r := range []*rig{rw.a, b} {
		if r.submitTx(x.tx, "") == nil {
			t.Fatal("the entry")
		}
	}
	if b.submitTx(sw, "") == nil {
		t.Fatal("the sweep")
	}
	unanchored := with(w.chainQ, "writer", w.alice.hex(), "after", "")
	if strings.Contains(fmt.Sprint(b.answer(unanchored)), x.txid) || !strings.Contains(fmt.Sprint(rw.a.answer(unanchored)), x.txid) {
		t.Fatal("set-up: B retracted the entry, A answers it")
	}
	if res := replayFrom(t, b, rw.a, 0, 0); res.kinds["entry"] != 0 {
		t.Fatalf("B was offered %v", res.kinds)
	}
	res := replayFrom(t, rw.a, b, 0, 0)
	if res.kinds["sweep"] != 1 {
		t.Fatalf("A replayed %v", res.kinds)
	}
	if got, want := rw.snapshot(rw.a), rw.snapshot(b); got != want || strings.Contains(fmt.Sprint(rw.a.answer(unanchored)), x.txid) {
		t.Fatalf("after the sweep:\n%s\nwant\n%s", got, want)
	}
}

// TestReplayAsksNoDroppedHistory: an entry replaced longer ago than the
// keep floor has left every host, so a walk does not ask for it, though a
// token still names it; while it is inside the floor, a host that lacks it
// is given it.
func TestReplayAsksNoDroppedHistory(t *testing.T) {
	rw := newReplayWorld(t, "e1")
	b := rw.hostB(t)
	asked := func(r *rig) (entries int) {
		ask := peerOf(rw.a)
		var cursor any
		for {
			a, err := r.ls.Replay(context.Background(), &bmodule.ReplayRequest{Topic: topic, Peer: "a", Cursor: cursor,
				Ask: func(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
					if strings.Contains(string(q.Query), `"entry"`) {
						entries++
					}
					return ask(ctx, q)
				},
				Applied: func(_ context.Context, txid chainhash.Hash) (bool, error) {
					return r.storage.isApplied(txid.String(), topic), nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range a.Objects {
				r.submit(o.BEEF, "")
			}
			if a.Done {
				return entries
			}
			cursor = a.Cursor
		}
	}
	// e1 (key a, replaced by e3) is missing on B and inside the floor: asked
	// for, and given.
	if n := asked(b); n != 1 || !b.storage.isApplied(rw.objects[0].txid, topic) {
		t.Fatalf("inside the floor: %d entry questions, e1 applied %v", n, b.storage.isApplied(rw.objects[0].txid, topic))
	}
	// Past the floor both hosts drop it, and no walk asks for it again.
	rw.w.clock.advance(40 * dayDur)
	rw.a.ls.Prune(context.Background())
	b.ls.Prune(context.Background())
	if b.storage.held(rw.objects[0].txid, 0, topic) {
		t.Fatal("e1 is still held after its floor")
	}
	if n := asked(b); n != 0 {
		t.Fatalf("past the floor: %d entry questions", n)
	}
}

// treesOf is a chain view holding the given transactions.
type treesOf []*transaction.Transaction

func (ts treesOf) TxRaw(_ context.Context, txid string) ([]byte, error) {
	for _, tx := range ts {
		if tx.TxID().String() == txid {
			return tx.Bytes(), nil
		}
	}
	return nil, errors.New("not known")
}

func (ts treesOf) Proof(_ context.Context, txid string) (*transaction.MerklePath, uint32, error) {
	for _, tx := range ts {
		if tx.TxID().String() == txid {
			return tx.MerklePath, tx.MerklePath.BlockHeight, nil
		}
	}
	return nil, 0, errors.New("not mined")
}
