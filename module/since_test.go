package module

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

func sinceQ(after string) map[string]string {
	return map[string]string{"bucket": bucket, "since": after}
}

// sinceAll pages since from the start and answers every txid with its
// position, holding each page to the class's order and bound.
func sinceAll(t *testing.T, r *rig) (txids, positions []string) {
	t.Helper()
	after := ""
	for {
		outs := r.outputs(sinceQ(after))
		if len(outs) > PageLimit {
			t.Fatalf("a page of %d", len(outs))
		}
		for _, o := range outs {
			pos := string(o.context)
			if !storerec.SincePosition(pos) || pos <= after {
				t.Fatalf("position %q after %q", pos, after)
			}
			if pos[17:81] != o.txid {
				t.Fatalf("position %q on output %s", pos, o.txid)
			}
			after = pos
			txids = append(txids, o.txid)
			positions = append(positions, pos)
		}
		if len(outs) == 0 {
			return txids, positions
		}
	}
}

// TestSinceListsWhatTheFreeClassesAnswer: since answers the entries no
// token names, the live advertisements, the hold output, the front
// statement, the outcome and the sweep, each once, in the order the host
// took them, and nothing else: no token, no entry a token names (the
// priced history sells those), no advertisement a renewal spent.
func TestSinceListsWhatTheFreeClassesAnswer(t *testing.T) {
	rw := newReplayWorld(t)
	r := rw.a
	if got, _ := sinceAll(t, r); len(got) != 0 {
		t.Fatalf("the present millisecond was answered: %v", got)
	}
	r.clock.advance(time.Millisecond)
	got, _ := sinceAll(t, r)
	by := map[string]string{}
	for _, o := range rw.objects {
		by[o.txid] = o.label
	}
	var labels []string
	for _, txid := range got {
		labels = append(labels, by[txid])
	}
	sort.Strings(labels)
	// e1, e2, e3 and p are named by tokens; ad was spent by its renewal;
	// doomed was retracted by the sweep.
	if want := "ad2 front hold outcome pend renewed sweep"; strings.Join(labels, " ") != want {
		t.Fatalf("since answered %v, want %s", labels, want)
	}
	// What a class stops answering leaves the list: an entry a token names.
	w := rw.w
	x := w.alice.put(w.owner.id, "x", putOpts{created: uint64(w.clock.ms())})
	if r.submitTx(x.tx, "") == nil {
		t.Fatal("the entry")
	}
	r.clock.advance(time.Millisecond)
	if got, _ := sinceAll(t, r); !strings.Contains(fmt.Sprint(got), x.txid) {
		t.Fatal("a new unanchored entry is not listed")
	}
	tok := w.owner.token(tokenOpts{anchor: []*posted{x}})
	if r.submit(tok.beef, "") == nil {
		t.Fatal("the token")
	}
	r.clock.advance(time.Millisecond)
	if got, _ := sinceAll(t, r); strings.Contains(fmt.Sprint(got), x.txid) {
		t.Fatal("an entry a token names is still listed")
	}
	// The window of unanchored is since's window too.
	r.clock.advance(UnanchoredDays * dayDur)
	if got, _ := sinceAll(t, r); strings.Contains(fmt.Sprint(got), rw.label("pend")) {
		t.Fatal("an entry past the unanchored window is still listed")
	}
	// A question that is no position is refused, never answered empty.
	for _, bad := range []string{"0", strings.Repeat("0", 92), "0000000000000001:" + strings.Repeat("g", 64) + ":0000000000"} {
		if r.refused(sinceQ(bad)) == "" {
			t.Fatalf("since %q was answered", bad)
		}
	}
}

// label is the txid of a published object.
func (rw *replayWorld) label(l string) string {
	for _, o := range rw.objects {
		if o.label == l {
			return o.txid
		}
	}
	return ""
}

// TestSincePages: more outputs than a page are answered page by page from
// the position of the last, each once.
func TestSincePages(t *testing.T) {
	w := newWorld(t, worldOpts{})
	want := map[string]bool{}
	for i := 0; i < PageLimit*2+5; i++ {
		e := w.alice.put(w.owner.id, fmt.Sprintf("k%03d", i), putOpts{created: uint64(w.clock.ms())})
		if w.r.submitTx(e.tx, "") == nil {
			t.Fatal("the entry")
		}
		want[e.txid] = true
		if i%7 == 0 {
			w.clock.advance(time.Millisecond)
		}
	}
	w.clock.advance(time.Millisecond)
	got, _ := sinceAll(t, w.r)
	if len(got) != len(want) {
		t.Fatalf("%d outputs over the pages, want %d", len(got), len(want))
	}
	for _, txid := range got {
		if !want[txid] {
			t.Fatalf("%s answered twice or never published", txid)
		}
		delete(want, txid)
	}
}

// TestSincePositionsHold: a restart lists every output at the position it
// had, and a clock that steps back puts nothing behind a position given.
func TestSincePositionsHold(t *testing.T) {
	rw := newReplayWorld(t)
	rw.a.clock.advance(time.Millisecond)
	_, before := sinceAll(t, rw.a)
	again := rw.a.restart()
	if _, after := sinceAll(t, again); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("after a restart:\n%v\nwant\n%v", after, before)
	}
	w := rw.w
	last := before[len(before)-1]
	w.clock.advance(-time.Hour)
	x := w.alice.put(w.owner.id, "x", putOpts{created: uint64(w.clock.ms())})
	if again.submitTx(x.tx, "") == nil {
		t.Fatal("the entry")
	}
	// Not answered while the host's clock is behind the position it gave.
	if got := again.answer(sinceQ(last)); len(got) != 0 {
		t.Fatalf("answered ahead of the clock: %v", got)
	}
	w.clock.advance(2 * time.Hour)
	outs := again.outputs(sinceQ(last))
	if len(outs) != 1 || outs[0].txid != x.txid || string(outs[0].context) <= last {
		t.Fatalf("after the clock stepped back: %d outputs after %s", len(outs), last)
	}
}

// counted counts the questions a walk asks, by class member.
func counted(ask bmodule.Ask, n map[string]int) bmodule.Ask {
	return func(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
		n["all"]++
		for _, m := range []string{"since", "spent", "root", "writer"} {
			if strings.Contains(string(q.Query), `"`+m+`"`) {
				n[m]++
			}
		}
		return ask(ctx, q)
	}
}

// TestReplayCostsWhatIsMissing: two hosts that hold the same ask each
// other one since question a walk whatever they hold, where a peer that
// refuses since is asked a question for every root and carrier; one new
// object at the peer is answered by the next walk, and the walk after asks
// one question again.
func TestReplayCostsWhatIsMissing(t *testing.T) {
	rw := newReplayWorld(t)
	b := rw.hostB(t)
	n := map[string]int{}
	replayPeer = func(r *rig) bmodule.Ask { return counted(peerOf(r), n) }
	defer func() { replayPeer = peerOf }()
	// The first walk reads everything the peer lists, once.
	if res := replayFrom(t, b, rw.a, 0, 0); len(res.kinds) != 0 {
		t.Fatalf("level hosts replayed %v", res.kinds)
	}
	for _, level := range []int{1, 2} {
		clear(n)
		replayFrom(t, b, rw.a, 0, 0)
		if n["since"] != 1 || n["spent"]+n["root"]+n["writer"] != 0 {
			t.Fatalf("walk %d of level hosts asked %v", level, n)
		}
	}
	inStep := n["all"]
	w := rw.w
	x := w.alice.put(w.owner.id, "x", putOpts{created: uint64(w.clock.ms())})
	if rw.a.submitTx(x.tx, "") == nil {
		t.Fatal("the entry")
	}
	clear(n)
	if res := replayFrom(t, b, rw.a, 0, 0); res.kinds["entry"] != 1 || len(res.kinds) != 1 || !b.ls.HoldsCarrier(cOf(x.txid)) {
		t.Fatalf("one new entry: replayed %v", res.kinds)
	}
	clear(n)
	replayFrom(t, b, rw.a, 0, 0)
	if n["since"] != 1 || n["all"] != inStep {
		t.Fatalf("the walk after asked %v, in step %d", n, inStep)
	}
	// The same hosts, the peer refusing since: a question a carrier and a root.
	clear(n)
	replayPeer = func(r *rig) bmodule.Ask { return counted(withoutSince(peerOf(r)), n) }
	replayFrom(t, b, rw.a, 0, 0)
	if n["spent"] == 0 || n["root"] == 0 || n["all"] <= 2*inStep {
		t.Fatalf("a peer that refuses since was asked %v", n)
	}
}

// TestReplayPositionWaitsForAdmission: the position does not move past an
// object the walk answered until the host applied it, so an object the
// host could not admit at once is offered again; one it never admits is
// passed over after SinceTries walks.
func TestReplayPositionWaitsForAdmission(t *testing.T) {
	rw := newReplayWorld(t, "pend")
	b := rw.hostB(t)
	rw.a.clock.advance(time.Millisecond)
	walk := func(admit bool) (offered int) {
		var cursor any
		for {
			a, err := b.ls.Replay(context.Background(), &bmodule.ReplayRequest{Topic: topic, Peer: "a", Ask: peerOf(rw.a), Cursor: cursor,
				Applied: func(_ context.Context, txid chainhash.Hash) (bool, error) {
					return b.storage.isApplied(txid.String(), topic), nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range a.Objects {
				offered++
				if admit {
					b.submit(o.BEEF, "")
				}
			}
			if a.Done {
				return offered
			}
			cursor = a.Cursor
		}
	}
	for i := 0; i < SinceTries; i++ {
		if n := walk(false); n != 1 {
			t.Fatalf("walk %d offered %d objects", i, n)
		}
		if b.ls.reachedAt(bucket+"|a") >= sinceKeyOf(t, rw.a, rw.label("pend")) {
			t.Fatal("the position moved past an object the host did not apply")
		}
	}
	if n := walk(false); n != 0 {
		t.Fatalf("offered again after %d walks", SinceTries)
	}
	if b.ls.reachedAt(bucket+"|a") < sinceKeyOf(t, rw.a, rw.label("pend")) {
		t.Fatal("the position stayed behind an object the host never admits")
	}
	// A host that admits it: offered once, then the position passes it.
	c := rw.hostB(t)
	b = c
	if n := walk(true); n != 1 || !c.storage.isApplied(rw.label("pend"), topic) {
		t.Fatalf("offered %d", n)
	}
	if n := walk(true); n != 0 || c.ls.reachedAt(bucket+"|a") < sinceKeyOf(t, rw.a, rw.label("pend")) {
		t.Fatalf("the walk after offered %d, position %q", n, c.ls.reachedAt(bucket+"|a"))
	}
}

// sinceKeyOf is the position a rig lists txid at.
func sinceKeyOf(t *testing.T, r *rig, txid string) string {
	t.Helper()
	txids, positions := sinceAll(t, r)
	for i, x := range txids {
		if x == txid {
			return positions[i]
		}
	}
	t.Fatalf("%s is not listed", txid)
	return ""
}

// TestReplayPositionsOutOfOrder: a peer whose positions do not ascend, or
// are no positions, is not followed: the walk ends with a gap and the
// position reached stays where it was.
func TestReplayPositionsOutOfOrder(t *testing.T) {
	rw := newReplayWorld(t, "pend")
	b := rw.hostB(t)
	rw.a.clock.advance(time.Millisecond)
	ask := peerOf(rw.a)
	bad := func(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
		a, err := ask(ctx, q)
		if err == nil && strings.Contains(string(q.Query), `"since"`) {
			if outs, ok := a.Result.([]bmodule.HydratedOutput); ok {
				for i := range outs {
					outs[i].Context = []byte(sinceKey(1, strings.Repeat("0", 64), 0))
				}
			}
		}
		return a, err
	}
	replayPeer = func(*rig) bmodule.Ask { return bad }
	defer func() { replayPeer = peerOf }()
	res := replayFrom(t, b, rw.a, 0, 0)
	if res.gaps == 0 || res.kinds["entry"] != 0 || b.ls.reachedAt(bucket+"|a") != "" {
		t.Fatalf("replayed %v, gaps %d, position %q", res.kinds, res.gaps, b.ls.reachedAt(bucket+"|a"))
	}
}

// TestFilePositions: the positions reached are read back after a restart,
// and a file cut short reads as none.
func TestFilePositions(t *testing.T) {
	f := &FilePositions{Path: filepath.Join(t.TempDir(), "state", PositionsFile)}
	if m, err := f.Load(); err != nil || len(m) != 0 {
		t.Fatalf("no file: %v, %v", m, err)
	}
	pos := sinceKey(7, strings.Repeat("a", 64), 3)
	if err := f.Save(map[string]string{"b|peer": pos}); err != nil {
		t.Fatal(err)
	}
	if m, err := f.Load(); err != nil || m["b|peer"] != pos {
		t.Fatalf("read back %v, %v", m, err)
	}
	s, err := NewService(Options{Buckets: []string{bucket}, Host: newFakeHost(), Positions: f})
	if err != nil || s.reachedAt("b|peer") != pos {
		t.Fatalf("the service read %q, %v", s.reachedAt("b|peer"), err)
	}
	s.reach("b|peer", sinceKey(8, strings.Repeat("a", 64), 3))
	s.savePositions()
	if m, _ := f.Load(); m["b|peer"] != sinceKey(8, strings.Repeat("a", 64), 3) {
		t.Fatalf("saved %v", m)
	}
}
