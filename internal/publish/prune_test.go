package publish

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/funding"
)

// Prune drops the hold records whose story is over (reclaimed and mined,
// spent elsewhere, or named by an entry whose race ended) and the trees
// spent out, and keeps the rest: holds in flight, ready, reclaiming or
// racing, the current tree and trees with outputs left.
func TestPruneDropsFinishedHoldsAndSpentTrees(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	p := &Publisher{cfg: Config{Dir: dir, Logf: t.Logf}, st: &state{}}
	settled, open := strings.Repeat("aa", 32), strings.Repeat("bb", 32)
	for _, r := range []*Race{{Entry: settled, State: RaceSettled}, {Entry: open, State: RaceOpen}} {
		if err := p.saveRace(r); err != nil {
			t.Fatal(err)
		}
	}
	p.st.Holds = []HoldRecord{
		{Txid: "settled", Used: true, Entry: settled},
		{Txid: "racing", Used: true, Entry: open},
		{Txid: "reclaimed", Used: true, Entry: "reclaimed by x", Reclaim: &ReclaimRecord{Height: 5}},
		{Txid: "reclaiming", Used: true, Entry: "reclaiming", Reclaim: &ReclaimRecord{}},
		{Txid: "spent", Used: true, Entry: "spent elsewhere"},
		{Txid: "ready", BumpHex: "00"},
		{Txid: "taken", Used: true},
	}
	n, err := p.PruneHolds()
	if err != nil || n != 3 {
		t.Fatalf("pruned %d holds, %v; want 3", n, err)
	}
	var left []string
	for _, h := range p.st.Holds {
		left = append(left, h.Txid)
	}
	if got := strings.Join(left, " "); got != "racing reclaiming ready taken" {
		t.Fatalf("holds left: %s", got)
	}
	p.st.Entry = treeSet{Tree: &funding.Tree{Txid: "cur", Count: 4, Next: 4},
		Trees: []funding.Tree{{Txid: "old", Count: 4, Next: 4}, {Txid: "cur", Count: 4, Next: 4}, {Txid: "half", Count: 4, Next: 2}}}
	p.st.Piece = treeSet{Trees: []funding.Tree{{Txid: "gone", Count: 8, Next: 8}}}
	n, err = p.pruneTrees()
	if err != nil || n != 2 {
		t.Fatalf("pruned %d trees, %v; want 2", n, err)
	}
	if len(p.st.Entry.Trees) != 2 || p.st.Entry.Trees[0].Txid != "cur" || p.st.Entry.Trees[1].Txid != "half" || len(p.st.Piece.Trees) != 0 {
		t.Fatalf("trees left: entry %+v, piece %+v", p.st.Entry.Trees, p.st.Piece.Trees)
	}
	st, err := loadState(p.cfg.Dir)
	if err != nil || len(st.Holds) != 4 || len(st.Entry.Trees) != 2 {
		t.Fatalf("saved state: %v, %d holds, %d entry trees", err, len(st.Holds), len(st.Entry.Trees))
	}
}

// A tree whose last outputs a put has reserved is spent out and not yet
// named by the outbox (the put writes its pieces there afterwards): prune
// keeps it while any put pins it, and for the grace after the last
// released it, or the pieces on those outputs could never be minted. Past
// the grace the outbox is the record, and an unnamed tree goes.
func TestPruneKeepsATreeJustSpentOut(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	p := &Publisher{cfg: Config{Dir: dir, Logf: t.Logf, Now: func() time.Time { return now }}, st: &state{}}
	p.st.Piece = treeSet{Tree: &funding.Tree{Txid: "next", Count: 8},
		Trees: []funding.Tree{{Txid: "last", Count: 8, Next: 8}, {Txid: "next", Count: 8}}}
	last := transaction.NewTransaction()
	id := last.TxID().String()
	p.st.Piece.Trees[0].Txid = id
	p.mu.Lock()
	p.pinTree(id)
	p.pinTree(id)
	p.mu.Unlock()
	// Pinned by two puts, however long they take.
	now = now.Add(3 * treeGrace)
	if n, err := p.pruneTrees(); err != nil || n != 0 || len(p.st.Piece.Trees) != 2 {
		t.Fatalf("a pinned tree: pruned %d, %v", n, err)
	}
	p.releaseTree(last)
	if n, _ := p.pruneTrees(); n != 0 {
		t.Fatalf("one of two puts released: pruned %d", n)
	}
	p.releaseTree(last)
	if n, err := p.pruneTrees(); err != nil || n != 0 || len(p.pinned) != 0 {
		t.Fatalf("just released: pruned %d, %v, %d pins left", n, err, len(p.pinned))
	}
	now = now.Add(treeGrace - time.Second)
	if n, _ := p.pruneTrees(); n != 0 {
		t.Fatalf("inside the grace: pruned %d", n)
	}
	now = now.Add(2 * time.Second)
	if n, err := p.pruneTrees(); err != nil || n != 1 || len(p.st.Piece.Trees) != 1 || len(p.spentAt) != 0 {
		t.Fatalf("past the grace: pruned %d, %v, %d notes left", n, err, len(p.spentAt))
	}
}
