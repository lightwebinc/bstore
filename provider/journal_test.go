package provider

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lightwebinc/bcommon/funding"
)

func TestAdJournalBringsBackWhatTheStateLacks(t *testing.T) {
	dir := t.TempDir()
	row := adRow{
		Ad:      PendingAd{Root: "aa", Txid: "t1", Expiry: 500, Tree: "tree", Vout: 7},
		Holding: Holding{Root: "aa", Verified: true},
		Entries: []EntryRec{{C: "c1", Root: "aa"}},
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "t1.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	// A write a crash cut off before its rename is passed over.
	if err := os.WriteFile(filepath.Join(dir, "t2.json.tmp"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	st := &State{Holdings: map[string]*Holding{}, Entries: map[string]*EntryRec{}, Tree: &funding.Tree{Txid: "tree", Next: 3, Count: 16}}
	files, err := st.replayAds(dir, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("%d rows read", len(files))
	}
	if len(st.PendingAds) != 1 || st.PendingAds[0].Txid != "t1" {
		t.Fatal("the pending advertisement did not come back")
	}
	if h := st.Holdings["aa"]; h == nil || h.Lease != 500 {
		t.Fatalf("the holding came back as %+v", h)
	}
	if st.Entries["c1"] == nil {
		t.Fatal("the entry did not come back")
	}
	if st.Tree.Next != 8 {
		t.Fatalf("the funding output is free again: next %d", st.Tree.Next)
	}
	// Read again, nothing is added twice.
	if _, err := st.replayAds(dir, t.Logf); err != nil {
		t.Fatal(err)
	}
	if len(st.PendingAds) != 1 {
		t.Fatal("a row was replayed twice")
	}
}

func TestAdJournalLeavesAFinishedAdvertisementAlone(t *testing.T) {
	st := &State{Holdings: map[string]*Holding{"aa": {Root: "aa", Lease: 900, Ad: &AdRec{Txid: "t2", Expiry: 900}}},
		Entries: map[string]*EntryRec{}}
	st.replayAd(&adRow{Ad: PendingAd{Root: "aa", Txid: "t1", Expiry: 500}})
	if len(st.PendingAds) != 0 {
		t.Fatal("a row older than the live advertisement came back as pending")
	}
}

func TestEntryIndexFollowsDecisions(t *testing.T) {
	a := &EntryRec{C: "a", Root: "r", Writer: "w", Hold: &HoldRec{Txid: "h", Vout: 1}}
	b := &EntryRec{C: "b", Root: "r", Writer: "w", Decision: decisionHeld, Hold: &HoldRec{Txid: "h", Vout: 2}}
	p := &Provider{index: newEntryIndex(map[string]*EntryRec{"a": a, "b": b})}
	if len(p.index.undecided["r"]) != 1 || p.index.writers["w"] != 1 || p.index.total != 1 {
		t.Fatalf("undecided: %+v", p.index)
	}
	if p.index.holds["h.2"] != "b" {
		t.Fatal("a held entry's hold output is not indexed")
	}
	p.setDecision(a, decisionHeld)
	if len(p.index.undecided) != 0 || len(p.index.writers) != 0 || p.index.total != 0 {
		t.Fatalf("an entry decided is still undecided: %+v", p.index)
	}
	if p.index.holds["h.1"] != "a" {
		t.Fatal("the entry just held is not indexed")
	}
	p.setDecision(a, "evicted")
	if p.index.holds["h.1"] != "" {
		t.Fatal("an entry no longer held keeps its hold output")
	}
}

// A tree's row brings back its pending record, unless the state adopted
// or kept the tree already.
func TestTreeJournalBringsBackAPendingTree(t *testing.T) {
	row := &adRow{Tree: &PendingTree{Tree: funding.Tree{Txid: "tree", Count: 16}}}
	st := &State{Holdings: map[string]*Holding{}, Entries: map[string]*EntryRec{}}
	st.replayAd(row)
	st.replayAd(row)
	if len(st.PendingTrees) != 1 || st.PendingTrees[0].Tree.Txid != "tree" {
		t.Fatalf("pending trees %+v", st.PendingTrees)
	}
	adopted := &State{Holdings: map[string]*Holding{}, Entries: map[string]*EntryRec{}, Tree: &funding.Tree{Txid: "tree"}, Trees: []funding.Tree{{Txid: "tree"}}}
	adopted.replayAd(row)
	if len(adopted.PendingTrees) != 0 {
		t.Fatal("a tree the state adopted came back as pending")
	}
}

// A start from a checkpoint before the log (no generation), the files of
// the journal before it and the log since: the checkpoint's rows, the
// files' rows and the log's rows all come back, the log's in order, and
// a log older than the checkpoint's generation is left alone.
func TestStateLoadsCheckpointFilesAndLog(t *testing.T) {
	dir := t.TempDir()
	old := &State{path: filepath.Join(dir, "state.json"), Holdings: map[string]*Holding{"h0": {Root: "h0", Verified: true}},
		Entries: map[string]*EntryRec{"e0": {C: "e0", Root: "h0", Decision: decisionHeld}}, Tree: &funding.Tree{Txid: "tree", Next: 1, Count: 16}}
	if err := old.save(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(adRow{Entries: []EntryRec{{C: "e1", Root: "h1"}}})
	if err := os.MkdirAll(filepath.Join(dir, "ads"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ads", "entry-e1.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := openLog(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	rows := []*logRow{
		{Entries: []EntryRec{{C: "e2", Root: "h2"}}, Pieces: map[string]*Unclaimed{"p1": {Root: "h2", Size: 7}}},
		{Hold: &Holding{Root: "h2", Verified: true}, Claimed: []string{"p1"}},
		{Ad: &PendingAd{Root: "h2", Txid: "ad2", Tree: "tree", Vout: 1}},
		{Entries: []EntryRec{{C: "e2", Root: "h2", Decision: decisionHeld}}},
		{AdDone: "ad2", Hold: &Holding{Root: "h2", Verified: true, Ad: &AdRec{Txid: "ad2"}}, Out: []Outgoing{{Host: "a", Topic: "t", Txid: "ad2"}}},
		{OutDone: []Outgoing{{Host: "a", Topic: "t", Txid: "ad2"}}},
		{Free: map[string]int{"op": 1}}, {Free: map[string]int{"op": 0}},
	}
	for _, r := range rows {
		if _, err := l.append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.flush(); err != nil {
		t.Fatal(err)
	}
	// A cut line at the end, as a crash leaves one.
	f, _ := os.OpenFile(logPath(dir, 0), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString(`{"entries":[{"c":"e9"`)
	f.Close()
	l.close()
	st := loadSaved(t, dir)
	if st.Entries["e0"] == nil || st.Entries["e1"] == nil || st.Entries["e9"] != nil {
		t.Fatalf("entries %v", st.Entries)
	}
	if e := st.Entries["e2"]; e == nil || e.Decision != decisionHeld {
		t.Fatalf("e2 %+v", e)
	}
	if h := st.Holdings["h2"]; h == nil || h.Ad == nil || h.Ad.Txid != "ad2" {
		t.Fatalf("h2 %+v", h)
	}
	if len(st.PendingAds) != 0 || len(st.Outbox) != 0 || len(st.Unclaimed) != 0 || len(st.Free) != 0 {
		t.Fatalf("pending %d outbox %d unclaimed %d free %d", len(st.PendingAds), len(st.Outbox), len(st.Unclaimed), len(st.Free))
	}
	if st.Tree.Next != 2 {
		t.Fatalf("the funding output is free again: next %d", st.Tree.Next)
	}
	// A checkpoint at generation 2 leaves generation 1 alone and takes 2 on.
	l1, _ := openLog(dir, 1)
	_, _ = l1.append(&logRow{Entries: []EntryRec{{C: "stale"}}})
	l1.flush()
	l1.close()
	l2, _ := openLog(dir, 2)
	_, _ = l2.append(&logRow{Entries: []EntryRec{{C: "fresh"}}})
	l2.flush()
	l2.close()
	cp := &State{path: filepath.Join(dir, "state.json"), LogGen: 2, Holdings: map[string]*Holding{}, Entries: map[string]*EntryRec{}}
	if err := cp.save(); err != nil {
		t.Fatal(err)
	}
	st = loadSaved(t, dir)
	if st.Entries["stale"] != nil || st.Entries["fresh"] == nil {
		t.Fatalf("entries %v", st.Entries)
	}
}
