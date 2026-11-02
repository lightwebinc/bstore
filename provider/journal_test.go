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
