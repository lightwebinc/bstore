package publish

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lightwebinc/bcommon/funding"
)

// The state saved is two parts: publisher.json names each transaction and
// carries no bytes of it, a row an output; each transaction is its own
// cold file, written once, gone once nothing names it. Loaded, every
// record and tree carries its transaction again.
func TestStateKeepsTransactionsInColdFiles(t *testing.T) {
	dir := t.TempDir()
	tree := &funding.Tree{Txid: "t1", RawHex: "1111", BumpHex: "1b", Height: 8, Count: 4, Next: 1}
	p := &Publisher{cfg: Config{Dir: dir, Logf: t.Logf}, st: &state{Holds: []HoldRecord{
		{Txid: "a", Vout: 0, Sats: 5, RawHex: "aa", BumpHex: "ab", Height: 7, Sent: 9},
		{Txid: "a", Vout: 1, Sats: 5, RawHex: "aa", BumpHex: "ab", Height: 7, Sent: 9, Used: true, Entry: "e"},
		{Txid: "b", Vout: 0, Sats: 5, RawHex: "bb", Sent: 9, Used: true, Entry: "reclaiming", Reclaim: &ReclaimRecord{Txid: "r", Raw: "rr"}},
	}, Piece: treeSet{Tree: tree, Trees: []funding.Tree{*tree}, Ahead: []funding.Tree{{Txid: "t2", RawHex: "2222", BeefHex: "2e", Count: 4}}}}}
	if err := p.save(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "publisher.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, hex := range []string{"aa", "bb", "1111", "2222", "ab", "1b", "2e"} {
		if strings.Contains(string(raw), `"`+hex+`"`) {
			t.Fatalf("publisher.json carries a transaction's bytes (%s): %s", hex, raw)
		}
	}
	var disk struct {
		Holds   json.RawMessage `json:"holds"`
		HoldTxs json.RawMessage `json:"holdTxs"`
		Groups  []holdGroup     `json:"holdOutputs"`
	}
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	if disk.Holds != nil || disk.HoldTxs != nil || len(disk.Groups) != 2 || disk.Groups[0].Txid != "a" || len(disk.Groups[0].Outs) != 2 ||
		disk.Groups[1].Txid != "b" || disk.Groups[1].Outs[0].Reclaim == nil || disk.Groups[1].Outs[0].Reclaim.Raw != "rr" {
		t.Fatalf("hold outputs on disk: %s", raw)
	}
	want := map[string]txRecord{"a": {Raw: "aa", Bump: "ab", Height: 7}, "b": {Raw: "bb"}, "t1": {Raw: "1111", Bump: "1b", Height: 8}, "t2": {Raw: "2222", Beef: "2e"}}
	for txid, w := range want {
		b, err := os.ReadFile(filepath.Join(dir, "txs", txid+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var r txRecord
		if json.Unmarshal(b, &r) != nil || r != w {
			t.Fatalf("txs/%s.json: %s, want %+v", txid, b, w)
		}
	}
	st, err := loadState(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := st.Holds
	if len(h) != 3 || h[1].RawHex != "aa" || h[1].BumpHex != "ab" || h[1].Height != 7 || h[1].Sent != 9 || !h[1].Used || h[1].Entry != "e" ||
		h[2].RawHex != "bb" || h[2].BumpHex != "" || h[2].Reclaim == nil || h[2].Reclaim.Raw != "rr" || st.HoldOutputs != nil {
		t.Fatalf("loaded: %+v", h)
	}
	if st.Piece.Tree.RawHex != "1111" || st.Piece.Tree.BumpHex != "1b" || st.Piece.Trees[0].RawHex != "1111" || st.Piece.Ahead[0].BeefHex != "2e" || st.Piece.Ahead[0].RawHex != "2222" {
		t.Fatalf("loaded trees: %+v", st.Piece)
	}
	// A record dropped takes its transaction's file with it, once nothing
	// names the transaction; a file nothing names is removed on load.
	p.st.Holds = p.st.Holds[:2]
	if err := p.save(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "txs", "b.json")); !os.IsNotExist(err) {
		t.Fatalf("txs/b.json after its record went: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "txs", "orphan.json"), []byte(`{"raw":"00"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadState(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "txs", "orphan.json")); !os.IsNotExist(err) {
		t.Fatalf("an orphan cold file after a load: %v", err)
	}
	// A transaction named and not kept refuses the load.
	if err := os.Remove(filepath.Join(dir, "txs", "a.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := loadState(dir); err == nil || !strings.Contains(err.Error(), "txs/a.json") {
		t.Fatalf("a state missing a cold file loaded: %v", err)
	}
	p.flushSaves()
}

// A state an earlier build saved in one file loads the same, whether the
// transaction is on each hold record or once in holdTxs, and the first
// save after it writes the two parts.
func TestStateLoadsAnEarlierBuildsLayout(t *testing.T) {
	tree := `{"txid":"t1","rawHex":"1111","bumpHex":"1b","height":8,"sats":1,"count":4,"next":1}`
	for _, old := range []string{
		`{"identity":"id","holds":[{"txid":"c","vout":0,"sats":5,"rawHex":"cc","bumpHex":"cb","height":3,"sent":4},{"txid":"d","vout":0,"sats":5,"rawHex":"dd","used":true}],"piece":{"tree":` + tree + `,"trees":[` + tree + `]}}`,
		`{"identity":"id","holds":[{"txid":"c","vout":0,"sats":5,"sent":4},{"txid":"d","vout":0,"sats":5,"used":true}],"holdTxs":{"c":{"raw":"cc","bump":"cb","height":3},"d":{"raw":"dd"}},"piece":{"tree":` + tree + `,"trees":[` + tree + `]}}`,
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "publisher.json"), []byte(old), 0o600); err != nil {
			t.Fatal(err)
		}
		st, known, err := readState(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Holds) != 2 || st.Holds[0].RawHex != "cc" || st.Holds[0].BumpHex != "cb" || st.Holds[0].Height != 3 || st.Holds[0].Sent != 4 ||
			st.Holds[1].RawHex != "dd" || !st.Holds[1].Used || st.Piece.Tree.RawHex != "1111" || st.HoldTxs != nil || len(known) != 0 {
			t.Fatalf("an earlier build's state: %+v, known %v", st.Holds, known)
		}
		p := &Publisher{cfg: Config{Dir: dir, Logf: t.Logf}, st: st}
		p.sv.cold = known
		if err := p.save(); err != nil {
			t.Fatal(err)
		}
		p.flushSaves()
		if p.sv.coldBytes == 0 {
			t.Fatal("the first save after an earlier build's state wrote no cold file")
		}
		raw, err := os.ReadFile(filepath.Join(dir, "publisher.json"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"cc"`) || strings.Contains(string(raw), `"1111"`) || strings.Contains(string(raw), "holdTxs") {
			t.Fatalf("publisher.json after the first save: %s", raw)
		}
		for _, txid := range []string{"c", "d", "t1"} {
			if _, err := os.Stat(filepath.Join(dir, "txs", txid+".json")); err != nil {
				t.Fatal(err)
			}
		}
		again, err := loadState(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(again.Holds) != 2 || again.Holds[0].RawHex != "cc" || again.Holds[0].BumpHex != "cb" || again.Holds[0].Height != 3 ||
			again.Holds[1].RawHex != "dd" || !again.Holds[1].Used || again.Piece.Tree.RawHex != "1111" || again.Piece.Trees[0].BumpHex != "1b" {
			t.Fatalf("the migrated state: %+v %+v", again.Holds, again.Piece)
		}
	}
}
