package publish

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The state keeps each hold transaction once on disk, and every record
// carries it again in memory; a state an earlier build saved with the
// transaction on each record loads the same.
func TestStateKeepsEachHoldTransactionOnce(t *testing.T) {
	dir := t.TempDir()
	p := &Publisher{cfg: Config{Dir: dir, Logf: t.Logf}, st: &state{Holds: []HoldRecord{
		{Txid: "a", Vout: 0, Sats: 5, RawHex: "aa", BumpHex: "ab", Height: 7},
		{Txid: "a", Vout: 1, Sats: 5, RawHex: "aa", BumpHex: "ab", Height: 7, Used: true, Entry: "e"},
		{Txid: "b", Vout: 0, Sats: 5, RawHex: "bb", Sent: 9},
	}}}
	if err := p.save(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "publisher.json"))
	if err != nil {
		t.Fatal(err)
	}
	var disk struct {
		Holds   []map[string]any  `json:"holds"`
		HoldTxs map[string]holdTx `json:"holdTxs"`
	}
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	if len(disk.HoldTxs) != 2 || disk.HoldTxs["a"] != (holdTx{Raw: "aa", Bump: "ab", Height: 7}) || disk.HoldTxs["b"] != (holdTx{Raw: "bb"}) {
		t.Fatalf("hold transactions on disk: %+v", disk.HoldTxs)
	}
	for _, h := range disk.Holds {
		if _, ok := h["rawHex"]; ok {
			t.Fatalf("a record carries its transaction on disk: %v", h)
		}
	}
	st, err := loadState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Holds) != 3 || st.Holds[1].RawHex != "aa" || st.Holds[1].BumpHex != "ab" || st.Holds[1].Height != 7 || !st.Holds[1].Used ||
		st.Holds[2].RawHex != "bb" || st.Holds[2].BumpHex != "" || st.HoldTxs != nil {
		t.Fatalf("loaded: %+v", st.Holds)
	}
	// The format an earlier build saved: the transaction on each record.
	old := `{"holds":[{"txid":"c","vout":0,"sats":5,"rawHex":"cc","bumpHex":"cb","height":3}]}`
	if err := os.WriteFile(filepath.Join(dir, "publisher.json"), []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	if st, err = loadState(dir); err != nil || len(st.Holds) != 1 || st.Holds[0].RawHex != "cc" || st.Holds[0].BumpHex != "cb" {
		t.Fatalf("an earlier build's state: %v %+v", err, st.Holds)
	}
}
