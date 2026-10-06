package publish

import (
	"testing"
	"time"
)

// A race remitted before mining is aged from its remittance (or its start,
// for a record from before v0.5.0); a settlement abandoned after it was
// remitted unmined makes the next one wait for its block.
func TestRemittedAgeAndAbandon(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	r := &Race{State: RaceRemitted, Started: now.Add(-3 * time.Hour).Unix(), Settlement: &SettleRecord{RemittedAt: now.Add(-2 * time.Hour).Unix()}}
	if a, ok := r.RemittedAge(now); !ok || a != 2*time.Hour {
		t.Fatalf("age %v %v", a, ok)
	}
	r.Settlement.RemittedAt = 0
	if a, _ := r.RemittedAge(now); a != 3*time.Hour {
		t.Fatalf("age from start %v", a)
	}
	r.State = RaceSettled
	if _, ok := r.RemittedAge(now); ok {
		t.Fatal("a settled race aged")
	}
	if r.RemittedBefore() {
		t.Fatal("nothing abandoned")
	}
	r.Abandoned = []SettleRecord{{Txid: "a"}}
	if r.RemittedBefore() {
		t.Fatal("an abandoned settlement never remitted unmined")
	}
	r.Abandoned = append(r.Abandoned, SettleRecord{Txid: "b", RemittedAt: 1})
	if !r.RemittedBefore() {
		t.Fatal("an abandoned remitted settlement not seen")
	}
}
