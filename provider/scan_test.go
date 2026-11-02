package provider

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
)

type countedSpends struct{ reads atomic.Int32 }

func (c *countedSpends) Spender(context.Context, string, uint32) (string, error) {
	c.reads.Add(1)
	return "", nil
}

// An entry waiting for its race is looked at less and less often: the scan
// is a backstop, and a read a minute per entry kept a node busy with no put
// running.
func TestScanPayoutsBacksOff(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	spends := &countedSpends{}
	p := &Provider{opts: Options{Now: func() time.Time { return now }}, chain: &nodeapi.Sources{Spends: spends}}
	p.st = &State{Entries: map[string]*EntryRec{"c": {C: "c", Decision: decisionHeld, Hold: &HoldRec{Txid: "t", Vout: 1}}},
		Payouts: map[string]*Payout{}}
	var at []time.Duration
	start := now
	for now.Sub(start) < 8*time.Hour {
		before := spends.reads.Load()
		p.scanPayouts(context.Background())
		if spends.reads.Load() != before {
			at = append(at, now.Sub(start))
		}
		now = now.Add(time.Minute)
	}
	if len(at) < 4 || at[0] != 0 || at[1] != scanEvery {
		t.Fatalf("looks at %v", at)
	}
	for i := 2; i < len(at); i++ {
		gap := at[i] - at[i-1]
		if gap < at[i-1]-at[i-2] || gap > scanMost+time.Minute {
			t.Fatalf("looks at %v: gap %v after %v", at, gap, at[i-1]-at[i-2])
		}
	}
	if len(at) > 16 {
		t.Fatalf("%d looks in eight hours: %v", len(at), at)
	}
}

// A checked hold transaction answers for its outputs until it ages out.
func TestMinedHoldMemory(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	p := &Provider{opts: Options{Now: func() time.Time { return now }}}
	if p.minedHoldOf("t") != nil {
		t.Fatal("known before it was noted")
	}
	tx := transaction.NewTransaction()
	p.noteMinedHold("t", tx, 700)
	if m := p.minedHoldOf("t"); m == nil || m.tx != tx || m.height != 700 {
		t.Fatalf("%+v", m)
	}
	now = now.Add(minedHoldTTL)
	if p.minedHoldOf("t") != nil {
		t.Fatal("answered past its time")
	}
}
