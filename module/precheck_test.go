package module

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

// Precheck before the lock and IdentifyAdmissibleOutputs under it decide
// every golden transaction vector as the manager does without a precheck,
// and count each verdict once, when the host shows the manager the
// transaction; Precheck alone counts and logs nothing. A precheck is used
// only by the manager that made it and only for the bytes it read.
func TestPrecheckDecidesAsAdmit(t *testing.T) {
	v, h := readTxVectors(t)
	direct, checked := newFakeHost(), newFakeHost()
	tmDirect, err := NewRecordsTopicManager(topic, direct, headersOf(h), storerec.DefaultMaxBEEF, storerec.DefaultMaxSweepInputs)
	if err != nil {
		t.Fatal(err)
	}
	tm, err := NewRecordsTopicManager(topic, checked, headersOf(h), storerec.DefaultMaxBEEF, storerec.DefaultMaxSweepInputs)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range v.Cases {
		raw := beefOf(t, c)
		coins := make([]uint32, 0, len(c.PreviousCoins))
		for _, i := range c.PreviousCoins {
			coins = append(coins, uint32(i)) //nolint:gosec // vector indices are small
		}
		want, werr := tmDirect.Admit(context.Background(), raw, c.PreviousCoins)

		before := maps.Clone(checked.counters)
		lines := len(checked.lines)
		ctx := tm.Precheck(bmodule.WithSubmission(context.Background(), raw))
		if !maps.Equal(before, checked.counters) || len(checked.lines) != lines {
			t.Fatalf("%s: Precheck counted or logged", c.Name)
		}
		got, gerr := tm.IdentifyAdmissibleOutputs(ctx, nil, nil, coins)
		if fmt.Sprint(werr) != fmt.Sprint(gerr) {
			t.Fatalf("%s: without a precheck %v, with one %v", c.Name, werr, gerr)
		}
		equalU32(t, got.OutputsToAdmit, want.OutputsToAdmit, c.Name+" outputs")
		equalU32(t, got.CoinsToRetain, want.CoinsToRetain, c.Name+" coins")
	}
	if !maps.Equal(direct.counters, checked.counters) {
		t.Fatalf("counted %v, without a precheck %v", checked.counters, direct.counters)
	}

	put, piece := beefOf(t, v.byName(t, "entry-put")), beefOf(t, v.byName(t, "piece"))
	// The verdict in ctx is what decides bytes it was made for: a verdict
	// prepared from the piece but filed under the entry's bytes refuses the
	// entry, which shows it is used rather than decided again.
	forged := context.WithValue(bmodule.WithSubmission(context.Background(), put), precheckKey{tm},
		&precheck{raw: put, p: storerec.Prepare(context.Background(), piece, tm.h)})
	var r *Refused
	if _, err := tm.IdentifyAdmissibleOutputs(forged, nil, nil, nil); !errors.As(err, &r) || r.Reason != "piece" {
		t.Fatalf("a verdict in ctx for these bytes: %v", err)
	}
	// For other bytes it is ignored, and so is another manager's.
	pieceChecked := tm.Precheck(bmodule.WithSubmission(context.Background(), piece))
	if _, err := tm.Admit(pieceChecked, put, nil); err != nil {
		t.Fatalf("a precheck of other bytes decided the entry: %v", err)
	}
	other, err := NewRecordsTopicManager(topic, newFakeHost(), headersOf(h), storerec.DefaultMaxBEEF, storerec.DefaultMaxSweepInputs)
	if err != nil {
		t.Fatal(err)
	}
	forged = context.WithValue(bmodule.WithSubmission(context.Background(), put), precheckKey{other},
		&precheck{raw: put, p: storerec.Prepare(context.Background(), piece, tm.h)})
	if _, err := tm.IdentifyAdmissibleOutputs(forged, nil, nil, nil); err != nil {
		t.Fatalf("another manager's verdict decided the entry: %v", err)
	}
	// No submission in ctx: nothing to precheck.
	if ctx := context.Background(); tm.Precheck(ctx) != ctx {
		t.Fatal("a precheck without a submission")
	}
}
