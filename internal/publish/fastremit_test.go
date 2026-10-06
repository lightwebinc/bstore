package publish

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/acceptance"
	bpub "github.com/lightwebinc/bcommon/publish"
)

// A settlement whose largest payout is at or below the threshold is
// remitted once arcade has it and the node shows its inputs spent by it
// alone, before it mines: the race is "remitted", and a later settle
// collects the proof and marks it settled. Above the threshold the writer
// waits for the block as before.
func TestSettleRemitsBeforeMining(t *testing.T) {
	r := newRaceRig(t)
	r.p.cfg.Accept = acceptance.DefaultPolicy()
	r.p.cfg.Status = &bpub.Arcade{Base: r.cs.URL + "/arcade"}
	race := r.put("k", Bounty{Satoshis: 10_000, T: 1, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a, b := r.provider("a"), r.provider("b")
	adA := r.advertise(a, 2_000_000_000, nil)
	adB := r.advertise(b, 2_000_000_000, nil)
	passed(race, a, adA, 1)
	passed(race, b, adB, 2)
	r.chain.SetHold(true)
	t0 := time.Now()
	rec, err := r.p.Settle(r.ctx, race, false)
	took := time.Since(t0)
	if rec == nil || race.State != RaceRemitted || r.chain.Mined(rec.Txid) || r.chain.Tx(rec.Txid) == nil {
		t.Fatalf("remitted before mining: %v %+v %s", err, rec, race.State)
	}
	// The payees have no front here: the remittances were tried at once,
	// built from the unmined settlement and its proven parents.
	if !errors.Is(err, ErrNotYet) || race.Settlement.Payees[0].Last == "" || rec.Bump != "" {
		t.Fatalf("remittance attempts: %v %+v", err, race.Settlement.Payees)
	}
	if took > 20*time.Second {
		t.Fatalf("a small settlement waited %s", took)
	}
	r.chain.SetHold(false)
	r.chain.Mine()
	if _, err := r.p.Settle(r.ctx, race, false); race.State != RaceSettled || race.Settlement.Bump == "" {
		t.Fatalf("settled once mined: %v %s", err, race.State)
	}
}

func TestSettleAboveThresholdWaits(t *testing.T) {
	r := newRaceRig(t)
	r.p.cfg.Accept = acceptance.DefaultPolicy()
	r.p.cfg.Accept.ThresholdSats = 1000
	r.p.cfg.Status = &bpub.Arcade{Base: r.cs.URL + "/arcade"}
	race := r.put("k", Bounty{Satoshis: 10_000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a := r.provider("a")
	passed(race, a, r.advertise(a, 2_000_000_000, nil), 1)
	r.chain.SetHold(true)
	ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
	defer cancel()
	rec, err := r.p.Settle(ctx, race, false)
	if !errors.Is(err, ErrNotYet) || rec == nil || race.State != RaceSettling {
		t.Fatalf("a large settlement waits for its block: %v %s", err, race.State)
	}
	if race.Settlement.Payees[0].Last != "" || race.Settlement.Payees[0].Remitted {
		t.Fatal("remitted before mining above the threshold")
	}
	r.chain.SetHold(false)
	r.chain.Mine()
}
