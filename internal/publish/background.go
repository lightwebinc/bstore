package publish

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Remitted reports whether every payee of the race's settlement has what
// it is owed: remitted to, refused, or seen spending its output; a payee a
// front answered for is told by that front.
func (r *Race) Remitted() bool {
	if r.Settlement == nil {
		return true
	}
	for _, p := range r.Settlement.Payees {
		if !p.Done() && r.Forwarder(p.Advertiser) == "" {
			return false
		}
	}
	return true
}

// RaceTick is one pass of the writer's own race work over every race of
// this identity, as `bstore holds`, `settle` and `reclaim` do it by hand:
// an open race is judged (Step) and, once closed, settled by rank; a
// settlement on its way is followed to its block and its remittances sent
// again until taken; a race with nothing to settle is reclaimed once its
// deadline has come. Each race is saved as it goes; one that cannot be
// decided now is left for the next pass. It returns how many races are
// still open or owed something.
func (p *Publisher) RaceTick(ctx context.Context, o RaceOptions) int {
	races, err := p.Races()
	if err != nil {
		p.cfg.Logf("races: %v", err)
		return 0
	}
	p.stepOpen(ctx, races, o)
	left := 0
	for _, r := range races {
		if ctx.Err() != nil {
			return left
		}
		switch r.State {
		case RaceOpen:
			switch {
			case r.State == RaceOpen && (r.Settlement != nil || r.Closed(p.cfg.Now(), o.Window)):
				p.settleQuiet(ctx, r)
			case r.State == RaceLapsed || r.State == RaceClosed:
				p.reclaimQuiet(ctx, r)
			}
		case RaceSettling, RaceRemitted:
			p.settleQuiet(ctx, r)
		case RaceSettled:
			if !r.Remitted() {
				p.settleQuiet(ctx, r)
			}
		case RaceClosed, RaceLapsed, RaceReclaim:
			p.reclaimQuiet(ctx, r)
		}
		switch r.State {
		case RaceOpen, RaceSettling, RaceRemitted, RaceReclaim, RaceClosed, RaceLapsed:
			left++
		case RaceSettled:
			if !r.Remitted() {
				left++
			}
		}
	}
	return left
}

// stepOpen steps every open race with nothing settled yet, o.Parallel at a
// time. Each step touches only its own race (its file, its candidates);
// what the steps share (the store, the reader, the fronts) already serves
// a race's candidates concurrently, and each front's calls go one at a
// time through its own client.
func (p *Publisher) stepOpen(ctx context.Context, races []*Race, o RaceOptions) {
	n := max(o.Parallel, 1)
	sem := make(chan struct{}, n)
	var wg sync.WaitGroup
	for _, r := range races {
		if r.State != RaceOpen || r.Settlement != nil {
			continue
		}
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(r *Race) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := p.Step(ctx, r, o); err != nil {
				p.cfg.Logf("race %s: %v", short(r.Entry), err)
			}
		}(r)
	}
	wg.Wait()
}

// settleQuiet and reclaimQuiet are the pass's settlement and reclaim: sent
// and left to a later pass, never waited for, so one pass covers every
// race whatever the block interval.
func (p *Publisher) settleQuiet(ctx context.Context, r *Race) {
	_, err := p.settle(ctx, r, false, false)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotYet):
	case errors.Is(err, ErrPastDeadline):
		p.reclaimQuiet(ctx, r)
	default:
		p.cfg.Logf("race %s: settlement: %v", short(r.Entry), err)
	}
}

func (p *Publisher) reclaimQuiet(ctx context.Context, r *Race) {
	_, err := p.reclaim(ctx, r, false)
	if err != nil && !errors.Is(err, ErrBeforeDeadline) && !errors.Is(err, ErrNotYet) {
		p.cfg.Logf("race %s: reclaim: %v", short(r.Entry), err)
	}
}

// Background runs RaceTick every tick, and collects payments' change and
// settles payments not answered, until ctx ends. It is what a gateway runs
// for its own identities (spec section 9.4: the writer's own client judges
// the race).
func (p *Publisher) Background(ctx context.Context, o RaceOptions, tick time.Duration) {
	if tick <= 0 {
		tick = 10 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	// The housekeeping on a goroutine of its own, so that a long pass over
	// the races never holds it off.
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.housekeeping(ctx)
	}()
	defer func() { <-done }()
	for {
		p.RaceTick(ctx, o)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// housekeeping collects payments' change, settles payments not answered
// and prunes the state, once a minute until ctx ends.
func (p *Publisher) housekeeping(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		p.CollectChange(ctx)
		if _, err := p.MergeCoins(ctx); err != nil {
			p.cfg.Logf("coin merge: %v", feeError(err))
		}
		p.ReconcilePayments(ctx)
		p.Prune()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
