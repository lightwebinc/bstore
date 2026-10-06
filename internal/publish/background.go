package publish

import (
	"context"
	"errors"
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
	left := 0
	for _, r := range races {
		if ctx.Err() != nil {
			return left
		}
		switch r.State {
		case RaceOpen:
			if r.Settlement == nil {
				if err := p.Step(ctx, r, o); err != nil {
					p.cfg.Logf("race %s: %v", short(r.Entry), err)
				}
			}
			switch {
			case r.State == RaceOpen && (r.Settlement != nil || r.Closed(p.cfg.Now(), o.Window)):
				p.settleQuiet(ctx, r)
			case r.State == RaceLapsed || r.State == RaceClosed:
				p.reclaimQuiet(ctx, r)
			}
		case RaceSettling:
			p.settleQuiet(ctx, r)
		case RaceSettled:
			if !r.Remitted() {
				p.settleQuiet(ctx, r)
			}
		case RaceClosed, RaceLapsed, RaceReclaim:
			p.reclaimQuiet(ctx, r)
		}
		switch r.State {
		case RaceOpen, RaceSettling, RaceReclaim, RaceClosed, RaceLapsed:
			left++
		case RaceSettled:
			if !r.Remitted() {
				left++
			}
		}
	}
	return left
}

func (p *Publisher) settleQuiet(ctx context.Context, r *Race) {
	_, err := p.Settle(ctx, r, false)
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
	_, err := p.Reclaim(ctx, r)
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
	last := time.Time{}
	for {
		p.RaceTick(ctx, o)
		if time.Since(last) >= time.Minute {
			p.CollectChange(ctx)
			p.ReconcilePayments(ctx)
			last = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
