package provider

import (
	"context"
	"time"
)

// recover is what a start does before it serves: the tip for coinbase
// maturity, the proofs of change the pool holds back, every funding tree a
// stopped run signed and did not adopt, and every advertisement it
// persisted and did not see mined.
func (p *Provider) recover(ctx context.Context) error {
	if tip, err := p.tip(ctx); err == nil {
		p.payer.Tip = tip
	} else {
		p.logf("%v; maturity is judged at height 0 until the node answers", err)
	}
	p.collectChange(ctx)
	if err := p.recoverTrees(ctx); err != nil {
		return err
	}
	p.resumeAds(ctx)
	p.mu.Lock()
	p.evictLocked(true)
	if p.front != nil {
		// A front that stopped between adopting an advertisement and
		// dropping its copy drops it now.
		for r, h := range p.st.Holdings {
			if h.Handed {
				p.dropStagedLocked(r)
			}
		}
	}
	p.mu.Unlock()
	p.collect()
	return nil
}

// collectChange gives every unproven coin in the pool its proof once its
// transaction mined.
func (p *Provider) collectChange(ctx context.Context) {
	for _, id := range p.pool.UnprovenTxids() {
		mp, h, err := p.chain.Proof(ctx, id)
		if err != nil {
			continue
		}
		_, _ = p.pool.Prove(id, mp.Hex(), h)
	}
}

// background runs the work that is no request's: advertisements pending,
// the outbox, change, entries still undecided, payouts, settling payments,
// eviction.
func (p *Provider) background(ctx context.Context) {
	t := time.NewTicker(p.opts.Tick)
	defer t.Stop()
	settling := make(chan struct{}, 1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		_, _ = p.tip(ctx)
		p.collectChange(ctx)
		p.waitPending(ctx)
		p.confirmHeld(ctx)
		p.makeOwed(ctx)
		p.recheckDamaged(ctx)
		p.publishOutbox(ctx)
		p.mu.Lock()
		var roots []string
		for _, e := range p.st.Entries {
			if e.Decision == "" {
				roots = append(roots, e.Root)
			}
		}
		p.evictLocked(true)
		live := map[string]bool{}
		for _, e := range p.st.Entries {
			if e.Hold != nil {
				live[e.Hold.Outpoint()] = true
			}
		}
		for op := range p.st.Free {
			if !live[op] {
				delete(p.st.Free, op)
				p.dirty = true
			}
		}
		for c := range p.scanned {
			if e := p.st.Entries[c]; e == nil || !p.payoutOpenLocked(c) {
				delete(p.scanned, c)
			}
		}
		cutoff := p.now().Add(-fundingMemory).Unix()
		for k, f := range p.st.Fundings {
			if f.At < cutoff {
				delete(p.st.Fundings, k)
				p.dirty = true
			}
		}
		if p.dirty {
			_ = p.save()
		}
		p.metrics.set("bstore_provider_holdings", float64(len(p.st.Holdings)))
		p.metrics.set("bstore_provider_store_bytes", float64(p.usageLocked()))
		p.metrics.set("bstore_provider_sessions", float64(p.auth.Sessions()))
		p.mu.Unlock()
		for _, r := range roots {
			p.enqueue(r)
		}
		p.scanPayouts(ctx)
		select {
		case settling <- struct{}{}:
			go func() {
				defer func() { <-settling }()
				p.settlePayments(ctx)
			}()
		default:
		}
	}
}

// waitPending finishes advertisements that mined since they were sent,
// and sends again those a previous attempt could not.
func (p *Provider) waitPending(ctx context.Context) {
	p.mu.Lock()
	n := len(p.st.PendingAds)
	p.mu.Unlock()
	if n == 0 {
		return
	}
	if !p.mintMu.TryLock() {
		return // an advertisement is being minted; next tick
	}
	defer p.mintMu.Unlock()
	p.resumeAds(ctx)
}

// fundingMemory is how long a funding outpoint's carrier is remembered:
// longer than an unclaimed piece or an undecided entry is kept.
const fundingMemory = 7 * 24 * time.Hour
