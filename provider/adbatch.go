package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/mint"
)

// The minter: one advertisement transaction for several roots.
//
// An advertisement is an output (spec section 9.1), and a mined
// transaction may carry up to storerec.MaxAdsPerTx of them (section 12.1);
// nothing says one a transaction. One a transaction pays the fee floor for
// about 400 bytes, and the settlement leg takes transactions at a rate of
// its own (a node announces about 150 a second), so a provider holding
// small objects was bounded by that rate, one object an advertisement.
// The minter collects the roots asked for inside limits.ad_batch_ms behind
// the first, up to limits.ad_batch, and signs one transaction with an
// output each: the fee is the floor plus a few satoshis an output, the leg
// sees one transaction for the batch, and each root's row (PendingAd,
// AdVout) is finished when the one proof arrives. The funding outputs are
// the provider's own, spent as many as the fee needs.

// adAsk is one root's request: its advertisement output, signed by the
// asker (the signature is the slow part, and runs on the workers side by
// side), and where the answer goes.
type adAsk struct {
	root  string
	until uint64
	lock  *script.Script
	reply chan adReply
}

// adReply is the minter's answer: the row journaled for the root and the
// transaction, already on its way to the settlement leg.
type adReply struct {
	pa  PendingAd
	tx  *transaction.Transaction
	err error
}

// adMinter collects asks into batches and mints each. It runs for the
// provider's life; an ask after Close is answered with the run's end.
func (p *Provider) adMinter(ctx context.Context) {
	for {
		var asks []adAsk
		select {
		case a := <-p.adAsks:
			asks = append(asks, a)
		case <-ctx.Done():
			return
		}
		wait := time.NewTimer(time.Duration(p.cfg.Limits.AdBatchMs) * time.Millisecond)
	collect:
		for len(asks) < p.cfg.Limits.AdBatch {
			select {
			case a := <-p.adAsks:
				asks = append(asks, a)
			case <-wait.C:
				break collect
			case <-ctx.Done():
				wait.Stop()
				for _, a := range asks {
					a.reply <- adReply{err: ctx.Err()}
				}
				return
			}
		}
		wait.Stop()
		p.mintBatch(ctx, asks)
	}
}

// fundingInputBytes is what one funding input adds to a transaction's
// size: outpoint, a P2PKH unlocking script and its length, sequence.
const fundingInputBytes = 150

// mintBatch signs one transaction for asks, journals a row a root, puts
// it on the settlement leg once, and answers every ask.
func (p *Provider) mintBatch(ctx context.Context, asks []adAsk) {
	minted := time.Now()
	fail := func(err error) {
		for _, a := range asks {
			a.reply <- adReply{err: err}
		}
	}
	locks := make([]*script.Script, len(asks))
	outBytes := 0
	for i, a := range asks {
		locks[i] = a.lock
		outBytes += 9 + len(*a.lock)
	}
	fees := p.fees()
	fees.Dust = p.cfg.TreeSats + 1
	change, err := p.signer.FundScript()
	if err != nil {
		fail(err)
		return
	}
	// Funding outputs enough for the fee, estimated from the size before
	// any is taken (one taken is marked used whatever happens next), then
	// one more at a time should the estimate fall short.
	var inputs []mint.Input
	take := func() error {
		tree, vout, err := p.spendTree(ctx)
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.st.markSpent(tree.TxID().String(), vout)
		p.record(&logRow{Spent: &spentRow{Tree: tree.TxID().String(), Vout: vout}})
		p.mu.Unlock()
		inputs = append(inputs, mint.Input{Tx: tree, Vout: vout, Unlocker: p.signer.FundUnlocker()})
		return nil
	}
	need := func(k int) uint64 {
		f, _ := fees.For(12 + k*fundingInputBytes + outBytes)
		return f + uint64(len(asks))
	}
	k := 1
	for uint64(k)*p.cfg.TreeSats < need(k) && k < len(asks)+8 {
		k++
	}
	p.mintMu.Lock()
	locked := time.Now()
	for len(inputs) < k {
		if err = take(); err != nil {
			break
		}
	}
	var tx *transaction.Transaction
	for attempt := 0; err == nil && attempt < 8; attempt++ {
		tx, err = mint.Many(locks, 1, inputs, change, fees)
		var short *mint.InsufficientError
		if errors.As(err, &short) {
			err = take()
			tx = nil
			continue
		}
		break
	}
	p.mintMu.Unlock()
	p.metrics.inc("bstore_provider_mint_lock_total")
	p.metrics.add("bstore_provider_mint_lock_seconds_total", time.Since(locked).Seconds())
	if err != nil {
		fail(fmt.Errorf("advertisement transaction: %w", err))
		return
	}
	if tx == nil {
		fail(errors.New("advertisement transaction: the funding outputs taken could not pay its fee"))
		return
	}
	txid, raw, at := tx.TxID().String(), hex.EncodeToString(tx.Bytes()), p.now().Unix()
	pas := make([]PendingAd, len(asks))
	for i, a := range asks {
		pas[i] = PendingAd{Root: a.root, Txid: txid, AdVout: uint32(i), RawHex: raw, Expiry: a.until,
			Tree: inputs[0].Tx.TxID().String(), Vout: inputs[0].Vout, At: at}
	}
	p.metrics.timed("bstore_provider_advertise_seconds", minted, "stage=mint")
	journaled := time.Now()
	if err := p.journalAds(pas); err != nil {
		fail(err)
		return
	}
	p.metrics.timed("bstore_provider_advertise_seconds", journaled, "stage=journal")
	p.metrics.add("bstore_provider_advertisement_roots_total", float64(len(asks)))
	p.metrics.inc("bstore_provider_advertisement_batches_total")
	// Sent once for every root, behind a send slot like a lone one.
	slot := time.Now()
	select {
	case p.adSends <- struct{}{}:
	case <-ctx.Done():
		fail(ctx.Err())
		return
	}
	p.metrics.timed("bstore_provider_advertise_seconds", slot, "stage=slot")
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.adSends }()
		if err := p.sendAd(ctx, pas[0], tx); err != nil {
			p.logf("advertisement %s (%d roots): %v", short(txid), len(pas), err)
		}
	}()
	for i, a := range asks {
		a.reply <- adReply{pa: pas[i], tx: tx}
	}
}
