package publish

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/producer"

	"github.com/lightwebinc/bstore/storerec"
)

// Hold outputs (spec section 9.2) are minted ahead, up to 16 in one
// transaction, and kept until a put takes one: a writer SHOULD mine its
// hold output well before it publishes the entry (spec section 9.4), and a
// put should not wait for a block. Each is recorded before its transaction
// reaches the settlement leg; one that was handed to the leg and not seen
// mined is asked about again (CheckHolds), and sent again when the node does
// not know it. A fill runs in cycles: a cycle mints the whole deficit, as
// many transactions as it takes and the pool has coins for, waits for them
// all to mine, and goes again while the pool is below HoldAhead. A
// transaction minted while more are owed leaves its change in a coin for
// each of them, so that the next cycle funds them side by side; a cycle
// always leaves the pool one spendable coin, for the funding trees and
// fees a put needs meanwhile.

// ErrNoHold is a put that asked for a bounty when no mined hold output of
// that value was ready within HoldWait; minting more is under way.
var ErrNoHold = errors.New("publish: no mined hold output is ready for the bounty yet")

const (
	maxHoldsPerTx = 16
	// maxHoldChange is how many change coins one hold transaction leaves
	// at most, for the transactions still owed.
	maxHoldChange = 8
)

// takeHold takes a mined, unused hold output of sats, marks it used and
// saves before returning, and delivers its transaction to the bucket's
// records topic. With none ready it starts minting and waits up to
// HoldWait.
func (p *Publisher) takeHold(ctx context.Context, bucket string, sats uint64) (*HoldRecord, error) {
	deadline := time.Now().Add(p.cfg.HoldWait)
	for {
		p.mu.Lock()
		for i := range p.st.Holds {
			h := &p.st.Holds[i]
			if h.Used || h.Sats != sats || h.BumpHex == "" {
				continue
			}
			h.Used = true
			rec := *h
			if err := p.saveLocked(); err != nil {
				h.Used = false
				p.mu.Unlock()
				return nil, err
			}
			p.mu.Unlock()
			go p.FillHolds(context.WithoutCancel(ctx), sats)
			tx, err := funding.Rebuild(rec.RawHex, rec.BumpHex, "")
			if err != nil {
				return nil, err
			}
			if beef, err := aloneBEEF(tx); err == nil {
				if targets := p.cfg.Delivery.recordTargets(bucket, false); len(targets) > 0 {
					if it, err := p.outbox.add(rec.Txid+"-"+short(bucket), "hold output", beef, targets); err == nil {
						p.outbox.send(ctx, it, beef)
					}
				}
			}
			return &rec, nil
		}
		p.mu.Unlock()
		go p.FillHolds(context.WithoutCancel(ctx), sats)
		if time.Now().After(deadline) {
			return nil, ErrNoHold
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// owedLocked is how many hold outputs of sats the pool is below HoldAhead,
// counting those on their way; the caller holds p.mu.
func (p *Publisher) owedLocked(sats uint64) int {
	have := 0
	for _, h := range p.st.Holds {
		if !h.Used && h.Sats == sats {
			have++
		}
	}
	return p.cfg.HoldAhead - have
}

// spendableCoins is how many coins of the pool a mint could take now.
func (p *Publisher) spendableCoins(tip uint32) int {
	n := 0
	for _, o := range p.wallet.Pool.Outputs() {
		if o.Spendable(tip) {
			n++
		}
	}
	return n
}

// FillHolds mints hold outputs of sats until HoldAhead are ready, in
// cycles: each cycle mints the deficit, at most 16 to a transaction and as
// many transactions as the pool has spendable coins for (one is always
// left), then waits for them to mine before it looks again. One fill runs
// at a time; a call while one runs returns at once, and the running fill
// sees the take that called it on its next cycle.
func (p *Publisher) FillHolds(ctx context.Context, sats uint64) {
	if !p.holdMu.TryLock() {
		return
	}
	defer p.holdMu.Unlock()
	for {
		tip := uint32(0)
		if h, err := p.bestHeader(ctx); err == nil {
			tip = h.Height
		}
		var wg sync.WaitGroup
		minted := 0
		for {
			p.mu.Lock()
			owed := p.owedLocked(sats)
			p.mu.Unlock()
			if owed <= 0 {
				break
			}
			if minted > 0 && p.spendableCoins(tip) <= 1 {
				// The rest waits for this cycle's change: a put needs a
				// coin for its funding trees and fees meanwhile.
				break
			}
			n := min(owed, maxHoldsPerTx)
			more := (owed - n + maxHoldsPerTx - 1) / maxHoldsPerTx
			if err := p.mintHolds(ctx, sats, n, more, tip, &wg); err != nil {
				if minted == 0 && ctx.Err() == nil {
					p.cfg.Logf("hold outputs: %v", feeError(err))
				}
				break
			}
			minted++
		}
		if minted == 0 {
			return
		}
		wg.Wait()
		if ctx.Err() != nil {
			return
		}
	}
}

// mintHolds mints one transaction of n hold outputs of sats, hands it to
// the leg and waits for its proof in the background, counted on wg. more
// is how many transactions are still owed after it: its change is left in
// a coin for each and one to spare (at most maxHoldChange), when it funds
// that many whole ones, so that the next cycle mints them side by side and
// a put still finds a coin.
func (p *Publisher) mintHolds(ctx context.Context, sats uint64, n, more int, tip uint32, wg *sync.WaitGroup) error {
	key, err := storerec.LockingKey(storerec.HoldDerivation, p.Identity())
	if err != nil {
		return err
	}
	lock := script.Script(storerec.HoldScript(key))
	// A Payer of its own: a fill runs beside puts, and a Payer's
	// reservations are its own.
	pay := &producer.Payer{Pool: p.wallet.Pool, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer},
		Kept: p.kept, Settler: p.cfg.Settler, Chain: p.cfg.Chain, Fees: p.fees(), Note: p.note, Poll: p.cfg.Poll, Tip: tip}
	parts := 1
	if more > 0 {
		// A coin for each transaction still owed and one to spare, for the
		// funding trees and fees a put needs while the next cycle mints.
		parts = min(more+1, maxHoldChange)
	}
	fee := p.feeAt(uint64(200 + 45*n + 34*parts))
	// A take that finds no coin would collect a funding tree minted ahead
	// (producer.takeAfterMints) from this goroutine, and the put waiting
	// on that mint under mintMu would miss its result and hang: with no
	// coin spendable the fill stops here, and the next cycle finds the
	// change. The take's own wait is kept short for the same reason.
	if p.spendableCoins(tip) == 0 {
		return &producer.NoCoinError{Err: bwallet.ErrNoSpendable, Held: len(p.wallet.Pool.UnprovenTxids())}
	}
	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	coin, err := pay.TakeAtLeast(tctx, uint64(n)*sats+fee+p.dust())
	cancel()
	if err != nil {
		return err
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(coin.Tx, coin.Vout, coin.Unlocker)
	for i := 0; i < n; i++ {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: &lock})
	}
	in := coin.Tx.Outputs[coin.Vout].Satoshis
	if change := in - uint64(n)*sats - fee; change >= p.dust() {
		cs, err := p.signer.FundScript()
		if err != nil {
			pay.GiveBack()
			return err
		}
		// Each coin able to fund a whole transaction; fewer, down to one,
		// when the change is not that much.
		full := uint64(maxHoldsPerTx)*sats + p.feeAt(uint64(200+45*maxHoldsPerTx+34)) + p.dust()
		if k := int(change / full); k < parts {
			parts = max(k, 1)
		}
		each := change / uint64(parts)
		for i := 0; i < parts; i++ {
			v := each
			if i == parts-1 {
				v = change - each*uint64(parts-1)
			}
			tx.AddOutput(&transaction.TransactionOutput{Satoshis: v, LockingScript: cs})
		}
	}
	if err := tx.Sign(); err != nil {
		pay.GiveBack()
		return err
	}
	if err := capFee(p.fees(), tx); err != nil {
		pay.GiveBack()
		return err
	}
	txid := tx.TxID().String()
	raw := hex.EncodeToString(tx.Bytes())
	p.mu.Lock()
	for i := 0; i < n; i++ {
		p.st.Holds = append(p.st.Holds, HoldRecord{Txid: txid, Vout: uint32(i), Sats: sats, RawHex: raw, Sent: time.Now().Unix()})
	}
	err = p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		p.dropHolds(txid)
		pay.GiveBack()
		return err
	}
	pay.Async = true
	if _, _, err := pay.Settle(ctx, "hold outputs", tx); err != nil {
		if errors.Is(err, producer.ErrRefused) {
			// Never on the chain: the record goes and the coin comes back
			// (unless the refusal showed it spent elsewhere).
			p.dropHolds(txid)
			pay.GiveBack()
			return err
		}
		// It may be on its way: the record stays, the coin stays out, and
		// CheckHolds asks again.
		return fmt.Errorf("hold transaction %s: %w (asked about again later)", short(txid), err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.awaitHolds(pay, tx)
	}()
	return nil
}

// awaitHolds waits for a hold transaction the leg accepted to mine, then
// records its proof (the minimal path, as the hosts take it) and returns
// its change to the pool. One the node shows refused is dropped and its
// coin given back; one not mined within the Payer's wait is left to
// CheckHolds.
func (p *Publisher) awaitHolds(pay *producer.Payer, tx *transaction.Transaction) {
	txid := tx.TxID().String()
	mp, height, err := pay.Await(p.run, "hold outputs", tx)
	if err != nil {
		if errors.Is(err, producer.ErrRefused) {
			p.dropHolds(txid)
			pay.GiveBack()
		}
		if p.run.Err() == nil {
			p.cfg.Logf("hold transaction %s: %v", short(txid), feeError(err))
		}
		return
	}
	if tx.MerklePath != nil {
		mp = tx.MerklePath // Await attached the minimal path
	}
	pay.Change(tx, height, mp)
	p.markMined(txid, mp, height)
}

func (p *Publisher) dropHolds(txid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	keep := p.st.Holds[:0]
	for _, h := range p.st.Holds {
		if h.Txid != txid {
			keep = append(keep, h)
		}
	}
	p.st.Holds = keep
	_ = p.saveLocked()
}

func (p *Publisher) markMined(txid string, mp *transaction.MerklePath, height uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Holds {
		if p.st.Holds[i].Txid == txid {
			p.st.Holds[i].BumpHex, p.st.Holds[i].Height = funding.BumpHex(mp), height
		}
	}
	_ = p.saveLocked()
}

// CheckHolds asks the node about every hold transaction handed to the leg
// and not seen mined: its proof once it has one, kept as the minimal path
// (a node's proof may flag the sibling leaf as a transaction id, which the
// hosts refuse); the same bytes sent again when the node does not know it.
func (p *Publisher) CheckHolds(ctx context.Context) {
	p.mu.Lock()
	seen := map[string]string{}
	for _, h := range p.st.Holds {
		if h.BumpHex == "" && time.Since(time.Unix(h.Sent, 0)) > time.Minute {
			seen[h.Txid] = h.RawHex
		}
	}
	p.mu.Unlock()
	for txid, raw := range seen {
		if mp, height, err := p.cfg.Chain.Proof(ctx, txid); err == nil {
			if id, err := chainhash.NewHashFromHex(txid); err == nil && !chaintoken.MinimalPath(mp, *id) {
				if m, err := chaintoken.Minimal(mp, *id); err == nil {
					mp = m
				}
			}
			p.markMined(txid, mp, height)
			if tx, err := funding.Rebuild(raw, funding.BumpHex(mp), ""); err == nil {
				(&producer.Payer{Pool: p.wallet.Pool, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer}, Kept: p.kept}).Change(tx, height, mp)
			}
			continue
		}
		if p.cfg.Known != nil {
			if known, err := p.cfg.Known.Known(ctx, txid); err == nil && known {
				continue // known, not mined yet
			}
		}
		if b, err := hex.DecodeString(raw); err == nil {
			if tx, err := transaction.NewTransactionFromBytes(b); err == nil {
				if err := p.cfg.Settler.Submit(ctx, tx); err != nil {
					p.cfg.Logf("hold transaction %s: sent again: %v", short(txid), err)
				}
			}
		}
	}
}
