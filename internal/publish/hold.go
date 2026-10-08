package publish

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/producer"

	"github.com/lightwebinc/bstore/storerec"
)

// Hold outputs (spec section 9.2) are minted ahead, up to 16 in one
// transaction, and kept until a put takes one: a writer SHOULD mine its
// hold output well before it publishes the entry (spec section 9.4), and a
// put should not wait for a block. Each is recorded before its transaction
// reaches the settlement leg; one that was handed to the leg and not seen
// mined is asked about again (Holds), and sent again when the node does
// not know it.

// ErrNoHold is a put that asked for a bounty when no mined hold output of
// that value was ready within HoldWait; minting more is under way.
var ErrNoHold = errors.New("publish: no mined hold output is ready for the bounty yet")

const maxHoldsPerTx = 16

// ready is the hold outputs of sats that are mined and not taken; the
// caller holds p.mu.
func (p *Publisher) readyLocked(sats uint64) int {
	n := 0
	for _, h := range p.st.Holds {
		if !h.Used && h.Sats == sats && h.BumpHex != "" {
			n++
		}
	}
	return n
}

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

// FillHolds mints hold outputs of sats until HoldAhead are ready (mined or
// on their way), at most 16 to a transaction, and waits for them to mine.
// One fill runs at a time; a call while one runs returns at once.
func (p *Publisher) FillHolds(ctx context.Context, sats uint64) {
	if !p.holdMu.TryLock() {
		return
	}
	defer p.holdMu.Unlock()
	p.mu.Lock()
	have := 0
	for _, h := range p.st.Holds {
		if !h.Used && h.Sats == sats {
			have++
		}
	}
	p.mu.Unlock()
	need := min(p.cfg.HoldAhead-have, maxHoldsPerTx)
	if need <= 0 {
		return
	}
	if err := p.mintHolds(ctx, sats, need); err != nil {
		p.cfg.Logf("hold outputs: %v", feeError(err))
	}
}

func (p *Publisher) mintHolds(ctx context.Context, sats uint64, n int) error {
	key, err := storerec.LockingKey(storerec.HoldDerivation, p.Identity())
	if err != nil {
		return err
	}
	lock := script.Script(storerec.HoldScript(key))
	// A Payer of its own: a fill runs beside puts, and a Payer's
	// reservations are its own.
	pay := &producer.Payer{Pool: p.wallet.Pool, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer},
		Kept: p.kept, Settler: p.cfg.Settler, Chain: p.cfg.Chain, Fees: p.fees(), Note: p.note, Poll: p.cfg.Poll}
	if h, err := p.bestHeader(ctx); err == nil {
		pay.Tip = h.Height
	}
	fee := p.feeAt(uint64(200 + 45*n))
	coin, err := pay.TakeAtLeast(ctx, uint64(n)*sats+fee+p.dust())
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
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: change, LockingScript: cs})
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
	mp, height, err := pay.SettleAndWait(ctx, "hold outputs", tx)
	if err != nil {
		if errors.Is(err, producer.ErrRefused) {
			// Never on the chain: the record goes and the coin comes back
			// (unless the refusal showed it spent elsewhere).
			p.dropHolds(txid)
			pay.GiveBack()
			return err
		}
		// It may be on its way: the record stays, the coin stays out, and
		// Holds asks again.
		return fmt.Errorf("hold transaction %s: %w (asked about again later)", short(txid), err)
	}
	tx.MerklePath = mp
	pay.Change(tx, height, mp)
	p.markMined(txid, mp, height)
	return nil
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
// and not seen mined: its proof once it has one; the same bytes sent
// again when the node does not know it.
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
