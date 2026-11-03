package publish

import (
	"context"
	"errors"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
)

// Small coins are merged into one. Change, refunds of lapsed bounties and
// payments each arrive as a coin of their own, and the pool is one file
// rewritten whole on every change: the lab gateway held 9,492 refunds of
// 22,400 satoshis after its races lapsed for hours (2026-11-03), a 19.7 MB
// pool, and wrote about 35 MB a minute standing still. Once more than
// mergeAbove proven coins are under mergeBelow satoshis, a housekeeping pass
// merges up to mergeBatch of them, smallest first, into one coin paying the
// fund key. A coin under mergeBelow funds no hold transaction (a refill of
// 1,024 holds at the lab bounty takes one input of 23M satoshis), so the
// merge takes nothing a refill could use. Variables for the tests.
var (
	mergeAbove        = 64
	mergeBelow uint64 = 1_000_000
	mergeBatch        = 250
)

// MergeCoins merges the identity's small coins, mergeBatch at most, when
// more than mergeAbove are held. It answers how many coins went into the
// merge: zero when there was nothing to do. The merged coin is in the pool
// unproven until its proof is collected (CollectChange).
func (p *Publisher) MergeCoins(ctx context.Context) (int, error) {
	pay := p.newPayer(ctx)
	small := 0
	for _, o := range p.wallet.Pool.Outputs() {
		if o.Spendable(pay.Tip) && o.Satoshis > 0 && o.Satoshis < mergeBelow {
			small++
		}
	}
	if small <= mergeAbove {
		return 0, nil
	}
	ins, err := pay.TakeSmall(ctx, mergeBelow, mergeBatch)
	if errors.Is(err, bwallet.ErrNoSpendable) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	fs, err := p.signer.FundScript()
	if err != nil {
		pay.GiveBack()
		return 0, err
	}
	tx := transaction.NewTransaction()
	var in uint64
	for _, c := range ins {
		tx.AddInputFromTx(c.Tx, c.Vout, c.Unlocker)
		in += c.Tx.Outputs[c.Vout].Satoshis
	}
	fee := p.feeAt(uint64(10 + 148*len(ins) + 34))
	if in < fee+p.dust() {
		pay.GiveBack()
		return 0, nil
	}
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: in - fee, LockingScript: fs})
	if err := tx.Sign(); err != nil {
		pay.GiveBack()
		return 0, err
	}
	if err := capFee(p.fees(), tx); err != nil {
		pay.GiveBack()
		return 0, err
	}
	// Not waited for, as a split is not: the merged coin is in the pool
	// unproven once the leg takes the merge, spendable once mined.
	pay.Async = true
	if _, _, err := pay.Settle(ctx, "coin merge", tx); err != nil {
		pay.GiveBack()
		return 0, err
	}
	pay.Change(tx, 0, nil)
	p.cfg.Logf("coin merge: %d coins of under %d satoshis into one of %d (%s); spendable once mined",
		len(ins), mergeBelow, in-fee, short(tx.TxID().String()))
	return len(ins), nil
}
