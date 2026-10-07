package publish

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/guard"
)

// A transaction recorded before it is sent may never mine: one of its
// inputs was spent by another transaction. That other transaction is
// either the same one as a miner re-encoded it (spec section 13.3: the
// unlocking scripts change, the outpoints spent and the outputs do not),
// which is adopted as what was sent, or a different one, which strands the
// record: it is abandoned, and the work is built again on what is left.

// Conflict is an input of a recorded transaction spent elsewhere.
type Conflict struct {
	Input int
	By    *transaction.Transaction
	// Same is set when By is the recorded transaction re-encoded.
	Same bool
}

// SpentElsewhere asks the node about each input of tx: the first one
// spent by another transaction, or nil when none is (as far as positive
// evidence goes). An input the node cannot say about is an error.
func (p *Publisher) SpentElsewhere(ctx context.Context, tx *transaction.Transaction) (*Conflict, error) {
	txid := tx.TxID().String()
	for i, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		free, by, err := p.unspent(ctx, in.SourceTXID.String(), in.SourceTxOutIndex)
		if err != nil {
			return nil, err
		}
		if free || by == txid {
			continue
		}
		raw, err := p.cfg.Chain.TxRaw(ctx, by)
		if err != nil {
			return nil, fmt.Errorf("%w: the transaction %s that spends an input: %v", ErrNotYet, short(by), err)
		}
		other, err := guard.ParseTransaction(raw, len(raw))
		if err != nil || other.TxID().String() != by {
			return nil, fmt.Errorf("%w: the node answered other bytes for %s", ErrNotYet, short(by))
		}
		return &Conflict{Input: i, By: other, Same: SameEffect(tx, other)}, nil
	}
	return nil, nil
}

// SameEffect reports whether b spends exactly a's outpoints, in a's order,
// and creates exactly a's outputs: a re-encoding of a.
func SameEffect(a, b *transaction.Transaction) bool {
	if len(a.Inputs) != len(b.Inputs) || len(a.Outputs) != len(b.Outputs) {
		return false
	}
	for i := range a.Inputs {
		x, y := a.Inputs[i], b.Inputs[i]
		if x.SourceTXID == nil || y.SourceTXID == nil || *x.SourceTXID != *y.SourceTXID || x.SourceTxOutIndex != y.SourceTxOutIndex {
			return false
		}
	}
	for i := range a.Outputs {
		x, y := a.Outputs[i], b.Outputs[i]
		if x.Satoshis != y.Satoshis || x.LockingScript == nil || y.LockingScript == nil ||
			hex.EncodeToString(*x.LockingScript) != hex.EncodeToString(*y.LockingScript) {
			return false
		}
	}
	return true
}

// BEEFSubject is the last transaction of a BEEF someone else sent, its
// shape held to bcommon guard's walk first (at most bound bytes), parsed
// under a recover: hostile bytes are an error, never a crash.
func BEEFSubject(b []byte, bound int) (tx *transaction.Transaction, err error) {
	if err := guard.CheckBEEF(b, bound); err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			tx, err = nil, fmt.Errorf("beef parse panicked: %v", r)
		}
	}()
	return transaction.NewTransactionFromBEEF(b)
}
