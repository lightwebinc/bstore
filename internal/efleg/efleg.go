// Package efleg hands a settlement leg every transaction with its inputs'
// source transactions attached. A leg that takes Extended Format (BRC-30),
// arcade and a fabric ingress, needs each input's source; a transaction
// rebuilt from its recorded bytes (a token, a hold transaction or a
// settlement sent again, an advertisement resent after a restart) has none,
// and such a leg refuses it (arcade: 460, not extended format). Each missing
// source is read from the node by its txid and held to it.
package efleg

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/publish"
)

// Settler is a leg whose transactions get their inputs' sources first.
type Settler struct {
	publish.Settler
	Asset *nodeapi.Asset
}

// Wrap is s with sources read from asset; s itself when either is nil.
func Wrap(s publish.Settler, asset *nodeapi.Asset) publish.Settler {
	if s == nil || asset == nil {
		return s
	}
	return &Settler{Settler: s, Asset: asset}
}

// Submit sends a copy of tx whose every input has its source transaction:
// the caller's own when it has one, else the node's, held to the txid. An
// input whose source the node does not answer is sent as it is, and the
// leg decides.
func (s *Settler) Submit(ctx context.Context, tx *transaction.Transaction) error {
	if tx == nil {
		return s.Settler.Submit(ctx, tx)
	}
	return s.Settler.Submit(ctx, Hydrate(ctx, s.Asset, tx))
}

// Hydrate is tx, or a shallow copy of it with the missing input sources
// read from asset. tx itself is never changed.
func Hydrate(ctx context.Context, asset *nodeapi.Asset, tx *transaction.Transaction) *transaction.Transaction {
	missing := false
	for _, in := range tx.Inputs {
		if in != nil && in.SourceTransaction == nil && in.SourceTXID != nil {
			missing = true
		}
	}
	if !missing {
		return tx
	}
	cp := *tx
	cp.Inputs = make([]*transaction.TransactionInput, len(tx.Inputs))
	cache := map[string]*transaction.Transaction{}
	for i, in := range tx.Inputs {
		if in == nil {
			continue
		}
		c := *in
		if c.SourceTransaction == nil && c.SourceTXID != nil {
			id := c.SourceTXID.String()
			src, ok := cache[id]
			if !ok {
				if raw, err := asset.TxRaw(ctx, id); err == nil {
					if t, err := transaction.NewTransactionFromBytes(raw); err == nil && t.TxID().String() == id {
						src = t
					}
				}
				cache[id] = src
			}
			if src != nil && int(c.SourceTxOutIndex) < len(src.Outputs) {
				c.SourceTransaction = src
			}
		}
		cp.Inputs[i] = &c
	}
	return &cp
}
