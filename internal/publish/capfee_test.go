package publish

import (
	"bytes"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/mint"
)

// A transaction built here is held to fee.max_tx as package mint holds its
// own: the fee For asks at the signed size, and the fee paid, change
// dropped as dust included.
func TestCapFee(t *testing.T) {
	lock := script.Script(bytes.Repeat([]byte{0x51}, 25))
	src := transaction.NewTransaction()
	src.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &lock})
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(src, 0, nil)
	unlock := script.Script(bytes.Repeat([]byte{0x51}, 107))
	tx.Inputs[0].UnlockingScript = &unlock
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 700, LockingScript: &lock}) // pays 300
	fees := mint.DefaultFees                                                          // floor 250
	for _, c := range []struct {
		max  uint64
		fail bool
	}{{0, false}, {300, false}, {1000, false}, {299, true}, {100, true}} {
		fees.Max = c.max
		err := capFee(fees, tx)
		if c.fail != errors.Is(err, mint.ErrFeeTooHigh) || (!c.fail && err != nil) {
			t.Errorf("max %d: %v", c.max, err)
		}
	}
}
