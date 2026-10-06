package publish

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/purse"
)

// A payment the buyer's own pool could not make is the buyer's state, never
// read as the answerer's silence: the SDK wraps the purse's error, and the
// pool's ErrNoSpendable survives the wrapping.
func TestPayErr(t *testing.T) {
	noCoin := fmt.Errorf("failed to create payment transaction: %w", &producer.NoCoinError{Err: bwallet.ErrNoSpendable, Held: 1})
	if err := payErr(noCoin); !errors.Is(err, ErrCannotPay) {
		t.Errorf("no coin: %v", err)
	}
	over := fmt.Errorf("failed to create payment transaction: %w", fmt.Errorf("%w: 900 satoshis asked, at most 10 allowed", purse.ErrOverMaxPay))
	if err := payErr(over); !errors.Is(err, ErrOverBudget) {
		t.Errorf("over budget: %v", err)
	}
	other := errors.New("connection reset")
	if err := payErr(other); err != other {
		t.Errorf("other: %v", err)
	}
}
