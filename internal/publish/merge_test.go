package publish

import (
	"testing"
)

// Past mergeAbove small coins, a pass merges mergeBatch of them, smallest
// first, into one coin paying the fund key: the pool holds one coin in their
// place, unproven until mined, and the balance falls by the fee alone. At or
// under mergeAbove it does nothing.
func TestMergeCoinsMergesSmallCoins(t *testing.T) {
	r := newRaceRig(t)
	defer func(a int, b uint64, n int) { mergeAbove, mergeBelow, mergeBatch = a, b, n }(mergeAbove, mergeBelow, mergeBatch)
	pool := r.p.wallet.Pool
	count, balance := pool.Count(), pool.Balance()
	// Only mature coinbase counts: the rig's younger coins are not spendable.
	tip, err := r.p.bestHeader(r.ctx)
	if err != nil {
		t.Fatal(err)
	}
	ready := 0
	for _, o := range pool.Outputs() {
		if o.Spendable(tip.Height) {
			ready++
		}
	}
	if ready < 4 {
		t.Fatalf("the rig holds %d spendable coins, want at least 4", ready)
	}
	mergeBelow, mergeBatch = 1<<62, 3

	mergeAbove = ready
	if n, err := r.p.MergeCoins(r.ctx); err != nil || n != 0 {
		t.Fatalf("at mergeAbove: merged %d, %v; want nothing", n, err)
	}

	mergeAbove = 2
	n, err := r.p.MergeCoins(r.ctx)
	if err != nil || n != 3 {
		t.Fatalf("merged %d, %v; want 3", n, err)
	}
	if got := pool.Count(); got != count-2 {
		t.Fatalf("pool holds %d coins after the merge, want %d", got, count-2)
	}
	unproven := pool.UnprovenTxids()
	if len(unproven) != 1 {
		t.Fatalf("unproven %v, want the merge alone", unproven)
	}
	fee := balance - pool.Balance()
	if fee == 0 || fee > 10_000 {
		t.Fatalf("the merge cost %d satoshis, want a fee and nothing more", fee)
	}
	r.blocks(1)
	r.p.CollectChange(r.ctx)
	if left := pool.UnprovenTxids(); len(left) != 0 {
		t.Fatalf("still unproven after a block: %v", left)
	}
}
