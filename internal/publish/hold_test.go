package publish

import (
	"context"
	"encoding/hex"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	bpub "github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/testchain"
)

// A fill is not one transaction a block: with one coin in the pool and 40
// owed, the first transaction mints 16 and leaves its change in a coin for
// each of the two still owed; once it mines, one fill mints both at once,
// and the pool is at its depth after two blocks, not three.
func TestFillHoldsMintsMoreThanOneTransactionABlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	chain := testchain.New(700)
	chain.Hold = true
	cs := httptest.NewServer(chain)
	defer cs.Close()
	dir := t.TempDir()
	if _, err := Init(filepath.Join(dir, "id")); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	host := newTarget(t)
	p, err := Open(ctx, Config{Dir: filepath.Join(dir, "id"), Chain: nodeChain(cs.URL), Tip: nodeTip(cs.URL),
		Settler: &bpub.RPCSettler{RPC: &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}}, Fees: mint.DefaultFees,
		PieceTree: 8, EntryTree: 4, HoldAhead: 40, Poll: 20 * time.Millisecond, Store: store,
		Delivery: &Delivery{Hosts: []string{host.srv.URL}}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	w := p.Wallet()
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		t.Fatal(err)
	}
	// One coin in the pool.
	outs := w.Pool.Outputs()
	for _, o := range outs[1:] {
		if _, err := w.Pool.Remove(o); err != nil {
			t.Fatal(err)
		}
	}
	fundScript, err := w.Signer().FundScript()
	if err != nil {
		t.Fatal(err)
	}

	p.FillHolds(ctx, 1000)
	holds := p.Holds()
	if len(holds) != maxHoldsPerTx {
		t.Fatalf("one coin: %d hold outputs on their way, want %d", len(holds), maxHoldsPerTx)
	}
	raw, _ := hex.DecodeString(holds[0].RawHex)
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	change := 0
	for _, o := range tx.Outputs {
		if o.LockingScript.String() == fundScript.String() {
			change++
		}
	}
	if change != 2 {
		t.Fatalf("the first transaction leaves %d change coins, want one for each of the 2 transactions still owed", change)
	}
	if chain.Mine() != 1 {
		t.Fatal("the hold transaction did not reach the chain")
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		mined, coins := 0, 0
		for _, h := range p.Holds() {
			if h.BumpHex != "" {
				mined++
			}
		}
		for _, o := range w.Pool.Outputs() {
			if !o.Unproven {
				coins++
			}
		}
		if mined == maxHoldsPerTx && coins == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the block: %d hold outputs mined, %d coins proven; want 16 and 2", mined, coins)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The next fill mints the two transactions still owed in one block.
	p.FillHolds(ctx, 1000)
	txids := map[string]bool{}
	for _, h := range p.Holds() {
		txids[h.Txid] = true
	}
	if n := len(p.Holds()); n != 40 || len(txids) != 3 {
		t.Fatalf("after the second fill: %d hold outputs in %d transactions, want 40 in 3", n, len(txids))
	}
	if chain.Mine() != 2 {
		t.Fatal("the two hold transactions did not reach the chain together")
	}
}
