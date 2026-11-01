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
// owed, the first cycle mints 16 and leaves its change in a coin for each
// of the two transactions still owed and one to spare; once it mines, the
// next cycle mints both at once (the spare stays for a put's trees and
// fees), and the pool is at its depth after two blocks, not three. The
// fill is one call: it waits for each cycle to mine and goes again.
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
	holdTxids := func() (int, map[string]bool) {
		ids := map[string]bool{}
		hs := p.Holds()
		for _, h := range hs {
			ids[h.Txid] = true
		}
		return len(hs), ids
	}
	until := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	done := make(chan struct{})
	go func() {
		p.FillHolds(ctx, 1000)
		close(done)
	}()
	// The first cycle: one coin, so one transaction of 16, with a change
	// coin for each of the two transactions still owed.
	until("the first transaction", func() bool { n, _ := holdTxids(); return n == maxHoldsPerTx })
	raw, _ := hex.DecodeString(p.Holds()[0].RawHex)
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
	if change != 3 {
		t.Fatalf("the first transaction leaves %d change coins, want one for each of the 2 transactions still owed and one to spare", change)
	}
	time.Sleep(100 * time.Millisecond)
	if n, _ := holdTxids(); n != maxHoldsPerTx {
		t.Fatalf("%d hold outputs before the block: the cycle minted beyond its coins", n)
	}
	if chain.Mine() != 1 {
		t.Fatal("the hold transaction did not reach the chain")
	}
	// The second cycle: two coins, two transactions at once, 40 in all.
	until("the second cycle", func() bool { n, ids := holdTxids(); return n == 40 && len(ids) == 3 })
	select {
	case <-done:
		t.Fatal("the fill returned before its last cycle mined")
	default:
	}
	if chain.Mine() != 2 {
		t.Fatal("the two hold transactions did not reach the chain together")
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the fill did not return once the pool was at its depth")
	}
	mined := 0
	for _, h := range p.Holds() {
		if h.BumpHex != "" {
			mined++
		}
	}
	if mined != 40 {
		t.Fatalf("%d of 40 hold outputs recorded mined", mined)
	}
}
