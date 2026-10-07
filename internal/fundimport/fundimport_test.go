package fundimport_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/goldentest"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/fundimport"
	"github.com/lightwebinc/bstore/internal/publish"
)

// A payment from another wallet to the fund address is imported once, with
// its proof checked; a second import adds nothing; a transaction that pays
// the fund address nothing, one not mined, and one whose output was spent
// add nothing.
func TestImport(t *testing.T) {
	ctx := context.Background()
	chain := testchain.New(700)
	srv := httptest.NewServer(chain)
	defer srv.Close()
	asset := &nodeapi.Asset{Base: srv.URL}

	// The user's own wallet: a key holding a mature coin.
	key := goldentest.FixedKey()
	addr, err := script.NewAddressFromPublicKey(key.PubKey(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chain.Generate(testchain.Maturity+3, addr.AddressString); err != nil {
		t.Fatal(err)
	}
	var coins []*transaction.Transaction
	for _, id := range chain.Txids() {
		if _, h, _ := chain.Proof(id); h <= 703 {
			coins = append(coins, chain.Tx(id))
		}
	}
	if len(coins) < 3 {
		t.Fatalf("%d mature coins", len(coins))
	}

	home := t.TempDir()
	if _, err := publish.Init(home); err != nil {
		t.Fatal(err)
	}
	w, err := bwallet.Open(publish.WalletDir(home), publish.Profile)
	if err != nil {
		t.Fatal(err)
	}
	s := w.Signer()
	fund, err := s.FundScript()
	if err != nil {
		t.Fatal(err)
	}
	pay := func(coin *transaction.Transaction, to *script.Script) *transaction.Transaction {
		unlock, _ := p2pkh.Unlock(key, nil)
		tx := transaction.NewTransaction()
		tx.AddInputFromTx(coin, 0, unlock)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 20_000, LockingScript: to})
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: coin.Outputs[0].Satoshis - 20_500, LockingScript: coin.Outputs[0].LockingScript})
		if err := tx.Sign(); err != nil {
			t.Fatal(err)
		}
		if err := chain.Send(tx); err != nil {
			t.Fatal(err)
		}
		return tx
	}

	// Not mined yet.
	chain.SetHold(true)
	tx := pay(coins[0], fund)
	if _, err := fundimport.Import(ctx, s, w.Pool, asset, chain, tx.TxID().String()); !errors.Is(err, fundimport.ErrNotMined) {
		t.Fatalf("unmined: %v", err)
	}
	chain.Mine()
	res, err := fundimport.Import(ctx, s, w.Pool, asset, chain, tx.TxID().String())
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 1 || res.Satoshis != 20_000 || w.Pool.Balance() != 20_000 {
		t.Fatalf("import: %+v, pool %d", res, w.Pool.Balance())
	}
	if o := w.Pool.Outputs()[0]; o.Coinbase || o.Raw == "" || o.Bump == "" || !o.Spendable(chain.Height()) {
		t.Fatalf("imported output %+v", o)
	}
	// Again: held already.
	if res, err = fundimport.Import(ctx, s, w.Pool, asset, chain, tx.TxID().String()); err != nil || res.Added != 0 || res.Held != 1 {
		t.Fatalf("again: %+v %v", res, err)
	}

	// Pays someone else.
	other := pay(coins[1], coins[1].Outputs[0].LockingScript)
	chain.Mine()
	if _, err := fundimport.Import(ctx, s, w.Pool, asset, chain, other.TxID().String()); err == nil {
		t.Fatal("a transaction paying nothing to the fund address was imported")
	}

	// Spent before it is imported (the pool lost it): left out.
	spent := pay(coins[2], fund)
	chain.Mine()
	chain.SpendElsewhere(spent.TxID().String(), 0, "ab"+spent.TxID().String()[2:])
	if res, err = fundimport.Import(ctx, s, w.Pool, asset, chain, spent.TxID().String()); err != nil || res.Added != 0 || res.Spent != 1 {
		t.Fatalf("spent: %+v %v", res, err)
	}

	if _, err := fundimport.Import(ctx, s, w.Pool, asset, chain, "nope"); err == nil {
		t.Fatal("a bad txid was taken")
	}
}
