package chaincfg_test

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
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/chaincfg"
	"github.com/lightwebinc/bstore/internal/publish"
)

// A payment from another wallet to the fund address is imported once, by
// txid or as BEEF, with its proof checked; a second import adds nothing; a
// transaction that pays the fund address nothing, one not mined (by txid),
// and one whose output was spent add nothing. An unmined payment as BEEF
// is taken held back, unless refused by configuration.
func TestFund(t *testing.T) {
	ctx := context.Background()
	chain := testchain.New(700)
	srv := httptest.NewServer(chain)
	defer srv.Close()
	v, err := chaincfg.Open(chaincfg.Spec{Network: "regtest", Chain: "asset:" + srv.URL, Settle: "rpc:" + srv.URL + "/rpc", Headers: srv.URL})
	if err != nil {
		t.Fatal(err)
	}

	// The user's own wallet: a key holding a mature coin.
	key := goldentest.FixedKey()
	addr, err := script.NewAddressFromPublicKey(key.PubKey(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chain.Generate(testchain.Maturity+5, addr.AddressString); err != nil {
		t.Fatal(err)
	}
	var coins []*transaction.Transaction
	for _, id := range chain.Txids() {
		if _, h, _ := chain.Proof(id); h <= 705 {
			coins = append(coins, chain.Tx(id))
		}
	}
	if len(coins) < 5 {
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
	build := func(coin *transaction.Transaction, to *script.Script) *transaction.Transaction {
		unlock, _ := p2pkh.Unlock(key, nil)
		tx := transaction.NewTransaction()
		tx.AddInputFromTx(coin, 0, unlock)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 20_000, LockingScript: to})
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: coin.Outputs[0].Satoshis - 20_500, LockingScript: coin.Outputs[0].LockingScript})
		if err := tx.Sign(); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	pay := func(coin *transaction.Transaction, to *script.Script) *transaction.Transaction {
		tx := build(coin, to)
		if err := chain.Send(tx); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	beefOf := func(tx *transaction.Transaction) []byte {
		for _, in := range tx.Inputs {
			mp, _, _ := chain.Proof(in.SourceTransaction.TxID().String())
			in.SourceTransaction.MerklePath = mp
		}
		if mp, _, ok := chain.Proof(tx.TxID().String()); ok {
			tx.MerklePath = mp
		}
		b, err := tx.BEEF()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	// Not mined yet.
	chain.SetHold(true)
	tx := pay(coins[0], fund)
	if _, err := v.FundTxid(ctx, s, w.Pool, tx.TxID().String()); !errors.Is(err, chaincfg.ErrUnmined) {
		t.Fatalf("unmined: %v", err)
	}
	chain.Mine()
	res, err := v.FundTxid(ctx, s, w.Pool, tx.TxID().String())
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 1 || res.Satoshis != 20_000 || !res.Mined || w.Pool.Balance() != 20_000 {
		t.Fatalf("import: %+v, pool %d", res, w.Pool.Balance())
	}
	if o := w.Pool.Outputs()[0]; o.Coinbase || o.Raw == "" || o.Bump == "" || !o.Spendable(chain.Height()) {
		t.Fatalf("imported output %+v", o)
	}
	// Again, by txid and as BEEF: held already.
	if res, err = v.FundTxid(ctx, s, w.Pool, tx.TxID().String()); err != nil || res.Added != 0 || res.Held != 1 {
		t.Fatalf("again: %+v %v", res, err)
	}
	if res, err = v.FundBEEF(ctx, s, w.Pool, beefOf(tx), false); err != nil || res.Added != 0 || res.Held != 1 {
		t.Fatalf("again as BEEF: %+v %v", res, err)
	}

	// Pays someone else.
	other := pay(coins[1], coins[1].Outputs[0].LockingScript)
	chain.Mine()
	if _, err := v.FundTxid(ctx, s, w.Pool, other.TxID().String()); !errors.Is(err, bwallet.ErrPaysNothing) {
		t.Fatalf("a transaction paying nothing to the fund address: %v", err)
	}

	// Spent before it is imported (the pool lost it): left out.
	spent := pay(coins[2], fund)
	chain.Mine()
	chain.SpendElsewhere(spent.TxID().String(), 0, "ab"+spent.TxID().String()[2:])
	if res, err = v.FundTxid(ctx, s, w.Pool, spent.TxID().String()); err != nil || res.Added != 0 || res.Spent != 1 {
		t.Fatalf("spent: %+v %v", res, err)
	}

	// A mined payment as BEEF: no lookup of the payment itself.
	mined := pay(coins[3], fund)
	chain.Mine()
	if res, err = v.FundBEEF(ctx, s, w.Pool, beefOf(mined), false); err != nil || res.Added != 1 || !res.Mined {
		t.Fatalf("mined BEEF: %+v %v", res, err)
	}

	// An unmined payment as BEEF, its parent proven: refused when the
	// configuration says so; else taken held back and sent to the leg.
	chain.SetHold(true)
	unmined := build(coins[4], fund)
	if _, err := v.FundBEEF(ctx, s, w.Pool, beefOf(unmined), true); !errors.Is(err, bwallet.ErrUnmined) {
		t.Fatalf("unmined BEEF refused: %v", err)
	}
	res, err = v.FundBEEF(ctx, s, w.Pool, beefOf(unmined), false)
	if err != nil || res.Added != 1 || res.Mined {
		t.Fatalf("unmined BEEF: %+v %v", res, err)
	}
	if got := w.Pool.UnprovenTxids(); len(got) != 1 || got[0] != unmined.TxID().String() {
		t.Fatalf("held back: %v", got)
	}
	if chain.Waiting() != 1 {
		t.Fatalf("the unmined payment was not sent to the leg: %d waiting", chain.Waiting())
	}

	if _, err := v.FundTxid(ctx, s, w.Pool, "nope"); err == nil {
		t.Fatal("a bad txid was taken")
	}
}
