package main

import (
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/storerec"
)

// A renewal's answer is taken only when it spends the advertisement it
// renews and an output advertises the same root by the same provider
// through exactly the expiry asked for.
func TestCheckSuccessor(t *testing.T) {
	host, _ := ec.NewPrivateKey()
	adKey, err := wallet.NewKeyDeriver(host).DerivePrivateKey(storerec.AdProtocol, storerec.AdKeyID, pushdrop.Anyone())
	if err != nil {
		t.Fatal(err)
	}
	hostKey := host.PubKey().Compressed()
	var root [32]byte
	root[0] = 7
	url := "https://provider.example.com/store" + storerec.CHIRPURLSuffix(root)
	prev := &transaction.Transaction{}
	prev.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: &script.Script{0x51}})
	ad := func(expiry uint64, spend *transaction.Transaction) *transaction.Transaction {
		fields, err := storerec.AdvertisementFields(hostKey, adKey, root, url, expiry, 1234)
		if err != nil {
			t.Fatal(err)
		}
		s := script.Script(storerec.AdvertisementScript(adKey.PubKey(), fields))
		tx := &transaction.Transaction{}
		tx.AddInputFromTx(spend, 0, nil)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: &s})
		return tx
	}
	other := &transaction.Transaction{LockTime: 9}
	other.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: &script.Script{0x51}})
	if _, err := storerec.DecodeAdvertisement(*ad(2000, prev).Outputs[0].LockingScript); err != nil {
		t.Fatalf("the test's advertisement does not decode: %v", err)
	}
	if v, err := checkSuccessor(ad(2000, prev), hostKey, root, 2000, prev.TxID().String()); err != nil || v != 0 {
		t.Fatalf("a good successor: %d %v", v, err)
	}
	if _, err := checkSuccessor(ad(2000, other), hostKey, root, 2000, prev.TxID().String()); err == nil {
		t.Error("a successor that does not spend the advertisement was taken")
	}
	if _, err := checkSuccessor(ad(1999, prev), hostKey, root, 2000, prev.TxID().String()); err == nil {
		t.Error("a successor with another expiry was taken")
	}
	stranger, _ := ec.NewPrivateKey()
	if _, err := checkSuccessor(ad(2000, prev), stranger.PubKey().Compressed(), root, 2000, prev.TxID().String()); err == nil {
		t.Error("another provider's advertisement was taken")
	}
	var root2 [32]byte
	if _, err := checkSuccessor(ad(2000, prev), hostKey, root2, 2000, prev.TxID().String()); err == nil {
		t.Error("another root's advertisement was taken")
	}
}
