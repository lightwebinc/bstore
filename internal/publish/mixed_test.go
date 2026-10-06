package publish

import (
	"errors"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/storerec"
)

// A sweep or reclaim that would also create a hold output or an
// advertisement retracts nothing (spec section 12.4): the tooling refuses
// to build one.
func TestMixedSweepRefused(t *testing.T) {
	priv, pub := ec.PrivateKeyFromBytes(append(make([]byte, 31), 7))
	adKey, err := wallet.NewKeyDeriver(priv).DerivePrivateKey(storerec.AdProtocol, storerec.AdKeyID, pushdrop.Anyone())
	if err != nil {
		t.Fatal(err)
	}
	var root [32]byte
	root[0] = 1
	fields, err := storerec.AdvertisementFields(pub.Compressed(), adKey, root,
		"https://a.example/chirp/v1/x/objects/x", 1900000000, 100)
	if err != nil {
		t.Fatal(err)
	}
	p2pkh := script.Script(append([]byte{0x76, 0xa9, 0x14}, append(make([]byte, 20), 0x88, 0xac)...))
	tomb := script.Script(storerec.FundingScript(pub))
	hold := script.Script(storerec.HoldScript(pub))
	ad := script.Script(storerec.AdvertisementScript(adKey.PubKey(), fields))
	build := func(scripts ...script.Script) *transaction.Transaction {
		tx := transaction.NewTransaction()
		for i := range scripts {
			tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: &scripts[i]})
		}
		return tx
	}
	if err := refuseMixed(build(tomb, p2pkh)); err != nil {
		t.Fatalf("a plain sweep refused: %v", err)
	}
	if err := refuseMixed(build(p2pkh)); err != nil {
		t.Fatalf("a plain reclaim refused: %v", err)
	}
	for name, tx := range map[string]*transaction.Transaction{
		"hold":          build(tomb, p2pkh, hold),
		"advertisement": build(tomb, ad, p2pkh),
		"reclaim hold":  build(hold),
	} {
		if err := refuseMixed(tx); !errors.Is(err, ErrMixedSweep) {
			t.Fatalf("%s: got %v, want ErrMixedSweep", name, err)
		}
	}
}
