package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// fakeChain mines each transaction alone in a block of two beside a fixed
// sibling, at successive heights, and is the chain tracker of those blocks.
type fakeChain struct {
	height uint32
	roots  map[uint32]chainhash.Hash
}

func (c *fakeChain) mine(tx *transaction.Transaction) {
	c.height++
	t := true
	sib := chainhash.Hash(bytes.Repeat([]byte{0x33}, 32))
	tx.MerklePath = transaction.NewMerklePath(c.height, [][]*transaction.PathElement{{
		{Offset: 0, Hash: &sib},
		{Offset: 1, Hash: tx.TxID(), Txid: &t},
	}})
	r, err := tx.MerklePath.ComputeRoot(tx.TxID())
	if err != nil {
		panic(err)
	}
	c.roots[c.height] = *r
}

func (c *fakeChain) IsValidRootForHeight(_ context.Context, r *chainhash.Hash, h uint32) (bool, error) {
	want, ok := c.roots[h]
	return ok && r != nil && *r == want, nil
}

func (c *fakeChain) CurrentHeight(context.Context) (uint32, error) { return c.height, nil }

// TestCorpusAdmits plans, mines on a fake chain, builds a set of every
// shape and admits a sample of each with storerec.Admit and SPV, as the
// hosts and the bridge would.
func TestCorpusAdmits(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	ch := &fakeChain{height: 100, roots: map[uint32]chainhash.Hash{}}

	key, _ := ec.NewPrivateKey()
	if err := writeJSON(filepath.Join(root, "wallet.json"), map[string]any{"wif": key.Wif()}); err != nil {
		t.Fatal(err)
	}
	fk, err := loadFeeKey(root)
	if err != nil {
		t.Fatal(err)
	}
	fund := transaction.NewTransaction()
	sib := chainhash.Hash(bytes.Repeat([]byte{0x11}, 32))
	fund.AddInput(&transaction.TransactionInput{SourceTXID: &sib, UnlockingScript: &script.Script{}, SequenceNumber: 0xffffffff})
	type out struct {
		Vout     uint32 `json:"vout"`
		Satoshis uint64 `json:"satoshis"`
	}
	var outs []out
	for i := range 8 {
		fund.AddOutput(&transaction.TransactionOutput{Satoshis: 1_000_000, LockingScript: fk.Lock})
		outs = append(outs, out{uint32(i), 1_000_000})
	}
	ch.mine(fund)
	if err := writeJSON(filepath.Join(root, "funding.json"), map[string]any{"txid": fund.TxID().String(), "raw": hex.EncodeToString(fund.Bytes()),
		"bump": hex.EncodeToString(fund.MerklePath.Bytes()), "height": ch.height, "address": fk.Addr.AddressString, "outputs": outs}); err != nil {
		t.Fatal(err)
	}
	if err := cmdInit([]string{"-root", root}); err != nil {
		t.Fatal(err)
	}
	home, _ := ec.NewPrivateKey()
	homeAddr, _ := script.NewAddressFromPublicKey(home.PubKey(), false)
	if err := cmdPlan(ctx, []string{"-root", root, "-entry-trees", "4", "-outcome-trees", "2", "-front-trees", "2", "-adverts", "30",
		"-holds", "30", "-mix-puts", "230", "-anchor-every", "100", "-fan-max", "1000", "-home", homeAddr.AddressString, "-home-coins", "3"}); err != nil {
		t.Fatal(err)
	}
	// Mine every planned transaction.
	var p planFile
	if err := readJSON(filepath.Join(root, "chain", "plan.json"), &p); err != nil {
		t.Fatal(err)
	}
	st, err := openStore(root)
	if err != nil {
		t.Fatal(err)
	}
	proofs := map[string]*transaction.MerklePath{}
	for _, lv := range p.Levels {
		for _, id := range lv {
			tx, err := st.tx(id)
			if err != nil {
				t.Fatal(err)
			}
			ch.mine(tx)
			proofs[id] = tx.MerklePath
		}
	}
	if err := st.addProofs(proofs); err != nil {
		t.Fatal(err)
	}
	shapes := "put4k,put1m,delete,outcome,front,advert,hold,mix"
	if !testing.Short() {
		shapes += ",put256m"
	}
	if err := cmdBuild(ctx, []string{"-root", root, "-name", "t1", "-count", "40", "-shapes", shapes}); err != nil {
		t.Fatal(err)
	}
	set := filepath.Join(root, "sets", "t1")
	var m manifest
	if err := readJSON(filepath.Join(set, "manifest.json"), &m); err != nil {
		t.Fatal(err)
	}
	if err := checkSet(ctx, set, &m, 1000, ch); err != nil {
		t.Fatal(err)
	}
	if got := m.Shapes["mix"].Count; got != len(p.Mix) || got < 1100 {
		t.Fatalf("mix holds %d objects, plan %d", got, len(p.Mix))
	}
}
