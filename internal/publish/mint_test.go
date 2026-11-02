package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	bpub "github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

// mintRig is a funded publisher with one 4 MiB object staged in its store
// and a mined piece tree output to spend: what one piece carrier is minted
// from, without a target to send it to.
type mintRig struct {
	ctx   context.Context
	p     *Publisher
	store *Store
	c     StoreClosure
	root  [32]byte
	blob  [32]byte // the one object of BlobSize
	tree  *transaction.Transaction
	vout  uint32
}

func newMintRig(tb testing.TB) *mintRig {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	tb.Cleanup(cancel)
	cs := httptest.NewServer(testchain.New(700))
	tb.Cleanup(cs.Close)
	dir := tb.TempDir()
	if _, err := Init(filepath.Join(dir, "id")); err != nil {
		tb.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		tb.Fatal(err)
	}
	p, err := Open(ctx, Config{Dir: filepath.Join(dir, "id"), Chain: nodeChain(cs.URL), Tip: nodeTip(cs.URL),
		Settler: &bpub.RPCSettler{RPC: &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}}, Fees: mint.DefaultFees,
		PieceTree: 8, EntryTree: 4, Store: store, Delivery: &Delivery{Providers: []string{"http://provider.invalid"}}, Logf: tb.Logf})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(p.Close)
	w := p.Wallet()
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		tb.Fatal(err)
	}
	content := make([]byte, storerec.BlobSize)
	_, _ = rand.Read(content)
	st, err := store.Stage(ctx, bytes.NewReader(content), 1<<30)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(st.Release)
	r := &mintRig{ctx: ctx, p: p, store: store, c: StoreClosure{Store: store, Manifest: st.Manifest, Staged: true}, root: st.Root, blob: sha256.Sum256(content)}
	if r.tree, r.vout, err = p.spend(ctx, kindPiece, 1); err != nil {
		tb.Fatal(err)
	}
	if r.tree.MerklePath == nil {
		tb.Fatal("the piece tree spent has no proof")
	}
	return r
}

// A piece carrier's BEEF built here is byte for byte the SDK's, for a 4 MiB
// piece and a small one, with the carrier last and its txid kept; a
// provider takes it. A carrier on a tree without a proof goes through the
// SDK and is placed the same way.
func TestPieceBEEFMatchesSDK(t *testing.T) {
	r := newMintRig(t)
	data, err := r.c.Object(r.ctx, r.blob)
	if err != nil || len(data) != storerec.BlobSize {
		t.Fatalf("the blob: %d bytes, %v", len(data), err)
	}
	for _, n := range []int{len(data), 1000} {
		pc := &storerec.Piece{Bucket: testBucket, Writer: r.p.Identity(), Root: r.root, Piece: data[:n]}
		rec, err := pc.Encode()
		if err != nil {
			t.Fatal(err)
		}
		tx, err := carrier.Mint(r.ctx, r.p.wallet, r.p.wallet.Originator, carrierParams(storerec.PieceDerivation), rec, r.tree, r.vout)
		if err != nil {
			t.Fatal(err)
		}
		want, err := tx.AtomicBEEF(false)
		if err != nil {
			t.Fatal(err)
		}
		wantID := *tx.TxID()
		got, at, err := pieceBEEF(tx, r.tree)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("piece of %d: the BEEF built here (%d bytes) is not the SDK's (%d bytes)", n, len(got), len(want))
		}
		if at != len(got)-tx.Size() || got[at-1] != byte(transaction.RawTx) || !bytes.Equal(got[at:], tx.Bytes()) {
			t.Fatalf("piece of %d: the carrier is not at %d of %d", n, at, len(got))
		}
		if *tx.TxID() != wantID || tx.TxID() != tx.TxID() {
			t.Fatalf("piece of %d: txid %s, want %s, cached %v", n, tx.TxID(), wantID, tx.TxID() == tx.TxID())
		}
		if _, err := storerec.CheckPieceCarrier(r.ctx, got, testBucket, nil); err != nil {
			t.Fatalf("piece of %d: a provider refuses the carrier: %v", n, err)
		}
	}
	// A tree not yet mined: the carrier's BEEF carries its ancestry.
	unmined := transaction.NewTransaction()
	unmined.AddInputFromTx(r.tree, r.vout, storerec.PieceDerivation.Unlocker(r.ctx, r.p.wallet, r.p.wallet.Originator))
	lock, err := carrier.FundingLock(r.ctx, r.p.wallet, r.p.wallet.Originator, carrierParams(storerec.PieceDerivation))
	if err != nil {
		t.Fatal(err)
	}
	unmined.AddOutput(&transaction.TransactionOutput{Satoshis: r.tree.Outputs[r.vout].Satoshis, LockingScript: lock})
	if err := unmined.Sign(); err != nil {
		t.Fatal(err)
	}
	pc := &storerec.Piece{Bucket: testBucket, Writer: r.p.Identity(), Root: r.root, Piece: data[:1000]}
	rec, _ := pc.Encode()
	tx, err := carrier.Mint(r.ctx, r.p.wallet, r.p.wallet.Originator, carrierParams(storerec.PieceDerivation), rec, unmined, 0)
	if err != nil {
		t.Fatal(err)
	}
	want, err := tx.AtomicBEEF(false)
	if err != nil {
		t.Fatal(err)
	}
	got, at, err := pieceBEEF(tx, unmined)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) || at != len(got)-tx.Size() || !bytes.Equal(got[at:], tx.Bytes()) {
		t.Fatalf("on an unmined tree: equal %v, carrier at %d of %d", bytes.Equal(got, want), at, len(got))
	}
}
