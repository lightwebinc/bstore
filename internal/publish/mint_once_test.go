package publish

import (
	"bytes"
	"testing"

	"github.com/lightwebinc/bcommon/carrier"

	"github.com/lightwebinc/bstore/storerec"
)

// mintPiece's carrier built once (carrier.MintOnce + pieceBEEFOnce) is byte for byte the BEEF of the
// copy-per-step path (Encode, carrier.Mint, pieceBEEF): same carrier, same txid, same object offset,
// and a provider takes it.
func TestMintPieceBuiltOnceMatchesCopyPath(t *testing.T) {
	r := newMintRig(t)
	if r.tree.MerklePath == nil {
		t.Skip("the rig's funding tree has no proof: the built-once path is not taken")
	}
	for _, n := range []int{2, 75, 76, 255, 256, 65535, 65536, 1 << 20, 4 << 20} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i*131 + n)
		}
		tx, beef, at, err := r.p.mintPiece(r.ctx, testBucket, r.root, data, r.tree, r.vout)
		if err != nil {
			t.Fatalf("%d: %v", n, err)
		}
		got := bytes.Clone(beef)
		pc := &storerec.Piece{Bucket: testBucket, Writer: r.p.Identity(), Root: r.root, Piece: data}
		rec, err := pc.Encode()
		if err != nil {
			t.Fatal(err)
		}
		ref, err := carrier.Mint(r.ctx, r.p.wallet, r.p.wallet.Originator, carrierParams(storerec.PieceDerivation), rec, r.tree, r.vout)
		if err != nil {
			t.Fatal(err)
		}
		want, wat, err := pieceBEEF(ref, r.tree)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%d: the built-once BEEF differs from the copy path's (%d vs %d bytes)", n, len(got), len(want))
		}
		if *tx.TxID() != *ref.TxID() {
			t.Fatalf("%d: txid %s vs %s", n, tx.TxID(), ref.TxID())
		}
		if a, b := at, objectAt(want, wat, ref, len(rec), n); a != b {
			t.Fatalf("%d: object at %d vs %d", n, a, b)
		}
		if _, err := storerec.CheckPieceCarrier(r.ctx, got, testBucket, nil); err != nil {
			t.Fatalf("%d: a provider refuses the built-once carrier: %v", n, err)
		}
	}
}
