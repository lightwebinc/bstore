package storerec_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

const pieceBucket = "photos_qzvmkwerty"

// pieceCarriers mints the piece carriers of content, as a writer does, on a
// local chain.
func pieceCarriers(tb testing.TB, content []byte) [][]byte {
	tb.Helper()
	srv := httptest.NewServer(testchain.New(700))
	tb.Cleanup(srv.Close)
	hc, err := headers.NewSource(srv.URL)
	if err != nil {
		tb.Fatal(err)
	}
	ctx := context.Background()
	w, err := testwriter.New(ctx, filepath.Join(tb.TempDir(), "writer"), pieceBucket,
		&nodeapi.RPC{URL: srv.URL + "/rpc", ID: "w"}, &nodeapi.Asset{Base: srv.URL}, hc, srv.Client())
	if err != nil {
		tb.Fatal(err)
	}
	o, err := w.Build(ctx, content)
	if err != nil {
		tb.Fatal(err)
	}
	return o.Pieces
}

// The check names a carrier by the txid the wire declares for it, which
// ReadWire has held to be the hash of the transaction carried; the
// commitment and the funding outpoint it returns are the ones an
// independent parse and hash of the same bytes give, and a carrier whose
// bytes no longer hash to the txid declared is refused.
func TestCheckPieceCarrierNamesTheCarrierByItsHash(t *testing.T) {
	ctx := context.Background()
	marker := []byte("piece carrier ")
	pieces := pieceCarriers(t, bytes.Repeat(marker, 1000))
	if len(pieces) < 2 {
		t.Fatalf("%d pieces, want the blob and the root", len(pieces))
	}
	tampered := 0
	for i, beef := range pieces {
		pc, err := storerec.CheckPieceCarrier(ctx, beef, pieceBucket, nil)
		if err != nil {
			t.Fatalf("piece %d: %v", i, err)
		}
		tx, err := transaction.NewTransactionFromBEEF(beef)
		if err != nil {
			t.Fatalf("piece %d: %v", i, err)
		}
		if want := chainhash.DoubleHashH(tx.Bytes()); pc.Commitment != want {
			t.Fatalf("piece %d: commitment %x, the carrier hashes to %x", i, pc.Commitment, want)
		}
		in := tx.Inputs[0]
		if pc.Funding.Txid != *in.SourceTXID || pc.Funding.Index != in.SourceTxOutIndex {
			t.Fatalf("piece %d: funding %s, the carrier spends %s:%d", i, pc.Funding.String(), in.SourceTXID, in.SourceTxOutIndex)
		}
		if src := in.SourceTransaction; src == nil || chainhash.DoubleHashH(src.Bytes()) != pc.Funding.Txid {
			t.Fatalf("piece %d: the funding tree carried is not the one named", i)
		}
		at := bytes.Index(beef, marker)
		if at < 0 {
			continue // the root node carries no content
		}
		bad := append([]byte(nil), beef...)
		bad[at] ^= 0x01
		if _, err := storerec.CheckPieceCarrier(ctx, bad, pieceBucket, nil); err == nil {
			t.Fatalf("piece %d: a carrier whose bytes changed under its txid was taken", i)
		}
		tampered++
	}
	if tampered == 0 {
		t.Fatal("no piece carries the content")
	}
}

// BenchmarkCheckPieceCarrier is the check of a full 4 MiB piece carrier, as
// the provider runs it on every submission.
func BenchmarkCheckPieceCarrier(b *testing.B) {
	ctx := context.Background()
	var beef []byte
	for _, p := range pieceCarriers(b, bytes.Repeat([]byte{0xab}, storerec.MaxPiece)) {
		if len(p) > len(beef) {
			beef = p
		}
	}
	b.SetBytes(int64(len(beef)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := storerec.CheckPieceCarrier(ctx, beef, pieceBucket, nil); err != nil {
			b.Fatal(err)
		}
	}
}
