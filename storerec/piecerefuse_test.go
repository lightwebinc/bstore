package storerec_test

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/pushdrop"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// minedPieces mints content's piece carriers on a local chain and returns
// them with that chain's headers.
func minedPieces(tb testing.TB, content []byte) ([][]byte, chaintracker.ChainTracker) {
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
	return o.Pieces, hc
}

// carrierTx parses a piece carrier's Atomic BEEF into its transaction, the
// funding tree attached and proven.
func carrierTx(t *testing.T, beef []byte) *transaction.Transaction {
	t.Helper()
	tx, err := transaction.NewTransactionFromBEEF(beef)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// rewire writes tx back as an Atomic BEEF after a change, its txid hashed
// afresh.
func rewire(t *testing.T, tx *transaction.Transaction) []byte {
	t.Helper()
	tx.SetTxHash(nil)
	b, err := tx.AtomicBEEF(false)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func lockOf(tx *transaction.Transaction) []byte { return *tx.Outputs[0].LockingScript }

func setLock(tx *transaction.Transaction, s []byte) {
	ls := script.Script(s)
	tx.Outputs[0].LockingScript = &ls
}

// Each rule of CheckPieceCarrier refuses a carrier that breaks it, with its
// label, and the check as it was before the ingest stopped copying the
// piece (CheckPieceCarrierRef) gives the same verdict on every case: the
// lock compared without rebuilding it and the record read in place refuse
// exactly what the rebuild and the decoder refused.
func TestCheckPieceCarrierRefusals(t *testing.T) {
	ctx := context.Background()
	// Two full blobs and a short one: pieces pushed with OP_PUSHDATA4, and
	// a root pushed with OP_PUSHDATA2.
	pieces, hc := minedPieces(t, bytes.Repeat([]byte("refusal paths "), (2*storerec.BlobSize+1000)/14))
	if len(pieces) < 3 {
		t.Fatalf("%d pieces", len(pieces))
	}
	big, other := -1, -1
	for i, p := range pieces {
		if len(p) > 1<<20 {
			if big < 0 {
				big = i
			} else if other < 0 {
				other = i
			}
		}
	}
	if big < 0 || other < 0 {
		t.Fatal("no two full pieces")
	}
	fields := func(tx *transaction.Transaction) [][]byte {
		f, ok := pushdrop.Fields(lockOf(tx))
		if !ok || len(f) != 2 {
			t.Fatal("not a signed one-field PushDrop")
		}
		return f
	}
	otherSig := fields(carrierTx(t, pieces[other]))[1]
	elsewhere := httptest.NewServer(testchain.New(700))
	t.Cleanup(elsewhere.Close)
	otherChain, err := headers.NewSource(elsewhere.URL)
	if err != nil {
		t.Fatal(err)
	}

	type tc struct {
		name    string
		beef    []byte
		bucket  string
		headers chaintracker.ChainTracker
		want    error // nil: taken
	}
	var cases []tc
	add := func(name string, want error, mut func(tx *transaction.Transaction)) {
		for _, i := range []int{0, big} {
			tx := carrierTx(t, pieces[i])
			mut(tx)
			cases = append(cases, tc{name: name, beef: rewire(t, tx), bucket: pieceBucket, want: want})
		}
	}
	for _, p := range pieces {
		cases = append(cases, tc{name: "valid", beef: p, bucket: pieceBucket, headers: hc})
	}
	cases = append(cases,
		tc{name: "bucket grammar", beef: pieces[big], bucket: "Not_a_bucket", want: storerec.ErrHostConfig},
		tc{name: "another bucket", beef: pieces[big], bucket: "other_qzvmkwerty", want: storerec.ErrBucket},
		tc{name: "over the bound", beef: make([]byte, storerec.MaxPieceBEEF+1), bucket: pieceBucket, want: storerec.ErrBEEF},
		tc{name: "not a BEEF", beef: []byte{1, 2, 3, 4, 5}, bucket: pieceBucket, want: storerec.ErrBEEF},
		tc{name: "unmined", beef: pieces[big], bucket: pieceBucket, headers: otherChain, want: storerec.ErrUnmined},
	)
	add("satoshis over 2^53", storerec.ErrBEEF, func(tx *transaction.Transaction) { tx.Outputs[0].Satoshis = storerec.MaxSafe + 1 })
	add("two outputs", storerec.ErrShape, func(tx *transaction.Transaction) {
		one := script.Script{script.OpTRUE}
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: &one})
	})
	add("claims no piece", storerec.ErrShape, func(tx *transaction.Transaction) { setLock(tx, []byte{script.OpTRUE}) })
	add("not the funding tree's shape", storerec.ErrBEEF, func(tx *transaction.Transaction) {
		// The funding tree carried with a proof of another transaction.
		src := tx.Inputs[0].SourceTransaction
		tx2 := carrierTx(t, pieces[other])
		cp := *src
		cp.MerklePath = tx2.Inputs[0].SourceTransaction.MerklePath
		cp.Version++
		cp.SetTxHash(nil)
		tx.Inputs[0].SourceTransaction = &cp
		id := cp.TxID()
		tx.Inputs[0].SourceTXID = id
	})
	add("record breaks its rule", storerec.ErrPiece, func(tx *transaction.Transaction) {
		rec, _ := pushdrop.FirstPush(lockOf(tx))
		p, err := storerec.DecodePiece(rec)
		if err != nil {
			t.Fatal(err)
		}
		// A record that claims a piece and carries none: built by hand,
		// since Encode refuses it.
		bad, err := (&storerec.Piece{Bucket: p.Bucket, Writer: p.Writer, Root: p.Root, Piece: []byte{1}}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		bad = append(bad[:len(bad)-2], 0x40) // the piece an empty byte string
		f := fields(tx)
		key, _ := storerec.LockingKey(storerec.PieceDerivation, p.Writer)
		setLock(tx, pushdrop.Script(key, [][]byte{bad}, f[1]))
	})
	add("record not CBOR", storerec.ErrCBOR, func(tx *transaction.Transaction) {
		s := append([]byte(nil), lockOf(tx)...)
		rec, _ := pushdrop.FirstPush(s)
		rec[0] = 0xa6 // a map of six pairs, five present
		setLock(tx, s)
	})
	add("locktime mineable", storerec.ErrMineable, func(tx *transaction.Transaction) { tx.LockTime = 0 })
	add("sequence final", storerec.ErrMineable, func(tx *transaction.Transaction) {
		tx.Inputs[0].SequenceNumber = transaction.MaxTxInSequenceNum
	})
	add("unlocking empty", storerec.ErrUnlock, func(tx *transaction.Transaction) {
		e := script.Script{}
		tx.Inputs[0].UnlockingScript = &e
	})
	add("unlocking two pushes", storerec.ErrUnlock, func(tx *transaction.Transaction) {
		u := append(script.Script{}, *tx.Inputs[0].UnlockingScript...)
		u = append(u, script.OpTRUE)
		tx.Inputs[0].UnlockingScript = &u
	})
	add("three fields", storerec.ErrLock, func(tx *transaction.Transaction) {
		f := fields(tx)
		rec, _ := pushdrop.FirstPush(lockOf(tx))
		p, _ := storerec.DecodePiece(rec)
		key, _ := storerec.LockingKey(storerec.PieceDerivation, p.Writer)
		setLock(tx, pushdrop.Script(key, [][]byte{rec, []byte("extra field")}, f[1]))
	})
	add("unsigned", storerec.ErrLock, func(tx *transaction.Transaction) {
		rec, _ := pushdrop.FirstPush(lockOf(tx))
		p, _ := storerec.DecodePiece(rec)
		key, _ := storerec.LockingKey(storerec.PieceDerivation, p.Writer)
		setLock(tx, pushdrop.Script(key, [][]byte{rec}, nil))
	})
	add("another key", storerec.ErrLock, func(tx *transaction.Transaction) {
		s := append([]byte(nil), lockOf(tx)...)
		s[1] ^= 0x01 // 02 to 03: the other key of the same x
		setLock(tx, s)
	})
	add("signature pushed long", storerec.ErrLock, func(tx *transaction.Transaction) {
		f := fields(tx)
		s := lockOf(tx)
		head := s[:len(s)-1-1-len(f[1])]
		out := append(append([]byte(nil), head...), script.OpPUSHDATA1, byte(len(f[1])))
		out = append(append(out, f[1]...), script.Op2DROP)
		setLock(tx, out)
	})
	add("drops spelled out", storerec.ErrLock, func(tx *transaction.Transaction) {
		s := append([]byte(nil), lockOf(tx)...)
		s = append(s[:len(s)-1], script.OpDROP, script.OpDROP)
		setLock(tx, s)
	})
	add("a byte after the drop", storerec.ErrLock, func(tx *transaction.Transaction) {
		setLock(tx, append(append([]byte(nil), lockOf(tx)...), script.OpNOP))
	})
	add("no drop", storerec.ErrLock, func(tx *transaction.Transaction) {
		s := lockOf(tx)
		setLock(tx, append([]byte(nil), s[:len(s)-1]...))
	})
	add("another piece's signature", storerec.ErrFieldSig, func(tx *transaction.Transaction) {
		rec, _ := pushdrop.FirstPush(lockOf(tx))
		p, _ := storerec.DecodePiece(rec)
		key, _ := storerec.LockingKey(storerec.PieceDerivation, p.Writer)
		setLock(tx, pushdrop.Script(key, [][]byte{rec}, otherSig))
	})
	add("no such funding output", storerec.ErrFunding, func(tx *transaction.Transaction) { tx.Inputs[0].SourceTxOutIndex = 4000 })
	add("the tree's change", storerec.ErrFunding, func(tx *transaction.Transaction) {
		tx.Inputs[0].SourceTxOutIndex = uint32(len(tx.Inputs[0].SourceTransaction.Outputs) - 1)
	})
	add("value not carried", storerec.ErrFunding, func(tx *transaction.Transaction) { tx.Outputs[0].Satoshis++ })

	for _, c := range cases {
		got, err := storerec.CheckPieceCarrier(ctx, c.beef, c.bucket, c.headers)
		ref, rerr := storerec.CheckPieceCarrierRef(ctx, c.beef, c.bucket, c.headers)
		if c.want == nil {
			if err != nil || rerr != nil {
				t.Fatalf("%s: %v (before: %v)", c.name, err, rerr)
			}
			if got.Commitment != ref.Commitment || got.Funding != ref.Funding || got.Piece.Bucket != ref.Piece.Bucket ||
				!bytes.Equal(got.Piece.Writer, ref.Piece.Writer) || got.Piece.Root != ref.Piece.Root ||
				!bytes.Equal(got.Piece.Piece, ref.Piece.Piece) || len(got.Piece.Extra) != len(ref.Piece.Extra) {
				t.Fatalf("%s: the carrier read differs from the one read before", c.name)
			}
			continue
		}
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: %v, want %v", c.name, err, c.want)
		}
		if rerr == nil || err.Error() != rerr.Error() {
			t.Fatalf("%s: %v, before %v", c.name, err, rerr)
		}
	}
}
