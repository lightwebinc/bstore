package storerec

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/record"
)

func testWriter(t *testing.T) []byte {
	t.Helper()
	k, err := ec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k.PubKey().Compressed()
}

// generic is the record codec's encoding of a piece, what Encode wrote
// before pieces were written directly.
func generic(p *Piece) ([]byte, error) {
	return encodeRecord(cbor.Map{
		{Key: PKMagic, Val: MagicPiece},
		{Key: PKBucket, Val: p.Bucket},
		{Key: PKWriter, Val: p.Writer},
		{Key: PKRoot, Val: b(p.Root)},
		{Key: PKPiece, Val: p.Piece},
	}, p.Extra, PKLast, MaxPieceRecord)
}

// A piece written directly is byte for byte the record codec's, across
// every CBOR length boundary of the piece and of the bucket, up to a 4 MiB
// piece; it decodes, and the piece is placed at its end. With Extra the
// codec writes it.
func TestPieceEncodeMatchesRecordCodec(t *testing.T) {
	writer := testWriter(t)
	buckets := []string{"a_abcdefghij", "abc_abcdefghij", "abcdefghijk_abcdefghij", "abcdefghijkl_abcdefghij",
		strings.Repeat("a", 25) + "_abcdefghij"}
	big := bytes.Repeat([]byte("piece bytes "), MaxPiece/12+1)[:MaxPiece]
	for _, bucket := range buckets {
		for _, n := range []int{1, 23, 24, 255, 256, 65535, 65536, MaxPiece} {
			p := &Piece{Bucket: bucket, Writer: writer, Root: [32]byte{7, 8, 9}, Piece: big[:n]}
			got, err := p.Encode()
			if err != nil {
				t.Fatalf("%s, %d bytes: %v", bucket, n, err)
			}
			want, err := generic(p)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%s, %d bytes: %d bytes written directly, %d by the codec", bucket, n, len(got), len(want))
			}
			if at := PieceAt(got, n); at != len(got)-n {
				t.Fatalf("%s, %d bytes: the piece placed at %d of %d", bucket, n, at, len(got))
			}
			if PieceAt(got, n+1) != -1 || PieceAt(got, n-1) != -1 {
				t.Fatalf("%s, %d bytes: a piece of another length placed", bucket, n)
			}
			d, err := DecodePiece(got)
			if err != nil || d.Bucket != bucket || !bytes.Equal(d.Writer, writer) || d.Root != p.Root || !bytes.Equal(d.Piece, p.Piece) || len(d.Extra) != 0 {
				t.Fatalf("%s, %d bytes: read back %+v: %v", bucket, n, d, err)
			}
		}
	}
	p := &Piece{Bucket: buckets[0], Writer: writer, Root: [32]byte{1}, Piece: big[:100], Extra: cbor.Map{{Key: PKLast + 1, Val: []byte("x")}}}
	got, err := p.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := generic(p); !bytes.Equal(got, want) {
		t.Fatal("a piece with Extra is not the codec's")
	}
	if PieceAt(got, 100) != -1 {
		t.Fatal("a piece followed by Extra was placed")
	}
}

// A piece written directly is refused as DecodePiece refuses the codec's
// bytes: the same sentinel and the same words.
func TestPieceEncodeRefusesAsDecodeDoes(t *testing.T) {
	writer := testWriter(t)
	good := &Piece{Bucket: "a_abcdefghij", Writer: writer, Root: [32]byte{1}, Piece: []byte("x")}
	if _, err := good.Encode(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		p    Piece
		want error
	}{
		{"empty piece", Piece{Bucket: good.Bucket, Writer: writer, Piece: nil}, ErrPiece},
		{"piece over the bound", Piece{Bucket: good.Bucket, Writer: writer, Piece: make([]byte, MaxPiece+1)}, ErrPiece},
		{"record over the bound", Piece{Bucket: good.Bucket, Writer: writer, Piece: make([]byte, MaxPieceRecord)}, record.ErrTooLarge},
		{"bucket", Piece{Bucket: "Not_A_Bucket", Writer: writer, Piece: []byte("x")}, ErrBucket},
		{"writer", Piece{Bucket: good.Bucket, Writer: []byte{1, 2, 3}, Piece: []byte("x")}, ErrIdentity},
		{"anyone writer", Piece{Bucket: good.Bucket, Writer: AnyoneKey, Piece: []byte("x")}, ErrIdentity},
	}
	for _, c := range cases {
		_, err := c.p.Encode()
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: %v, want %v", c.name, err, c.want)
		}
		// The codec's words: what DecodePiece says of the codec's bytes, or
		// what the codec says of a record it refuses to write.
		raw, werr := generic(&c.p)
		var old error
		if werr != nil {
			old = werr
		} else {
			_, old = DecodePiece(raw)
		}
		if old == nil || old.Error() != err.Error() {
			t.Fatalf("%s: %q, the codec said %q", c.name, err, old)
		}
	}
	// A bucket that is not UTF-8 is refused as the codec refuses it.
	bad := Piece{Bucket: "a_abcdefghi\xff", Writer: writer, Piece: []byte("x")}
	_, err := bad.Encode()
	_, werr := generic(&bad)
	if !errors.Is(err, record.ErrCBOR) || werr == nil || err.Error() != werr.Error() {
		t.Fatalf("a bucket that is not UTF-8: %v, the codec said %v", err, werr)
	}
}
