package storerec

import (
	"bytes"
	"math/rand/v2"
	"reflect"
	"testing"
	"unsafe"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"
)

// sameDecode holds decodePieceInPlace to DecodePiece on rec: the same piece
// or the same refusal.
func sameDecode(t testing.TB, rec []byte) (inPlace bool) {
	t.Helper()
	want, werr := DecodePiece(rec)
	got, err := decodePieceInPlace(rec)
	if (werr == nil) != (err == nil) || (err != nil && err.Error() != werr.Error()) {
		t.Fatalf("in place %v, DecodePiece %v", err, werr)
	}
	if err != nil {
		return false
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("in place read another piece")
	}
	return len(got.Piece) > 0 && unsafe.SliceData(got.Piece) == unsafe.SliceData(rec[len(rec)-len(got.Piece):])
}

// A piece record read in place is the piece DecodePiece reads, across every
// CBOR length boundary of the piece and the bucket up to a full piece, the
// piece a slice of the record; one with Extra, and every record changed by
// a byte at its joints, cut, extended or written with a longer head than
// the shortest, is DecodePiece's verdict too.
func TestPieceInPlaceAgreesWithDecodePiece(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	writer := testWriter(t)
	big := make([]byte, MaxPiece+1)
	for i := range big {
		big[i] = byte(rng.Uint32())
	}
	buckets := []string{"a_qzvmkwerty", "photos_qzvmkwerty", "abcdefghijklmnopqrstuvwxy_qzvmkwerty"}
	sizes := []int{1, 23, 24, 255, 256, 65535, 65536, MaxPiece}
	inPlace := 0
	for _, bk := range buckets {
		for _, n := range sizes {
			p := &Piece{Bucket: bk, Writer: writer, Root: [32]byte{byte(n), 2, 3}, Piece: big[:n]}
			rec, err := p.Encode()
			if err != nil {
				t.Fatal(err)
			}
			if !sameDecode(t, rec) {
				t.Fatalf("bucket %q, piece %d: not read in place", bk, n)
			}
			inPlace++
			// Every byte of the head and the piece's first and last, and
			// some between.
			at := PieceAt(rec, n)
			pos := map[int]bool{}
			for i := 0; i < at+4 && i < len(rec); i++ {
				pos[i] = true
			}
			pos[len(rec)-1] = true
			for range 16 {
				pos[rng.IntN(len(rec))] = true
			}
			for i := range pos {
				for _, x := range []byte{0x01, 0x20, 0x80, 0xff} {
					m := append([]byte(nil), rec...)
					m[i] ^= x
					sameDecode(t, m)
				}
			}
			sameDecode(t, rec[:len(rec)-1])
			sameDecode(t, append(append([]byte(nil), rec...), 0))
			// The bucket's length in a two-byte head where one byte does.
			long := append([]byte(nil), rec[:1+1+1+len(MagicPiece)+1]...)
			long = append(long, 0x78, byte(len(bk)))
			long = append(long, rec[len(long)-1:]...)
			sameDecode(t, long)
		}
	}
	// With Extra, DecodePiece reads it.
	p := &Piece{Bucket: buckets[0], Writer: writer, Root: [32]byte{9}, Piece: big[:300], Extra: cbor.Map{{Key: PKLast + 1, Val: []byte("x")}}}
	rec, err := p.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if sameDecode(t, rec) {
		t.Fatal("a record with Extra read in place")
	}
	// Records that break a rule: an empty piece, one too large, a bad
	// bucket, the anyone key, another magic.
	for _, mk := range []func() []byte{
		func() []byte {
			r, _ := (&Piece{Bucket: buckets[0], Writer: writer, Piece: []byte{1}}).Encode()
			return append(r[:len(r)-2], 0x40)
		},
		func() []byte {
			r, _ := (&Piece{Bucket: buckets[0], Writer: writer, Piece: big[:MaxPiece]}).Encode()
			r = append(r, 0)
			r[len(r)-MaxPiece-2] = 0x01 // the head now declares MaxPiece+1
			return r
		},
		func() []byte {
			r, _ := (&Piece{Bucket: buckets[0], Writer: writer, Piece: []byte{1}}).Encode()
			return bytes.Replace(r, []byte(buckets[0]), []byte("A_qzvmkwerty"), 1)
		},
		func() []byte {
			r, _ := (&Piece{Bucket: buckets[0], Writer: writer, Piece: []byte{1}}).Encode()
			return bytes.Replace(r, writer, AnyoneKey, 1)
		},
		func() []byte {
			r, _ := (&Piece{Bucket: buckets[0], Writer: writer, Piece: []byte{1}}).Encode()
			return bytes.Replace(r, MagicPiece, []byte("stp\x02"), 1)
		},
	} {
		r := mk()
		if _, err := DecodePiece(r); err == nil {
			t.Fatal("a broken record decodes")
		}
		sameDecode(t, r)
	}
	if inPlace == 0 {
		t.Fatal("nothing read in place")
	}
}

// FuzzPieceInPlace holds the in-place read to DecodePiece on any bytes.
func FuzzPieceInPlace(f *testing.F) {
	k, err := ec.NewPrivateKey()
	if err != nil {
		f.Fatal(err)
	}
	w := k.PubKey().Compressed()
	for _, n := range []int{1, 24, 256} {
		rec, err := (&Piece{Bucket: "photos_qzvmkwerty", Writer: w, Piece: bytes.Repeat([]byte{7}, n)}).Encode()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(rec)
	}
	f.Fuzz(func(t *testing.T, rec []byte) { sameDecode(t, rec) })
}
