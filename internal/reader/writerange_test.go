package reader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/storerec"
)

// mapLocal serves blobs by hash; a hash in fail is refused, and every Get
// is counted.
type mapLocal struct {
	m    map[[32]byte][]byte
	fail map[[32]byte]bool
	gets atomic.Int64
}

func (l *mapLocal) Get(h [32]byte) ([]byte, error) {
	l.gets.Add(1)
	if l.fail[h] {
		return nil, errors.New("gone")
	}
	b, ok := l.m[h]
	if !ok {
		return nil, errors.New("missing")
	}
	return b, nil
}

// testObject is an Object over content cut into blobs, every blob in a
// local store (none from advertisers).
func testObject(t *testing.T, content []byte) (*Object, *mapLocal) {
	t.Helper()
	l := &mapLocal{m: map[[32]byte][]byte{}, fail: map[[32]byte]bool{}}
	var blobs []chirp.Child
	for off := 0; off < len(content); off += storerec.BlobSize {
		b := content[off:min(off+storerec.BlobSize, len(content))]
		h := sha256.Sum256(b)
		l.m[h] = b
		blobs = append(blobs, chirp.Child{Kind: chirp.ChildBlob, Length: uint64(len(b)), Hash: h})
	}
	o := &Object{r: &Reader{Logf: t.Logf}, entry: &storerec.Entry{Size: uint64(len(content)), ContentHash: sha256.Sum256(content)},
		blobs: blobs, local: l}
	return o, l
}

// serial is WriteRange as it was, one blob at a time: the reference the
// pipelined read must match, byte for byte and error for error.
func serial(o *Object, off, n uint64) ([]byte, error) {
	var out bytes.Buffer
	whole := off == 0 && n == o.entry.Size
	ch := sha256.New()
	first, last := off/storerec.BlobSize, (off+n-1)/storerec.BlobSize
	for i := first; i <= last; i++ {
		c := o.blobs[i]
		b, err := o.fetch(context.Background(), c.Hash, c.Length)
		if err != nil {
			return out.Bytes(), err
		}
		if whole {
			ch.Write(b)
			if i == last && [32]byte(ch.Sum(nil)) != o.entry.ContentHash {
				return out.Bytes(), ErrContentHash
			}
		}
		start, end := uint64(0), c.Length
		if i == first {
			start = off - i*storerec.BlobSize
		}
		if i == last {
			end = off + n - i*storerec.BlobSize
		}
		out.Write(b[start:end])
	}
	return out.Bytes(), nil
}

func TestWriteRangeMatchesSerial(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	content := make([]byte, 7*storerec.BlobSize+12345)
	for i := range content {
		content[i] = byte(r.Uint32())
	}
	o, _ := testObject(t, content)
	size := uint64(len(content))
	B := uint64(storerec.BlobSize)
	ranges := [][2]uint64{{0, size}, {0, 1}, {size - 1, 1}, {B - 1, 2}, {B, B}, {3*B + 7, 2*B + 5}, {0, B}, {size - 12345, 12345}}
	for range 20 {
		off := r.Uint64N(size)
		ranges = append(ranges, [2]uint64{off, 1 + r.Uint64N(size-off)})
	}
	for _, ahead := range []int{0, 1, 2, 8} {
		ReadAhead = ahead
		for _, rg := range ranges {
			var got bytes.Buffer
			if err := o.WriteRange(context.Background(), &got, rg[0], rg[1]); err != nil {
				t.Fatalf("ahead %d range %v: %v", ahead, rg, err)
			}
			if !bytes.Equal(got.Bytes(), content[rg[0]:rg[0]+rg[1]]) {
				t.Fatalf("ahead %d range %v: wrong bytes", ahead, rg)
			}
		}
	}
	ReadAhead = 2
}

// A whole read whose content hash fails writes every blob but the last,
// as the serial read did; a range read is not held to the content hash.
func TestWriteRangeContentHash(t *testing.T) {
	content := bytes.Repeat([]byte("bstore"), 3*storerec.BlobSize/6+100)
	o, _ := testObject(t, content)
	o.entry.ContentHash[0] ^= 1
	want, werr := serial(o, 0, o.entry.Size)
	var got bytes.Buffer
	err := o.WriteRange(context.Background(), &got, 0, o.entry.Size)
	if !errors.Is(err, ErrContentHash) || !errors.Is(werr, ErrContentHash) {
		t.Fatalf("got %v, serial %v", err, werr)
	}
	if !bytes.Equal(got.Bytes(), want) || uint64(got.Len()) != 3*storerec.BlobSize {
		t.Fatalf("wrote %d bytes before refusing, serial %d", got.Len(), len(want))
	}
	got.Reset()
	if err := o.WriteRange(context.Background(), &got, 1, o.entry.Size-1); err != nil {
		t.Fatal(err)
	}
}

// A blob that cannot be fetched stops the read where the serial read
// stopped, with its error, and nothing after it is written.
func TestWriteRangeFetchError(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	content := make([]byte, 6*storerec.BlobSize)
	for i := range content {
		content[i] = byte(r.Uint32())
	}
	o, l := testObject(t, content)
	l.fail[o.blobs[3].Hash] = true
	want, werr := serial(o, 0, o.entry.Size)
	var got bytes.Buffer
	err := o.WriteRange(context.Background(), &got, 0, o.entry.Size)
	if !errors.Is(err, ErrNoSource) || !errors.Is(werr, ErrNoSource) {
		t.Fatalf("got %v, serial %v", err, werr)
	}
	if !bytes.Equal(got.Bytes(), want) || got.Len() != 3*storerec.BlobSize {
		t.Fatalf("wrote %d bytes, serial %d", got.Len(), len(want))
	}
}

// A writer that fails stops the read; blobs fetched ahead are dropped and
// no more are asked for.
func TestWriteRangeWriterError(t *testing.T) {
	content := make([]byte, 40*storerec.BlobSize)
	o, l := testObject(t, content)
	w := &failAfter{n: 2}
	if err := o.WriteRange(context.Background(), w, 0, o.entry.Size); err == nil {
		t.Fatal("a failing writer went unnoticed")
	}
	if g := l.gets.Load(); g > int64(2+ReadAhead+2) {
		t.Fatalf("%d blobs fetched for a read that stopped after 2", g)
	}
}

type failAfter struct{ n int }

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n == 0 {
		return 0, errors.New("closed")
	}
	f.n--
	return len(p), nil
}

func BenchmarkWriteRange(b *testing.B) {
	content := make([]byte, 64*storerec.BlobSize)
	for i := range content {
		content[i] = byte(i)
	}
	t := &testing.T{}
	o, _ := testObject(t, content)
	b.Run("serial", func(b *testing.B) {
		b.SetBytes(int64(len(content)))
		for range b.N {
			if _, err := serial(o, 0, o.entry.Size); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, ahead := range []int{0, 2} {
		b.Run("ahead"+string(rune('0'+ahead)), func(b *testing.B) {
			ReadAhead = ahead
			b.SetBytes(int64(len(content)))
			for range b.N {
				if err := o.WriteRange(context.Background(), discard{}, 0, o.entry.Size); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	ReadAhead = 2
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
