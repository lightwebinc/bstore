package storerec

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/lightwebinc/bcommon/commit"
)

// The parallel builder makes exactly the closure, proof root and blob
// hashes Build does, whatever the write sizes and the worker count.
func TestParallelMatchesBuild(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, size := range []int{0, 1, SegmentSize - 1, SegmentSize + 1, BlobSize - 1, BlobSize, BlobSize + 1, 3*BlobSize + 5, 9 * BlobSize} {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(rng.Uint32())
		}
		want, err := Build(content)
		if err != nil {
			t.Fatal(err)
		}
		for _, workers := range []int{1, 3, 8} {
			var mu sync.Mutex
			got := map[int][32]byte{}
			p := NewParallel(workers, func(i int, h [32]byte, blob []byte) error {
				if sha256.Sum256(blob) != h {
					t.Errorf("blob %d: hash does not name it", i)
				}
				mu.Lock()
				got[i] = h
				mu.Unlock()
				return nil
			})
			for off := 0; off < len(content); {
				k := min(1+rng.IntN(700_000), len(content)-off)
				if _, err := p.Write(content[off : off+k]); err != nil {
					t.Fatal(err)
				}
				off += k
			}
			b, err := p.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if b.RootHash != want.RootHash || b.ProofRoot != want.ProofRoot || b.ContentHash != want.ContentHash || b.Size != want.Size ||
				!bytes.Equal(b.Root, want.Root) || len(b.Branches) != len(want.Branches) {
				t.Fatalf("size %d workers %d: closure differs", size, workers)
			}
			if len(got) != len(want.Blobs) {
				t.Fatalf("size %d: %d blobs stored, want %d", size, len(got), len(want.Blobs))
			}
			for i, bl := range want.Blobs {
				if got[i] != sha256.Sum256(bl) {
					t.Fatalf("size %d: blob %d hash", size, i)
				}
			}
		}
	}
}

// An error from store stops the build.
func TestParallelStoreError(t *testing.T) {
	boom := errors.New("boom")
	p := NewParallel(2, func(i int, _ [32]byte, _ []byte) error {
		if i == 1 {
			return boom
		}
		return nil
	})
	_, _ = p.Write(make([]byte, 3*BlobSize))
	if _, err := p.Finish(); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	q := NewParallel(2, func(int, [32]byte, []byte) error { return nil })
	_, _ = q.Write(make([]byte, BlobSize+1))
	q.Abort()
	q.Abort()
}

// The parallel builder's per-blob subtree, commit.SegmentRoot at the
// segment size, is BlobSubtree.
func TestBlobSubtreeIsSegmentRoot(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, size := range []int{0, 1, SegmentSize, SegmentSize + 1, BlobSize - 1, BlobSize} {
		blob := make([]byte, size)
		for i := range blob {
			blob[i] = byte(rng.Uint32())
		}
		r, err := commit.SegmentRoot(blob, SegmentSize)
		if err != nil || r != BlobSubtree(blob) {
			t.Fatalf("size %d: SegmentRoot differs from BlobSubtree", size)
		}
	}
}
