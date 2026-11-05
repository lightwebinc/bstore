package storerec

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/lightwebinc/bcommon/commit"
)

// oldBlobHashes is what HashBlob replaced: three passes, sha256.Sum256,
// commit.HashLeaf a segment and commit.RootOfLeafHashes over the leaves.
func oldBlobHashes(blob []byte) ([32]byte, []byte, [32]byte) {
	var leaves []byte
	var lh [][32]byte
	for off := 0; off < len(blob); off += SegmentSize {
		l := commit.HashLeaf(blob[off:min(off+SegmentSize, len(blob))])
		lh = append(lh, l)
		leaves = append(leaves, l[:]...)
	}
	return sha256.Sum256(blob), leaves, commit.RootOfLeafHashes(lh)
}

func TestHashBlobMatchesTheSeparatePasses(t *testing.T) {
	for _, n := range []int{0, 1, 100, SegmentSize - 1, SegmentSize, SegmentSize + 1, 3 * SegmentSize, 10*SegmentSize + 7, BlobSize - 1, BlobSize} {
		blob := make([]byte, n)
		for i := range blob {
			blob[i] = byte(i*7 + n)
		}
		h, leaves, sub := HashBlob(blob)
		oh, ol, os := oldBlobHashes(blob)
		if h != oh {
			t.Errorf("%d bytes: hash differs", n)
		}
		if !bytes.Equal(leaves, ol) || len(leaves) != 32*((n+SegmentSize-1)/SegmentSize) {
			t.Errorf("%d bytes: leaves differ (%d bytes)", n, len(leaves))
		}
		if sub != os || (n > 0 && sub != SegmentRootOf(blob)) {
			t.Errorf("%d bytes: subtree root differs", n)
		}
	}
}

// BenchmarkHashBlob is one 4 MiB blob: one pass against the three it took.
func BenchmarkHashBlob(b *testing.B) {
	blob := make([]byte, BlobSize)
	for i := range blob {
		blob[i] = byte(i)
	}
	b.Run("onepass", func(b *testing.B) {
		b.SetBytes(BlobSize)
		b.ReportAllocs()
		for range b.N {
			HashBlob(blob)
		}
	})
	b.Run("separate", func(b *testing.B) {
		b.SetBytes(BlobSize)
		b.ReportAllocs()
		for range b.N {
			oldBlobHashes(blob)
		}
	})
}
