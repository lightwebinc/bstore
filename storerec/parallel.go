package storerec

import (
	"crypto/sha256"
	"fmt"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"
)

// Parallel builds the same closure and proof root as Streaming, with the
// hashing spread over several cores by bcommon chirp.ParallelChunker: each
// complete blob is hashed (its SHA-256, which names it, and its segment
// subtree) and handed to store by one of workers goroutines, while one
// more goroutine hashes the content in order. Memory holds at most
// workers + 2 blobs.
//
// store is called concurrently, from the worker goroutines, once per blob
// with its index and hash; the slice is reused after it returns. The first
// error from store stops the build: Write and Finish return it.
type Parallel struct {
	c        *chirp.ParallelChunker
	subs     commit.SegmentRoots
	n        uint64
	finished bool
}

// NewParallel returns a parallel builder with workers blob hashers
// (chirp.DefaultWorkers when 0 or less) that hands each blob, with its
// index and hash, to store.
func NewParallel(workers int, store func(i int, h [32]byte, blob []byte) error) *Parallel {
	p := &Parallel{}
	p.c = chirp.NewParallelChunker(workers, func(i int, ref chirp.Child, blob []byte) error {
		p.subs.Set(i, SegmentRootOf(blob))
		return store(i, ref.Hash, blob)
	})
	return p
}

// SegmentRootOf is commit.SegmentRoot of a blob at SegmentSize, the
// segment subtree root a proof root is composed of, hashed in place: each
// segment is a slice of the blob, copied nowhere.
func SegmentRootOf(blob []byte) [32]byte {
	var b commit.Builder
	for len(blob) > 0 {
		k := min(SegmentSize, len(blob))
		b.AddBytes(blob[:k])
		blob = blob[k:]
	}
	return b.Root()
}

// HashBlob is a blob's three hashes from one pass over its bytes: its
// SHA-256 (the object it is), the leaf hash of each SegmentSize segment
// (commit.HashLeaf, 32 bytes a segment, in order) and the subtree root over
// them (commit.RootOfLeafHashes), which a proof root is composed of. Each
// segment is read once, while it is in cache, by both digests, and the leaf
// digest is reused across segments, so nothing is allocated but the leaves.
// The values are exactly those of sha256.Sum256, commit.HashLeaf and
// commit.RootOfLeafHashes over the same bytes.
func HashBlob(blob []byte) (hash [32]byte, leaves []byte, sub [32]byte) {
	d := sha256.New()
	l := sha256.New()
	leaves = make([]byte, 0, 32*((len(blob)+SegmentSize-1)/SegmentSize))
	var b commit.Builder
	for len(blob) > 0 {
		k := min(SegmentSize, len(blob))
		seg := blob[:k]
		blob = blob[k:]
		d.Write(seg)
		l.Reset()
		l.Write(leafPrefix[:])
		l.Write(seg)
		leaves = l.Sum(leaves)
		b.Add([32]byte(leaves[len(leaves)-32:]))
	}
	return [32]byte(d.Sum(nil)), leaves, b.Root()
}

// leafPrefix is RFC 6962's leaf domain separator, as commit.HashLeaf writes it.
var leafPrefix = [1]byte{0x00}

// Write takes content in order.
func (p *Parallel) Write(b []byte) (int, error) {
	if p.finished {
		return 0, fmt.Errorf("storerec: write after finish")
	}
	if p.n+uint64(len(b)) > MaxSize {
		return 0, fmt.Errorf("%w: over 2^43 bytes", ErrSize)
	}
	n, err := p.c.Write(b)
	p.n += uint64(n)
	return n, err
}

// Finish hands over the last blob, waits for every blob, and returns the
// closure (Blobs nil) and its proof root.
func (p *Parallel) Finish() (*Built, error) {
	if p.finished {
		return nil, fmt.Errorf("storerec: finish twice")
	}
	p.finished = true
	b, err := p.c.Finish(nil)
	if err != nil {
		return nil, err
	}
	root, err := chirp.DecodeRoot(b.Root)
	if err != nil {
		return nil, err
	}
	proof, err := p.subs.Root()
	if err != nil {
		return nil, err
	}
	return &Built{Built: b, ContentHash: root.ContentHash, ProofRoot: proof, Size: p.n}, nil
}

// Abort stops the builder without a result; it is safe after Finish.
func (p *Parallel) Abort() {
	p.finished = true
	p.c.Abort()
}
