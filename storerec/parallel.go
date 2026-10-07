package storerec

import (
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
		sub, err := commit.SegmentRoot(blob, SegmentSize)
		if err != nil {
			return err
		}
		p.subs.Set(i, sub)
		return store(i, ref.Hash, blob)
	})
	return p
}

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
