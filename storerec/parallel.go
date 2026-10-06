package storerec

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"runtime"
	"sync"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"
)

// Parallel builds the same closure and proof root as Streaming, with the
// hashing spread over several cores: each complete blob is hashed (its
// SHA-256, which names it, and its segment subtree) and handed to store by
// one of workers goroutines, while one more goroutine hashes the content
// in order. Write only copies into the current blob, so the caller's loop
// is never held up by a hash. Memory holds at most workers + 2 blobs.
//
// store is called concurrently, from the worker goroutines, once per blob
// with its hash; the slice is reused after it returns. The first error
// from store stops the build: Write and Finish return it.
//
// bcommon later: the per-blob work (chirp.Child of a blob, its segment
// subtree) and the content hash pipeline belong in bcommon chirp as a
// parallel Chunker; it is kept here until bcommon takes it.
type Parallel struct {
	store func(i int, h [32]byte, blob []byte) error

	free    chan []byte
	work    chan pjob
	content chan pjob
	wg      sync.WaitGroup // the workers and the content hasher
	cur     []byte
	n       uint64
	blobs   int

	mu   sync.Mutex
	refs []chirp.Child
	subs [][32]byte
	err  error

	contentHash [32]byte
	finished    bool
}

type pjob struct {
	i    int
	blob []byte
	done *sync.WaitGroup
}

// DefaultWorkers is the number of blob hashers when none is given: the
// cores the process may use, at most 8.
func DefaultWorkers() int { return min(max(runtime.GOMAXPROCS(0), 1), 8) }

// NewParallel returns a parallel builder with workers blob hashers (at
// least 1) that hands each blob, with its index and hash, to store.
func NewParallel(workers int, store func(i int, h [32]byte, blob []byte) error) *Parallel {
	if workers < 1 {
		workers = 1
	}
	p := &Parallel{store: store, free: make(chan []byte, workers+2), work: make(chan pjob, workers), content: make(chan pjob, workers+2)}
	for range workers + 2 {
		p.free <- make([]byte, 0, BlobSize)
	}
	p.wg.Add(workers + 1)
	for range workers {
		go p.worker()
	}
	go p.hashContent()
	return p
}

func (p *Parallel) worker() {
	defer p.wg.Done()
	for j := range p.work {
		if p.failed() == nil {
			h := sha256.Sum256(j.blob)
			sub := BlobSubtree(j.blob)
			err := p.store(j.i, h, j.blob)
			p.mu.Lock()
			if err != nil && p.err == nil {
				p.err = err
			}
			p.refs[j.i] = chirp.Child{Kind: chirp.ChildBlob, Length: uint64(len(j.blob)), Hash: h}
			p.subs[j.i] = sub
			p.mu.Unlock()
		}
		j.done.Done()
	}
}

func (p *Parallel) hashContent() {
	defer p.wg.Done()
	var h hash.Hash = sha256.New()
	for j := range p.content {
		h.Write(j.blob)
		j.done.Done()
	}
	p.contentHash = [32]byte(h.Sum(nil))
}

func (p *Parallel) failed() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// dispatch hands the current blob to a worker and to the content hasher;
// the buffer goes back to the free list once both are done with it.
func (p *Parallel) dispatch() {
	b := p.cur
	p.cur = nil
	i := p.blobs
	p.blobs++
	p.mu.Lock()
	p.refs = append(p.refs, chirp.Child{})
	p.subs = append(p.subs, [32]byte{})
	p.mu.Unlock()
	done := &sync.WaitGroup{}
	done.Add(2)
	p.content <- pjob{i: i, blob: b, done: done}
	p.work <- pjob{i: i, blob: b, done: done}
	go func() {
		done.Wait()
		p.free <- b[:0]
	}()
}

// Write takes content in order.
func (p *Parallel) Write(b []byte) (int, error) {
	if p.finished {
		return 0, fmt.Errorf("storerec: write after finish")
	}
	if err := p.failed(); err != nil {
		return 0, err
	}
	if p.n+uint64(len(b)) > MaxSize {
		return 0, fmt.Errorf("%w: over 2^43 bytes", ErrSize)
	}
	n := len(b)
	for len(b) > 0 {
		if p.cur == nil {
			p.cur = <-p.free
		}
		k := min(BlobSize-len(p.cur), len(b))
		p.cur = append(p.cur, b[:k]...)
		b = b[k:]
		p.n += uint64(k)
		if len(p.cur) == BlobSize {
			p.dispatch()
		}
	}
	return n, nil
}

// Finish hands over the last blob, waits for every blob, and returns the
// closure (Blobs nil) and its proof root.
func (p *Parallel) Finish() (*Built, error) {
	if p.finished {
		return nil, fmt.Errorf("storerec: finish twice")
	}
	if len(p.cur) > 0 {
		p.dispatch()
	}
	p.finished = true
	close(p.work)
	close(p.content)
	p.wg.Wait()
	if p.err != nil {
		return nil, p.err
	}
	b, err := chirp.BuildTree(p.refs, p.contentHash, nil)
	if err != nil {
		return nil, err
	}
	proof := EmptyHash
	if len(p.subs) > 0 {
		if proof, err = commit.RootOfSubtrees(p.subs); err != nil {
			return nil, err
		}
	}
	return &Built{Built: b, ContentHash: p.contentHash, ProofRoot: proof, Size: p.n}, nil
}

// Abort stops the builder without a result; it is safe after Finish.
func (p *Parallel) Abort() {
	if p.finished {
		return
	}
	p.finished = true
	p.mu.Lock()
	if p.err == nil {
		p.err = fmt.Errorf("storerec: aborted")
	}
	p.mu.Unlock()
	close(p.work)
	close(p.content)
	p.wg.Wait()
}
