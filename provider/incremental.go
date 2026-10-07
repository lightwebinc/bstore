package provider

import (
	"sync"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/storerec"
)

// The closure is checked as its pieces arrive, not at the end: each piece
// is hashed by the request that brings it (its SHA-256, which names it, and
// its segment leaves and subtree), on whatever core serves that request,
// and handed to the root's bcommon chirp.Incremental, which walks the
// closure as chirp.Verify does as far as the pieces on hand allow. When the
// entry is evaluated, Check finishes the walk from there, and the proof
// files are written from the leaves already hashed. A closure with branch
// nodes is walked the same way; a root whose walk was evicted, or one after
// a restart, starts a walk on demand that reads back only the pieces held
// and continues as more arrive.

// maxPre bounds the pieces whose leaves are kept (32 KiB a blob), maxRoots
// the roots whose walk is kept, and rootBuffer the bytes each walk keeps of
// pieces that arrived ahead of it (a piece not kept is read back from the
// store when the walk reaches it): at most 64 x 8 MiB.
const (
	maxPre     = 1024
	maxRoots   = 64
	rootBuffer = 2 * chirp.ChunkSize
)

type preBlob struct {
	size   uint64
	leaves []byte // 32 bytes a segment
	sub    [32]byte
}

// closureWalk is one root's walk and its fetch's wrong-bytes report.
type closureWalk struct {
	inc   *chirp.Incremental
	wrong func() bool
}

type incremental struct {
	mu    sync.Mutex
	pre   map[[32]byte]*preBlob
	fifo  [][32]byte
	walks map[[32]byte]*closureWalk
	wfifo [][32]byte
}

func newIncremental() *incremental {
	return &incremental{pre: map[[32]byte]*preBlob{}, walks: map[[32]byte]*closureWalk{}}
}

func (in *incremental) blob(h [32]byte) *preBlob {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.pre[h]
}

// walk returns root's walk, made on demand over the store.
func (p *Provider) walk(root [32]byte) *closureWalk {
	in := p.inc
	in.mu.Lock()
	defer in.mu.Unlock()
	if w := in.walks[root]; w != nil {
		return w
	}
	fetch, wrong := storerec.CheckedFetch(p.fetch)
	w := &closureWalk{inc: chirp.NewIncremental(root, fetch, storerec.ClosureLimits, rootBuffer), wrong: wrong}
	in.walks[root] = w
	in.wfifo = append(in.wfifo, root)
	for len(in.wfifo) > maxRoots {
		delete(in.walks, in.wfifo[0])
		in.wfifo = in.wfifo[1:]
	}
	return w
}

// checkClosure is storerec.VerifyCHIRP's answer for root from its walk:
// only what the walk has not yet reached is read and hashed.
func (p *Provider) checkClosure(root [32]byte) (*chirp.Closure, error) {
	w := p.walk(root)
	c, err := w.inc.Check()
	if err != nil {
		return nil, storerec.ClosureError(err, w.wrong())
	}
	return c, nil
}

// arrived does a piece's share of the closure check: its leaves when it can
// be a blob, and the root's walk moved on. data hashes to h and is stored.
func (p *Provider) arrived(h [32]byte, data []byte, root [32]byte) {
	in := p.inc
	if len(data) <= storerec.BlobSize && in.blob(h) == nil {
		leaves := make([]byte, 0, 32*((len(data)+storerec.SegmentSize-1)/storerec.SegmentSize))
		var lh [][32]byte
		for off := 0; off < len(data); off += storerec.SegmentSize {
			l := commit.HashLeaf(data[off:min(off+storerec.SegmentSize, len(data))])
			lh = append(lh, l)
			leaves = append(leaves, l[:]...)
		}
		pb := &preBlob{size: uint64(len(data)), leaves: leaves, sub: commit.RootOfLeafHashes(lh)}
		in.mu.Lock()
		if in.pre[h] == nil {
			in.pre[h] = pb
			in.fifo = append(in.fifo, h)
			for len(in.fifo) > maxPre {
				delete(in.pre, in.fifo[0])
				in.fifo = in.fifo[1:]
			}
		}
		in.mu.Unlock()
	}
	p.walk(root).inc.Add(data)
}

// forget drops what was kept for a root once it is decided.
func (in *incremental) forget(root [32]byte, blobs []chirp.Child) {
	in.mu.Lock()
	defer in.mu.Unlock()
	delete(in.walks, root)
	for _, b := range blobs {
		delete(in.pre, b.Hash)
	}
}

// preLeaves are a blob's leaves and subtree root when hashed on arrival.
func (p *Provider) preLeaves(h [32]byte) ([]byte, [32]byte, bool) {
	pb := p.inc.blob(h)
	if pb == nil {
		return nil, [32]byte{}, false
	}
	return pb.leaves, pb.sub, true
}
