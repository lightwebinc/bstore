package provider

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"sync"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/storerec"
)

// The closure is checked as its pieces arrive, not at the end: each piece
// is hashed by the request that brings it (its SHA-256, which names it, and
// its segment leaves and subtree), on whatever core serves that request,
// and the content hash advances over the blobs in order as they land. When
// the entry is evaluated the closure is then checked from what was
// computed: the root rebuilt from its blob references and the content hash
// (profile 1's canonical construction, which is everything chirp.Verify
// checks of a closure whose root names blobs only), and the proof files
// written from the leaves already hashed. Nothing is read back or hashed a
// second time. A closure with branch nodes, a piece hashed before a
// restart, or anything else not on hand takes the full check as before.

// maxPre bounds the pieces whose leaves are kept (32 KiB a blob), and
// maxRuns the roots whose content hash is followed.
const (
	maxPre  = 1024
	maxRuns = 64
)

type preBlob struct {
	size   uint64
	leaves []byte // 32 bytes a segment
	sub    [32]byte
}

// contentRun is one root's content hash over its blobs in order.
type contentRun struct {
	mu    sync.Mutex
	order [][32]byte
	next  int
	h     hash.Hash
	sum   *[32]byte
}

type incremental struct {
	mu    sync.Mutex
	pre   map[[32]byte]*preBlob
	fifo  [][32]byte
	runs  map[[32]byte]*contentRun
	rfifo [][32]byte
}

func newIncremental() *incremental {
	return &incremental{pre: map[[32]byte]*preBlob{}, runs: map[[32]byte]*contentRun{}}
}

func (in *incremental) blob(h [32]byte) *preBlob {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.pre[h]
}

// arrived does a piece's share of the closure check: its leaves when it can
// be a blob, the root's content run when it is a root naming blobs only,
// and every run it moves on. data hashes to h.
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
	if h == root {
		if r, err := chirp.DecodeRoot(data); err == nil {
			order := make([][32]byte, 0, len(r.Children))
			for _, c := range r.Children {
				if c.Kind != chirp.ChildBlob {
					order = nil
					break
				}
				order = append(order, c.Hash)
			}
			if order != nil {
				in.mu.Lock()
				if in.runs[root] == nil {
					in.runs[root] = &contentRun{order: order, h: sha256.New()}
					in.rfifo = append(in.rfifo, root)
					for len(in.rfifo) > maxRuns {
						delete(in.runs, in.rfifo[0])
						in.rfifo = in.rfifo[1:]
					}
				}
				in.mu.Unlock()
			}
		}
	}
	in.mu.Lock()
	run := in.runs[root]
	in.mu.Unlock()
	if run != nil {
		p.advance(run, h, data)
	}
}

// advance feeds the run every blob, from its next one on, that the store
// holds; data is h's bytes, used instead of a read.
func (p *Provider) advance(run *contentRun, h [32]byte, data []byte) {
	run.mu.Lock()
	defer run.mu.Unlock()
	for run.sum == nil && run.next < len(run.order) {
		want := run.order[run.next]
		var b []byte
		if want == h {
			b = data
		} else {
			if !p.store.Has(want) {
				return
			}
			got, err := p.fetch(want)
			if err != nil || sha256.Sum256(got) != want {
				return
			}
			b = got
		}
		run.h.Write(b)
		run.next++
	}
	if run.sum == nil && run.next == len(run.order) {
		s := [32]byte(run.h.Sum(nil))
		run.sum = &s
	}
}

// quickClosure is the closure of a root whose pieces were all checked on
// arrival, or nil when anything is not on hand (the full check then runs).
// rootBytes hash to root.
func (p *Provider) quickClosure(root [32]byte, rootBytes []byte) *chirp.Closure {
	in := p.inc
	in.mu.Lock()
	run := in.runs[root]
	in.mu.Unlock()
	if run == nil {
		return nil
	}
	p.advance(run, [32]byte{}, nil)
	run.mu.Lock()
	sum := run.sum
	run.mu.Unlock()
	if sum == nil {
		return nil
	}
	r, err := chirp.DecodeRoot(rootBytes)
	if err != nil || r.Profile != chirp.Profile1 || r.Length > storerec.ClosureLimits.MaxLength || len(r.Children) > chirp.DefaultMaxReferences {
		return nil
	}
	refs := make([]chirp.Child, 0, len(r.Children))
	objects := [][32]byte{root}
	seen := map[[32]byte]bool{root: true}
	for _, c := range r.Children {
		pb := in.blob(c.Hash)
		if c.Kind != chirp.ChildBlob || pb == nil || pb.size != c.Length || !p.store.Has(c.Hash) {
			return nil
		}
		refs = append(refs, c)
		if !seen[c.Hash] {
			seen[c.Hash] = true
			objects = append(objects, c.Hash)
		}
	}
	// The canonical construction over these references and this content
	// hash is the root itself: its lengths, its content hash and its shape.
	b, err := chirp.BuildTree(refs, *sum, nil)
	if err != nil || b.RootHash != root || !bytes.Equal(b.Root, rootBytes) {
		return nil
	}
	return &chirp.Closure{Root: r, RootHash: root, Blobs: refs, Objects: objects, Canonical: true}
}

// forget drops what was kept for a root once it is decided.
func (in *incremental) forget(root [32]byte, blobs []chirp.Child) {
	in.mu.Lock()
	defer in.mu.Unlock()
	delete(in.runs, root)
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
