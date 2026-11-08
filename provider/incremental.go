package provider

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/storerec"
)

// The closure is checked as its pieces arrive, not at the end: each piece
// is hashed by the request that brings it (its SHA-256, which names it, and
// its segment leaves and subtree, all from one pass in keepPiece), on
// whatever core serves that request, and handed to the root's bcommon
// chirp.Incremental, which walks the closure as chirp.Verify does as far as
// the pieces on hand allow. When the entry is evaluated, Check finishes the
// walk from there, and the proof files are written from the leaves already
// hashed. A closure with branch nodes is walked the same way; a root whose
// walk was evicted, or one after a restart, starts a walk on demand that
// reads back only the pieces held and continues as more arrive.
//
// A closure its gateway in this process staged is not walked (the owner's
// ruling of 2026-11-08 on the storage node): pieces a gateway hands over on
// its account (TakeLocal with the piece check off) are carried, and the
// root they name is checked on that account at the evaluation
// (storerec.VerifyCarried): its root and branch nodes read, hashed and
// checked, its blobs taken unread where this provider stored them from the
// bytes hashed to their names, the rest read and hashed, the content hash
// the gateway's, and the proof root recomputed from the leaves the gateway
// handed over with each piece. A provider in another process or on another
// host walks every closure.

// maxPre bounds the pieces whose leaves are kept (32 KiB a blob: 128 MiB
// for 16 GiB of content waiting for its entries), maxRoots the roots whose
// walk is kept (least recently used dropped first), maxCarried the roots
// marked carried, and rootBuffer the bytes each walk keeps of pieces that
// arrived ahead of it (one piece: the walk is fed from recent instead, see
// below).
//
// defaultRecentBytes is limits.closure_cache_mb's default, the bound on the
// pieces this provider has just stored that are kept for their walks (see
// recent): pieces taken at once reach their walk out of order, and the walk
// is handed each one with its hash (AddHashed) the moment it waits for it,
// so a kept piece is never hashed again. One not kept is read back from the
// store when the walk reaches it and hashed once, by the check of the
// walk's fetch (feed). About the pieces in flight at once: a feeder's takes
// (maxTakes) and the submissions processed (16), with room for the plane's
// reordering; a piece lost on the way holds every later piece of its root
// until it is sent again, which a larger cache rides out.
const (
	maxPre             = 4096
	maxRoots           = 64
	maxCarried         = 4096
	rootBuffer         = chirp.ChunkSize
	defaultRecentBytes = 32 * chirp.ChunkSize
)

type preBlob struct {
	size   uint64
	leaves []byte // 32 bytes a segment
	sub    [32]byte
}

// closureWalk is one root's walk and its fetch's wrong-bytes report. final
// is set by the evaluation's check: until then the walk's fetch declines a
// piece kept in recent, so that feed hands it over with its hash instead
// (see walk); from then on it serves whatever it holds, so Check never
// reports missing what is here.
type closureWalk struct {
	inc   *chirp.Incremental
	wrong func() bool
	// checked is the walk's own fetch from the store, each object held to
	// its hash (storerec.CheckedFetch, which wrong reports on): feed reads
	// a spilled piece through it once and hands it over with that hash.
	checked func([32]byte) ([]byte, error)
	final   atomic.Bool
}

// errNotYet is a walk's fetch declining a piece feed will hand it.
var errNotYet = errors.New("kept for the walk")

type incremental struct {
	mu    sync.Mutex
	pre   map[[32]byte]*preBlob
	fifo  [][32]byte
	walks map[[32]byte]*closureWalk
	wfifo [][32]byte

	// recent are pieces this provider has just stored, by hash, kept for
	// their roots' walks: the bytes it hashed to name the piece and wrote to
	// the store. feed hands each to its walk as the walk comes to wait for
	// it, with the hash this provider vouches for, so the walk neither reads
	// the store nor hashes the piece again; a walk started later (after a
	// restart, or for a root whose piece came after its blobs) is served
	// them by its fetch. Each is dropped when a walk takes it, when its root
	// is decided, or oldest first past recentMax bytes.
	recent    map[[32]byte]recentPiece
	rfifo     [][32]byte
	recentLen int
	recentMax int

	// carried are roots a gateway in this process staged, by the account
	// of a piece it handed over (carry): their closures are checked on that
	// account (checkClosure), not walked. Dropped when the root is decided,
	// oldest first past maxCarried.
	carried map[[32]byte]bool
	cfifo   [][32]byte

	// spilled are pieces dropped from recent for room, by hash, so feed
	// knows to have a walk waiting for one read it from the store rather
	// than wait on; oldest forgotten past maxSpilled.
	spilled map[[32]byte]bool
	sfifo   [][32]byte

	// feeding are pieces taken from recent by a feed that has not handed
	// them to the walk yet, so a walk another request is moving declines
	// them as it does a kept piece, rather than read them from the store.
	feeding map[[32]byte]bool
}

// maxSpilled bounds the spilled marks kept (32 bytes each).
const maxSpilled = 4096

type recentPiece struct {
	root [32]byte
	b    []byte
}

// newIncremental keeps up to recentMax bytes of pieces for their walks
// (defaultRecentBytes when 0 or less).
func newIncremental(recentMax int) *incremental {
	if recentMax <= 0 {
		recentMax = defaultRecentBytes
	}
	return &incremental{pre: map[[32]byte]*preBlob{}, walks: map[[32]byte]*closureWalk{}, recent: map[[32]byte]recentPiece{}, spilled: map[[32]byte]bool{}, feeding: map[[32]byte]bool{},
		recentMax: recentMax, carried: map[[32]byte]bool{}}
}

// keepRecent keeps a piece just stored for root's walk; in.mu is held.
func (in *incremental) keepRecent(h, root [32]byte, b []byte) {
	if _, ok := in.recent[h]; ok || len(b) > in.recentMax {
		return
	}
	in.recent[h] = recentPiece{root: root, b: b}
	in.rfifo = append(in.rfifo, h)
	in.recentLen += len(b)
	for in.recentLen > in.recentMax && len(in.rfifo) > 0 {
		h := in.rfifo[0]
		in.dropRecent(h)
		if !in.spilled[h] {
			in.spilled[h] = true
			in.sfifo = append(in.sfifo, h)
			for len(in.sfifo) > maxSpilled {
				delete(in.spilled, in.sfifo[0])
				in.sfifo = in.sfifo[1:]
			}
		}
	}
}

// pending reports whether a piece is kept, or on its way to its walk.
func (in *incremental) pending(h [32]byte) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	_, ok := in.recent[h]
	return ok || in.feeding[h]
}

// takeSpilled reports, once, that a piece was dropped from recent for room.
func (in *incremental) takeSpilled(h [32]byte) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.spilled[h] {
		return false
	}
	delete(in.spilled, h)
	in.sfifo = slices.DeleteFunc(in.sfifo, func(x [32]byte) bool { return x == h })
	return true
}

// dropRecent drops a kept piece; in.mu is held.
func (in *incremental) dropRecent(h [32]byte) {
	r, ok := in.recent[h]
	if !ok {
		return
	}
	delete(in.recent, h)
	in.recentLen -= len(r.b)
	in.rfifo = slices.DeleteFunc(in.rfifo, func(x [32]byte) bool { return x == h })
}

// takeRecent hands a kept piece to the walk that reads it, and drops it;
// with feeding set it is marked as on its way to the walk until fed.
func (in *incremental) takeRecent(h [32]byte, feeding bool) []byte {
	in.mu.Lock()
	defer in.mu.Unlock()
	r, ok := in.recent[h]
	if !ok {
		return nil
	}
	in.dropRecent(h)
	if feeding {
		in.feeding[h] = true
	}
	return r.b
}

// fed marks a piece handed to its walk.
func (in *incremental) fed(h [32]byte) {
	in.mu.Lock()
	delete(in.feeding, h)
	in.mu.Unlock()
}

func (in *incremental) blob(h [32]byte) *preBlob {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.pre[h]
}

// walk returns root's walk, made on demand over the store when create is
// true, or nil.
func (p *Provider) walk(root [32]byte, create bool) *closureWalk {
	in := p.inc
	in.mu.Lock()
	defer in.mu.Unlock()
	if w := in.walks[root]; w != nil {
		in.touch(root)
		return w
	}
	if !create {
		return nil
	}
	checked, wrong := storerec.CheckedFetch(p.fetch)
	w := &closureWalk{wrong: wrong, checked: checked}
	// A piece kept in recent is declined while the walk is fed (feed hands
	// it over with its hash, so the walk takes it without hashing it), and
	// served as it is once the walk is final: it is the bytes this provider
	// hashed to the name asked for, so the check would pass, and the walk
	// hashes what it fetches. Anything else is the store's.
	fetch := func(h [32]byte) ([]byte, error) {
		if w.final.Load() {
			if b := in.takeRecent(h, false); b != nil {
				return b, nil
			}
		} else if in.pending(h) {
			return nil, errNotYet
		}
		p.countWalkRead()
		return checked(h)
	}
	w.inc = chirp.NewIncremental(root, fetch, storerec.ClosureLimits, rootBuffer)
	in.walks[root] = w
	in.wfifo = append(in.wfifo, root)
	for len(in.wfifo) > maxRoots {
		delete(in.walks, in.wfifo[0])
		in.wfifo = in.wfifo[1:]
	}
	return w
}

// checkClosure is storerec.VerifyCHIRP's answer for root from its walk:
// only what the walk has not yet reached is read and hashed. A root a
// gateway in this process staged is checked on its account instead
// (storerec.VerifyCarried), with the blobs this provider holds as bytes
// hashed to their names (heldBlob) taken unread.
func (p *Provider) checkClosure(root [32]byte) (*chirp.Closure, error) {
	if p.inc.isCarried(root) {
		p.countCheck("carried")
		return storerec.VerifyCarried(root, p.fetch, p.heldBlob)
	}
	p.countCheck("walked")
	w := p.walk(root, true)
	p.feed(w)
	w.final.Store(true)
	c, err := w.inc.Check()
	if err != nil {
		return nil, storerec.ClosureError(err, w.wrong())
	}
	return c, nil
}

// touch moves root to the back of the eviction order; in.mu is held.
func (in *incremental) touch(root [32]byte) {
	for i, r := range in.wfifo {
		if r == root {
			in.wfifo = append(in.wfifo[:i], in.wfifo[i+1:]...)
			break
		}
	}
	in.wfifo = append(in.wfifo, root)
}

// arrived does a piece's share of the closure check: its leaves kept when
// it can be a blob, and the root's walk moved on. data hashes to h; leaves
// and sub are its segment leaves and subtree root (storerec.HashBlob), from
// the same pass; stored is true when this request wrote it to the store. A
// piece the store already held is not taken from the request: the walk
// reads the store's copy, hashing it, so a damaged copy is never vouched
// for by a good one sent again. A walk is started only by its root's own
// piece, so a walk dropped while its closure arrives is not started again
// by every later piece (each would read back what the store holds); the
// evaluation's check resumes it once. Any other piece is kept in recent,
// its root's walk there or not yet, and fed to the walk when it waits for
// it (feed).
//
// A carried piece (its gateway's account, TakeLocal) moves no walk: its
// root is marked carried, and its leaves are kept when this request stored
// it, the bytes its gateway hashed to h, for the carried check to take it
// unread and for the proof files.
func (p *Provider) arrived(h [32]byte, data, leaves []byte, sub [32]byte, root [32]byte, stored, carried bool) {
	in := p.inc
	if carried {
		in.mu.Lock()
		if stored && len(data) <= storerec.BlobSize {
			in.keepPre(h, &preBlob{size: uint64(len(data)), leaves: leaves, sub: sub})
		}
		in.carry(root)
		in.mu.Unlock()
		return
	}
	if !stored {
		if w := p.walk(root, false); w != nil {
			w.inc.Advance()
		}
		return
	}
	if len(data) <= storerec.BlobSize {
		in.mu.Lock()
		in.keepPre(h, &preBlob{size: uint64(len(data)), leaves: leaves, sub: sub})
		in.mu.Unlock()
	}
	w := p.walk(root, h == root)
	if h == root && w != nil {
		w.inc.AddHashed(h, data)
	} else {
		in.mu.Lock()
		in.keepRecent(h, root, data)
		in.mu.Unlock()
	}
	if w != nil {
		p.feed(w)
	}
}

// feed hands a walk every kept piece it waits for, in turn, each with the
// hash this provider computed for it, until it waits for one not kept: the
// walk takes a copy of each and moves on without hashing it again (its
// fetch declines a kept piece, so it waits rather than reads one). One
// dropped from recent for room is read back from the store here, through
// the walk's checked fetch, and handed over with the hash that check just
// made: the walk then takes it without hashing it a second time (it read
// it and hashed it twice before: the fetch's check, then its own). One
// that does not hash to its name is refused as the check always refused
// it (the walk's wrong reports it), and the walk reads it itself as before
// (Advance), so it waits for the piece rather than taking those bytes.
func (p *Provider) feed(w *closureWalk) {
	for {
		want, ok := w.inc.Waiting()
		if !ok {
			return
		}
		if b := p.inc.takeRecent(want, true); b != nil {
			w.inc.AddHashed(want, b)
			p.inc.fed(want)
		} else if p.inc.takeSpilled(want) {
			p.countWalkRead()
			if b, err := w.checked(want); err == nil {
				w.inc.AddHashed(want, b)
			} else {
				w.inc.Advance()
			}
		} else {
			return
		}
	}
}

// forget drops what was kept for a root once it is decided.
func (in *incremental) forget(root [32]byte, blobs []chirp.Child) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.walks[root] != nil {
		delete(in.walks, root)
		in.wfifo = slices.DeleteFunc(in.wfifo, func(r [32]byte) bool { return r == root })
	}
	for _, b := range blobs {
		delete(in.pre, b.Hash)
	}
	for _, h := range slices.Clone(in.rfifo) {
		if in.recent[h].root == root {
			in.dropRecent(h)
		}
	}
	if in.carried[root] {
		delete(in.carried, root)
		in.cfifo = slices.DeleteFunc(in.cfifo, func(r [32]byte) bool { return r == root })
	}
}

// keepPre keeps a stored piece's leaves, the first account of it kept;
// in.mu is held.
func (in *incremental) keepPre(h [32]byte, pb *preBlob) {
	if in.pre[h] != nil {
		return
	}
	in.pre[h] = pb
	in.fifo = append(in.fifo, h)
	for len(in.fifo) > maxPre {
		delete(in.pre, in.fifo[0])
		in.fifo = in.fifo[1:]
	}
}

// carry marks a root a gateway in this process staged; in.mu is held.
func (in *incremental) carry(root [32]byte) {
	if in.carried[root] {
		return
	}
	in.carried[root] = true
	in.cfifo = append(in.cfifo, root)
	for len(in.cfifo) > maxCarried {
		delete(in.carried, in.cfifo[0])
		in.cfifo = in.cfifo[1:]
	}
}

// markCarried marks a root a gateway in this process staged.
func (in *incremental) markCarried(root [32]byte) {
	in.mu.Lock()
	in.carry(root)
	in.mu.Unlock()
}

// isCarried reports whether a root's closure is checked on its gateway's
// account.
func (in *incremental) isCarried(root [32]byte) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.carried[root]
}

// heldBlob reports a blob a carried check takes unread: one this provider
// stored, in this process, from bytes hashed to its name (by itself or by
// its gateway: its leaves are kept, see arrived), still held at the
// reference's length.
func (p *Provider) heldBlob(c chirp.Child) bool {
	pb := p.inc.blob(c.Hash)
	return pb != nil && pb.size == c.Length && p.store.Size(c.Hash) == int64(c.Length)
}

// countCheck counts a closure check by how it was made.
func (p *Provider) countCheck(mode string) {
	if p.metrics != nil {
		p.metrics.inc("bstore_provider_closure_checks_total", "mode="+mode)
	}
}

// countWalkRead counts an object a walk read back from the store: a piece
// past the recent cache, or one a walk started on demand reads.
func (p *Provider) countWalkRead() {
	if p.metrics != nil {
		p.metrics.inc("bstore_provider_walk_reads_total")
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
