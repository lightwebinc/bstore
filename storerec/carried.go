package storerec

import (
	"crypto/sha256"
	"fmt"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"
)

// VerifyCarried is VerifyCHIRP for a closure whose blobs the caller holds
// on its own account: held reports a blob reference whose bytes the caller
// holds, at the reference's length, as bytes hashed to the reference's
// name before they were stored, as a storage node's provider holds the
// pieces its own gateway staged and handed it in process (the owner's
// ruling of 2026-11-08). Such a blob is not read. The rest is
// VerifyCHIRP's: the root and every branch are fetched and hashed before
// any field of theirs is read, every node check, length, depth, cycle,
// reference bound and the canonical construction is applied, and a blob
// held does not report is fetched and held to its length and its hash.
//
// The content hash is not recomputed: it is the root's, and the call is
// only for a root the caller's account covers (the gateway computed that
// content hash over the bytes it staged and handed over). Refusals are
// labelled as VerifyCHIRP labels them.
func VerifyCarried(rootHash [32]byte, fetch chirp.Fetch, held func(chirp.Child) bool) (*chirp.Closure, error) {
	c, err := verifyCarried(rootHash, fetch, held, ClosureLimits)
	if err != nil {
		return nil, ClosureError(err, false)
	}
	return c, nil
}

// verifyCarried is bcommon chirp.Verify with held blobs taken unread and
// no content hash: the same steps, in the same order, with the same errors.
func verifyCarried(rootHash [32]byte, fetch chirp.Fetch, held func(chirp.Child) bool, lim chirp.Limits) (*chirp.Closure, error) {
	if lim.MaxReferences <= 0 {
		lim.MaxReferences = chirp.DefaultMaxReferences
	}
	rb, err := fetchObject(fetch, rootHash)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(rb) != rootHash {
		return nil, fmt.Errorf("%w: the root", chirp.ErrHash)
	}
	root, err := chirp.DecodeRoot(rb)
	if err != nil {
		return nil, err
	}
	if lim.MaxLength > 0 && root.Length > lim.MaxLength {
		return nil, fmt.Errorf("%w: %d bytes", chirp.ErrTooLarge, root.Length)
	}
	out := &chirp.Closure{Root: root, RootHash: rootHash, Objects: [][32]byte{rootHash}}
	w := carriedWalk{
		fetch:    fetch,
		held:     held,
		lim:      lim,
		seen:     map[[32]byte]bool{rootHash: true},
		branches: map[[32]byte]chirp.Branch{},
		ancestry: map[[32]byte]bool{rootHash: true},
		out:      out,
	}
	if err := w.children(root.Children, 1); err != nil {
		return nil, err
	}
	if root.Profile == chirp.Profile1 {
		built, err := chirp.BuildTree(out.Blobs, root.ContentHash, root.Extensions)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", chirp.ErrCanonical, err)
		}
		if built.RootHash != rootHash {
			return nil, fmt.Errorf("%w: another grouping of the same blobs", chirp.ErrCanonical)
		}
		out.Canonical = true
	}
	return out, nil
}

func fetchObject(fetch chirp.Fetch, h [32]byte) ([]byte, error) {
	b, err := fetch(h)
	if err != nil {
		return nil, fmt.Errorf("%w: %x: %v", chirp.ErrMissing, h, err)
	}
	return b, nil
}

type carriedWalk struct {
	fetch    chirp.Fetch
	held     func(chirp.Child) bool
	lim      chirp.Limits
	refs     int
	seen     map[[32]byte]bool
	branches map[[32]byte]chirp.Branch
	ancestry map[[32]byte]bool
	out      *chirp.Closure
}

// children walks one node's child list; depth is the number of nodes above
// the children, the root counted.
func (w *carriedWalk) children(list []chirp.Child, depth int) error {
	for _, c := range list {
		w.refs++
		if w.refs > w.lim.MaxReferences {
			return chirp.ErrReferences
		}
		if depth+1 > chirp.MaxDepth {
			return chirp.ErrDepth
		}
		if c.Kind == chirp.ChildBranch {
			if err := w.branch(c, depth); err != nil {
				return err
			}
			continue
		}
		if err := w.blob(c); err != nil {
			return err
		}
	}
	return nil
}

func (w *carriedWalk) branch(c chirp.Child, depth int) error {
	if w.ancestry[c.Hash] {
		return chirp.ErrCycle
	}
	br, ok := w.branches[c.Hash]
	if !ok {
		b, err := fetchObject(w.fetch, c.Hash)
		if err != nil {
			return err
		}
		if sha256.Sum256(b) != c.Hash {
			return fmt.Errorf("%w: branch %x", chirp.ErrHash, c.Hash)
		}
		if br, err = chirp.DecodeBranch(b); err != nil {
			return err
		}
		w.branches[c.Hash] = br
	}
	if br.Length != c.Length {
		return fmt.Errorf("%w: branch %x covers %d bytes, its reference %d", chirp.ErrLength, c.Hash, br.Length, c.Length)
	}
	w.note(c.Hash)
	w.ancestry[c.Hash] = true
	err := w.children(br.Children, depth+1)
	delete(w.ancestry, c.Hash)
	return err
}

// blob takes a held blob as it is, and reads and checks any other.
func (w *carriedWalk) blob(c chirp.Child) error {
	if !w.held(c) {
		b, err := fetchObject(w.fetch, c.Hash)
		if err != nil {
			return err
		}
		if uint64(len(b)) != c.Length {
			return fmt.Errorf("%w: blob %x is %d bytes, its reference %d", chirp.ErrLength, c.Hash, len(b), c.Length)
		}
		if sha256.Sum256(b) != c.Hash {
			return fmt.Errorf("%w: blob %x", chirp.ErrHash, c.Hash)
		}
	}
	w.out.Blobs = append(w.out.Blobs, c)
	w.note(c.Hash)
	return nil
}

func (w *carriedWalk) note(h [32]byte) {
	if !w.seen[h] {
		w.seen[h] = true
		w.out.Objects = append(w.out.Objects, h)
	}
}

// RootOfLeaves is the subtree root over a blob's segment leaves (32 bytes
// a segment, in order, as LeavesOf returns them): the blob's share of a
// proof root, recomputed from the leaves alone, without the blob.
func RootOfLeaves(leaves []byte) [32]byte {
	var b commit.Builder
	for i := 0; i+32 <= len(leaves); i += 32 {
		b.Add([32]byte(leaves[i : i+32]))
	}
	return b.Root()
}

// LeavesLen is the length of a blob's segment leaves: 32 bytes for each
// SegmentSize segment of n bytes, the last one short or not.
func LeavesLen(n int) int { return 32 * ((n + SegmentSize - 1) / SegmentSize) }
