package publish

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/lightwebinc/bstore/storerec"
)

// ErrTooLarge is a body over the bound Stage was given.
var ErrTooLarge = errors.New("publish: the content is over the bound")

// Staged is content built into a CHIRP Profile 1 closure (no extension on
// the root) and its proof root, every object of it in the store. Nothing is
// published yet.
type Staged struct {
	Root        [32]byte
	ContentHash [32]byte
	ProofRoot   [32]byte
	Size        uint64
	// Manifest is the closure as the store keeps it.
	Manifest *Manifest
	release  func()
}

// Release lets the store trim the closure again.
func (s *Staged) Release() {
	if s != nil && s.release != nil {
		s.release()
		s.release = nil
	}
}

// Stage reads content from r to its end, at most max bytes, building the
// closure and the proof root as it streams: each 4 MiB blob goes to the
// store as soon as it is complete, so memory holds one blob and one
// segment whatever the content's size (spec section 16.2). An error from r,
// or content over max (ErrTooLarge), leaves nothing to publish.
func (st *Store) Stage(ctx context.Context, r io.Reader, max uint64) (*Staged, error) {
	if max > storerec.MaxSize {
		max = storerec.MaxSize
	}
	var blobs []string
	var pinned, wrote [][32]byte
	ok := false
	defer func() {
		if !ok {
			st.unpin(pinned)
			st.unstage(wrote)
		}
	}()
	put := func(b []byte) ([32]byte, error) {
		h, w, err := st.put(b, true)
		pinned = append(pinned, h)
		if w {
			wrote = append(wrote, h)
		}
		return h, err
	}
	b := storerec.NewStreaming(func(blob []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, err := put(blob)
		if err != nil {
			return err
		}
		blobs = append(blobs, hex.EncodeToString(h[:]))
		return nil
	})
	buf := make([]byte, 256<<10)
	var n uint64
	for {
		k, err := r.Read(buf)
		if k > 0 {
			n += uint64(k)
			if n > max {
				return nil, fmt.Errorf("%w: more than %d bytes", ErrTooLarge, max)
			}
			if _, werr := b.Write(buf[:k]); werr != nil {
				return nil, werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	built, err := b.Finish()
	if err != nil {
		return nil, err
	}
	if _, err := put(built.Root); err != nil {
		return nil, err
	}
	m := &Manifest{Root: hex.EncodeToString(built.RootHash[:]), Size: built.Size, Blobs: blobs}
	for _, br := range built.Branches {
		h, err := put(br)
		if err != nil {
			return nil, err
		}
		m.Branches = append(m.Branches, hex.EncodeToString(h[:]))
	}
	hold := st.Hold(built.RootHash)
	if err := st.SaveManifest(m); err != nil {
		hold()
		return nil, err
	}
	ok = true
	release := func() {
		hold()
		st.unpin(pinned)
		st.unstage(wrote)
	}
	return &Staged{Root: built.RootHash, ContentHash: built.ContentHash, ProofRoot: built.ProofRoot, Size: built.Size,
		Manifest: m, release: release}, nil
}

// Closure is a closure's objects, wherever they come from: the store for
// what the publisher built, a verified reader for a copy across buckets.
type Closure interface {
	// Objects are the closure's distinct objects: the root first, then the
	// branches, then the blobs.
	Objects() [][32]byte
	// Object returns one of them, its bytes held to its hash.
	Object(ctx context.Context, h [32]byte) ([]byte, error)
}

// StoreClosure is a stored closure as a Closure. Staged marks a closure
// this process staged in the same request, still pinned: its objects are
// read without hashing them a second time.
type StoreClosure struct {
	Store    *Store
	Manifest *Manifest
	Staged   bool
}

func (c StoreClosure) Objects() [][32]byte { return objectsOf(c.Manifest) }

func (c StoreClosure) Object(_ context.Context, h [32]byte) ([]byte, error) {
	if c.Staged {
		return c.Store.readStaged(h)
	}
	return c.Store.Get(h)
}
