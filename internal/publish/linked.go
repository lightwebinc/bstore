package publish

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/storerec"
)

// HashBlob is what Stage computes of each blob: its SHA-256, which names
// it, and its segment subtree root, which the proof root is composed of.
func HashBlob(b []byte) (h, seg [32]byte, err error) {
	return sha256.Sum256(b), storerec.SegmentRootOf(b), nil
}

// LinkedBlob is one blob of content already on disk outside the store, as
// a multipart part's blobs are kept: the file at Path holds exactly the
// blob, Hash and SegRoot are HashBlob of it.
type LinkedBlob struct {
	Path    string
	Hash    [32]byte
	SegRoot [32]byte
	Length  uint64
	// Fresh marks a blob this process hashed from the bytes it wrote at
	// Path: it is read back without hashing again while the stage holds
	// it, as Stage's own blobs are.
	Fresh bool
}

// StageLinked is Stage of content whose blobs are already on disk, hashed:
// blobs in content order (every one ChunkSize bytes but the last) and
// contentHash the SHA-256 of the whole content. Each blob file is linked
// into the store (copied where it cannot be), not read; the closure, the
// proof root and the manifest are what Stage returns for the same content,
// byte for byte.
func (st *Store) StageLinked(ctx context.Context, blobs []LinkedBlob, contentHash [32]byte) (*Staged, error) {
	refs := make([]chirp.Child, len(blobs))
	var subs commit.SegmentRoots
	var size uint64
	for i, b := range blobs {
		refs[i] = chirp.Child{Kind: chirp.ChildBlob, Length: b.Length, Hash: b.Hash}
		subs.Set(i, b.SegRoot)
		size += b.Length
	}
	if size > storerec.MaxSize {
		return nil, fmt.Errorf("%w: more than %d bytes", ErrTooLarge, uint64(storerec.MaxSize))
	}
	built, err := chirp.BuildTree(refs, contentHash, nil)
	if err != nil {
		return nil, err
	}
	proof, err := subs.Root()
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	var pinned, wrote [][32]byte
	ok := false
	defer func() {
		if !ok {
			st.unpin(pinned)
			st.unstage(wrote)
		}
	}()
	dirs := map[string]bool{}
	seen := map[[32]byte]bool{}
	for _, b := range blobs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pinned = append(pinned, b.Hash)
		if seen[b.Hash] {
			st.pin(b.Hash)
			continue
		}
		seen[b.Hash] = true
		linked, err := st.link(b)
		if err != nil {
			return nil, err
		}
		if linked {
			dirs[filepath.Dir(st.objectPath(b.Hash))] = true
			if b.Fresh {
				st.mu.Lock()
				st.staged[b.Hash]++
				st.mu.Unlock()
				wrote = append(wrote, b.Hash)
			}
		}
	}
	// A link is a directory entry: each directory a blob was linked into is
	// synced once, several at a time.
	if err := syncDirs(dirs); err != nil {
		return nil, err
	}
	put := func(h [32]byte, b []byte) error {
		h, w, err := st.putHashed(h, b, true)
		mu.Lock()
		pinned = append(pinned, h)
		if w {
			wrote = append(wrote, h)
		}
		mu.Unlock()
		return err
	}
	if err := put(built.RootHash, built.Root); err != nil {
		return nil, err
	}
	m := &Manifest{Root: hex.EncodeToString(built.RootHash[:]), Size: size}
	for _, b := range blobs {
		m.Blobs = append(m.Blobs, hex.EncodeToString(b.Hash[:]))
	}
	for _, br := range built.Branches {
		h := sha256.Sum256(br)
		if err := put(h, br); err != nil {
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
	return &Staged{Root: built.RootHash, ContentHash: contentHash, ProofRoot: proof, Size: size,
		Manifest: m, release: release}, nil
}

// link pins a blob and, unless the store holds it already, links its file
// in (or copies it, across file systems). It reports whether it added the
// object; the caller syncs the directory.
func (st *Store) link(b LinkedBlob) (bool, error) {
	p := st.objectPath(b.Hash)
	st.mu.Lock()
	st.pins[b.Hash]++
	_, err := os.Stat(p)
	st.mu.Unlock()
	if err == nil {
		now := time.Now()
		_ = os.Chtimes(p, now, now)
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return false, err
	}
	err = os.Link(b.Path, p)
	switch {
	case err == nil:
		if info, serr := os.Stat(p); serr == nil {
			st.grown.Add(info.Size())
		}
		return true, nil
	case errors.Is(err, os.ErrExist):
		return false, nil
	}
	// Not linkable (another file system): copy, held to its hash.
	raw, rerr := os.ReadFile(b.Path)
	if rerr != nil {
		return false, rerr
	}
	if uint64(len(raw)) != b.Length || sha256.Sum256(raw) != b.Hash {
		return false, fmt.Errorf("publish: blob file %s does not hash to %x", filepath.Base(b.Path), b.Hash[:6])
	}
	if err := writeFileSync(filepath.Join(st.dir, "tmp"), p, raw); err != nil {
		return false, err
	}
	st.grown.Add(int64(len(raw)))
	return true, nil
}

// syncDirs syncs each directory, up to 16 at once.
func syncDirs(dirs map[string]bool) error {
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for d := range dirs {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			f, err := os.Open(d)
			if err == nil {
				err = f.Sync()
				f.Close()
			}
			if err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return first
}
