package s3d

import (
	"encoding/hex"
	"sync"
	"time"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
)

// keepStore is the local store as a reader.Keeper (Config.KeepFetched): an
// object a read fetched from an advertiser is kept, and a closure held
// whole is recorded by its manifest, so Trim treats it as any other closure
// (least recently used first) instead of as what a cut-short put left.
type keepStore struct {
	*publish.Store
	mu      sync.Mutex
	touched map[[32]byte]time.Time
}

// touchEvery is how often a read marks a closure it found whole as used.
// Trim orders closures by that mark; once a minute is close enough for it
// and keeps a busy object's reads off the disk's metadata.
const touchEvery = time.Minute

func (k *keepStore) Held(h [32]byte) bool { return k.Has(h) }

// Closure records a closure held whole. One the store already has a
// manifest for (this gateway wrote it, or an earlier read completed it) is
// marked as used instead, at most once in touchEvery.
func (k *keepStore) Closure(root [32]byte, size uint64, branches, blobs [][32]byte) error {
	now := time.Now()
	k.mu.Lock()
	if at, ok := k.touched[root]; ok && now.Sub(at) < touchEvery {
		k.mu.Unlock()
		return nil
	}
	if k.touched == nil || len(k.touched) >= 65536 {
		k.touched = map[[32]byte]time.Time{}
	}
	k.touched[root] = now
	k.mu.Unlock()
	// Manifest reads the manifest and marks it used.
	if _, err := k.Manifest(root); err == nil {
		return nil
	}
	m := &publish.Manifest{Root: hex.EncodeToString(root[:]), Size: size}
	for _, h := range branches {
		m.Branches = append(m.Branches, hex.EncodeToString(h[:]))
	}
	for _, h := range blobs {
		m.Blobs = append(m.Blobs, hex.EncodeToString(h[:]))
	}
	if err := k.SaveManifest(m); err != nil {
		k.mu.Lock()
		delete(k.touched, root)
		k.mu.Unlock()
		return err
	}
	return nil
}

// readLocal is the local copy a read tries first.
func (s *Server) readLocal() reader.Local {
	if s.keep != nil {
		return s.keep
	}
	return s.store
}
