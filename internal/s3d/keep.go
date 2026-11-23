package s3d

import (
	"encoding/hex"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
)

// keepStore is the local store as a reader.Keeper (Config.KeepFetched): an
// object a read fetched from an advertiser is kept, and a closure held
// whole is recorded by its manifest, so Trim treats it as any other closure
// (least recently used first) instead of as what a cut-short put left.
type keepStore struct{ *publish.Store }

func (k keepStore) Closure(root [32]byte, size uint64, branches, blobs [][32]byte) error {
	m := &publish.Manifest{Root: hex.EncodeToString(root[:]), Size: size}
	for _, h := range branches {
		m.Branches = append(m.Branches, hex.EncodeToString(h[:]))
	}
	for _, h := range blobs {
		m.Blobs = append(m.Blobs, hex.EncodeToString(h[:]))
	}
	return k.SaveManifest(m)
}

// readLocal is the local copy a read tries first.
func (s *Server) readLocal() reader.Local {
	if s.cfg.KeepFetched {
		return keepStore{s.store}
	}
	return s.store
}
