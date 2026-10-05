package publish

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Store is the publisher's local copy of the closures it built: each CHIRP
// object under its SHA-256, and per root a manifest naming its objects. A
// gateway reads its own writes from it before any provider advertises them
// (spec section 4.4), a cross-bucket copy republishes from it, and a put
// retried after a crash finds its pieces here. It is a cache: Trim removes
// the least recently used closures beyond a bound, and nothing is lost when
// it does, because the closure's providers hold it.
//
// Every path is made from a hash's hex digits alone.
type Store struct {
	dir string
	mu  sync.Mutex
	// busy counts the readers and writers of each root; Trim never removes
	// a busy root.
	busy map[[32]byte]int
}

// Manifest is a stored closure: the root and branch nodes and every blob,
// by hash, in closure order (repeats included for blobs).
type Manifest struct {
	Root     string   `json:"root"`
	Size     uint64   `json:"size"`
	Branches []string `json:"branches,omitempty"`
	Blobs    []string `json:"blobs,omitempty"`
}

// OpenStore opens (creating) a store under dir.
func OpenStore(dir string) (*Store, error) {
	for _, d := range []string{"objects", "roots", "tmp"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			return nil, err
		}
	}
	// Anything left in tmp is a write a stop cut short.
	if ents, err := os.ReadDir(filepath.Join(dir, "tmp")); err == nil {
		for _, e := range ents {
			_ = os.Remove(filepath.Join(dir, "tmp", e.Name()))
		}
	}
	return &Store{dir: dir, busy: map[[32]byte]int{}}, nil
}

func (s *Store) objectPath(h [32]byte) string {
	x := hex.EncodeToString(h[:])
	return filepath.Join(s.dir, "objects", x[:2], x)
}

func (s *Store) manifestPath(root [32]byte) string {
	return filepath.Join(s.dir, "roots", hex.EncodeToString(root[:])+".json")
}

// Put stores an object under its hash, durably; an object already held is
// left alone.
func (s *Store) Put(b []byte) ([32]byte, error) {
	h := sha256.Sum256(b)
	p := s.objectPath(h)
	if _, err := os.Stat(p); err == nil {
		now := time.Now()
		_ = os.Chtimes(p, now, now)
		return h, nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return h, err
	}
	return h, writeFileSync(filepath.Join(s.dir, "tmp"), p, b)
}

// Get reads an object and holds it to its hash: bytes that went bad on disk
// are never returned.
func (s *Store) Get(h [32]byte) ([]byte, error) {
	b, err := os.ReadFile(s.objectPath(h))
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(b) != h {
		_ = os.Remove(s.objectPath(h))
		return nil, fmt.Errorf("publish: stored object %x does not hash to its name", h[:6])
	}
	return b, nil
}

// Has reports whether an object is held.
func (s *Store) Has(h [32]byte) bool {
	_, err := os.Stat(s.objectPath(h))
	return err == nil
}

// SaveManifest records a closure, durably.
func (s *Store) SaveManifest(m *Manifest) error {
	root, err := parseHash(m.Root)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeFileSync(filepath.Join(s.dir, "tmp"), s.manifestPath(root), raw)
}

// Manifest reads a closure's manifest; it touches the closure as used.
func (s *Store) Manifest(root [32]byte) (*Manifest, error) {
	p := s.manifestPath(root)
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if m.Root != hex.EncodeToString(root[:]) {
		return nil, errors.New("publish: manifest names another root")
	}
	now := time.Now()
	_ = os.Chtimes(p, now, now)
	return &m, nil
}

// Hold marks root busy until the returned function is called.
func (s *Store) Hold(root [32]byte) func() {
	s.mu.Lock()
	s.busy[root]++
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		if s.busy[root]--; s.busy[root] <= 0 {
			delete(s.busy, root)
		}
		s.mu.Unlock()
	}
}

// Trim removes the least recently used closures until the objects held
// take at most max bytes, never a busy one, and never an object another
// remaining closure names. It returns the bytes held after.
func (s *Store) Trim(max int64) (int64, error) {
	ents, err := os.ReadDir(filepath.Join(s.dir, "roots"))
	if err != nil {
		return 0, err
	}
	type rootAt struct {
		root [32]byte
		at   time.Time
		m    *Manifest
	}
	var roots []rootAt
	names := map[[32]byte]int{}
	for _, e := range ents {
		h, err := parseHash(stripExt(e.Name()))
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(s.manifestPath(h))
		if err != nil {
			continue
		}
		var m Manifest
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		roots = append(roots, rootAt{h, info.ModTime(), &m})
		for _, o := range objectsOf(&m) {
			names[o]++
		}
	}
	total := s.bytes()
	if total <= max {
		return total, nil
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].at.Before(roots[j].at) })
	for _, r := range roots {
		if total <= max {
			break
		}
		s.mu.Lock()
		busy := s.busy[r.root] > 0
		s.mu.Unlock()
		if busy {
			continue
		}
		_ = os.Remove(s.manifestPath(r.root))
		for _, o := range objectsOf(r.m) {
			if names[o]--; names[o] > 0 {
				continue
			}
			if info, err := os.Stat(s.objectPath(o)); err == nil {
				if os.Remove(s.objectPath(o)) == nil {
					total -= info.Size()
				}
			}
		}
	}
	return total, nil
}

func (s *Store) bytes() int64 {
	var n int64
	_ = filepath.WalkDir(filepath.Join(s.dir, "objects"), func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				n += info.Size()
			}
		}
		return nil
	})
	return n
}

// objectsOf is a manifest's distinct objects: the root, the branches, then
// the blobs.
func objectsOf(m *Manifest) [][32]byte {
	seen := map[[32]byte]bool{}
	var out [][32]byte
	add := func(s string) {
		if h, err := parseHash(s); err == nil && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	add(m.Root)
	for _, b := range m.Branches {
		add(b)
	}
	for _, b := range m.Blobs {
		add(b)
	}
	return out
}

func stripExt(name string) string {
	if i := len(name) - len(filepath.Ext(name)); i > 0 {
		return name[:i]
	}
	return name
}

func parseHash(s string) ([32]byte, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return [32]byte{}, fmt.Errorf("publish: %q is not 64 hex digits", s)
	}
	return [32]byte(b), nil
}

// writeFileSync writes b to path through a temporary file in tmp, synced,
// then renamed into place, and syncs the directory.
func writeFileSync(tmp, path string, b []byte) error {
	f, err := os.CreateTemp(tmp, "w-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}
