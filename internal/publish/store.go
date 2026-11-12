package publish

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
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
	// a busy root. pins counts the stages using each object, so an object
	// a stage wrote, or found already held, is never removed under it.
	busy map[[32]byte]int
	pins map[[32]byte]int
	// staged counts the stages that wrote or found each object in this
	// process: only those are read without hashing them again.
	staged map[[32]byte]int
	// fresh counts the pins taken on an object while a stage of this
	// process still marked it (pinFresh): the outbox's sends of a put's
	// pieces after the put answered read them without hashing again, as
	// the stage itself would have. A pin taken on bytes another process or
	// an earlier run left (an outbox reloaded after a restart) is not fresh,
	// and those bytes are held to their hash.
	fresh map[[32]byte]int
	// leaves are the segment leaves of an object a stage of this process
	// wrote, from the pass that hashed it (32 bytes a segment), kept while
	// the object is staged or fresh: the provider in this process takes
	// them with the piece instead of hashing its segments again.
	// leavesBytes is their total, held to maxLeavesBytes.
	leaves      map[[32]byte][]byte
	leavesBytes int
	// grown is the bytes written into the store since the last Trim began
	// (Grown): what a caller watches to trim before its usual time.
	grown atomic.Int64
}

// maxLeavesBytes bounds the segment leaves a store keeps (keepLeaves): 128
// MiB is the leaves of 16 GiB of content staged or waiting to be sent. A
// blob past it keeps none, and the provider in this process hashes its
// segments itself, as for a blob the stage found already held.
var maxLeavesBytes = 128 << 20

// orphanAge is how long an object no closure names is kept: what a put cut
// short left behind.
const orphanAge = time.Hour

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
	return &Store{dir: dir, busy: map[[32]byte]int{}, pins: map[[32]byte]int{}, staged: map[[32]byte]int{}, fresh: map[[32]byte]int{},
		leaves: map[[32]byte][]byte{}}, nil
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
	h, _, err := s.put(b, false)
	return h, err
}

// put stores an object and, with pin, pins it in the same step, so Trim
// cannot remove an object a stage found already held.
func (s *Store) put(b []byte, pin bool) ([32]byte, bool, error) {
	return s.putHashed(sha256.Sum256(b), b, pin)
}

// putHashed is put of bytes whose SHA-256 the caller just computed.
func (s *Store) putHashed(h [32]byte, b []byte, pin bool) ([32]byte, bool, error) {
	p := s.objectPath(h)
	s.mu.Lock()
	if pin {
		s.pins[h]++
	}
	_, err := os.Stat(p)
	s.mu.Unlock()
	if err == nil {
		now := time.Now()
		_ = os.Chtimes(p, now, now)
		return h, false, nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return h, false, err
	}
	if err := writeFileSync(filepath.Join(s.dir, "tmp"), p, b); err != nil {
		return h, false, err
	}
	s.grown.Add(int64(len(b)))
	if pin {
		// Written by this stage from the bytes it hashed: read back
		// without hashing again while the stage holds it.
		s.mu.Lock()
		s.staged[h]++
		s.mu.Unlock()
	}
	return h, pin, nil
}

// unstage releases a stage's mark on the objects it wrote.
func (s *Store) unstage(hs [][32]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hs {
		if s.staged[h]--; s.staged[h] <= 0 {
			delete(s.staged, h)
			s.dropLeavesLocked(h)
		}
	}
}

// keepLeaves keeps an object's segment leaves while a stage of this
// process marks it (it wrote the object from the bytes it hashed); an
// object found already held, no longer marked, or past maxLeavesBytes
// keeps none.
func (s *Store) keepLeaves(h [32]byte, leaves []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.staged[h] > 0 && s.leaves[h] == nil && s.leavesBytes+len(leaves) <= maxLeavesBytes {
		s.leaves[h] = leaves
		s.leavesBytes += len(leaves)
	}
}

// leavesOf returns the segment leaves kept for an object, or nil (and nil
// without a store).
func (s *Store) leavesOf(h [32]byte) []byte {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaves[h]
}

// dropLeavesLocked drops an object's leaves once neither a stage nor a
// fresh pin marks it. The caller holds mu.
func (s *Store) dropLeavesLocked(h [32]byte) {
	if s.staged[h] <= 0 && s.fresh[h] <= 0 {
		s.deleteLeavesLocked(h)
	}
}

// deleteLeavesLocked drops an object's leaves from the total. The caller
// holds mu.
func (s *Store) deleteLeavesLocked(h [32]byte) {
	if l, ok := s.leaves[h]; ok {
		s.leavesBytes -= len(l)
		delete(s.leaves, h)
	}
}

// unpin releases a stage's pins.
func (s *Store) unpin(hs [][32]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range hs {
		if s.pins[h]--; s.pins[h] <= 0 {
			delete(s.pins, h)
		}
	}
}

// remove deletes an object unless a stage pins it; it returns the bytes
// freed.
func (s *Store) remove(h [32]byte) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pins[h] > 0 {
		return 0
	}
	info, err := os.Stat(s.objectPath(h))
	if err != nil || os.Remove(s.objectPath(h)) != nil {
		return 0
	}
	s.forgetLocked(h)
	return info.Size()
}

// forgetLocked drops what this process knew of an object's bytes once its
// file is gone: bytes written under the same name later (a stage's write
// marks them again; a part linked in does not) are held to their hash. The
// caller holds mu.
func (s *Store) forgetLocked(h [32]byte) {
	delete(s.fresh, h)
	delete(s.staged, h)
	s.deleteLeavesLocked(h)
}

// drop removes an object that did not hash to its name, and what this
// process knew of it.
func (s *Store) drop(h [32]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = os.Remove(s.objectPath(h))
	s.forgetLocked(h)
}

// Get reads an object and holds it to its hash: bytes that went bad on disk
// are never returned.
func (s *Store) Get(h [32]byte) ([]byte, error) {
	b, err := os.ReadFile(s.objectPath(h))
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(b) != h {
		s.drop(h)
		return nil, fmt.Errorf("publish: stored object %x does not hash to its name", h[:6])
	}
	return b, nil
}

// readStaged reads an object a stage of this process wrote or found and
// still holds, without
// holding it to its hash again: Stage hashed the bytes as it wrote them,
// moments before. Anything else is read through Get.
func (s *Store) readStaged(h [32]byte) ([]byte, error) {
	s.mu.Lock()
	fresh := s.staged[h] > 0
	s.mu.Unlock()
	if !fresh {
		return s.Get(h)
	}
	return os.ReadFile(s.objectPath(h))
}

// readInto reads an object into buf (grown when too small), held to its
// hash unless fresh: one a stage of this process wrote and still holds, or
// one a fresh pin holds (pinFresh), was hashed as it was written. With
// fresh false it is held to its hash whatever marks it.
func (s *Store) readInto(h [32]byte, fresh bool, buf []byte) ([]byte, error) {
	if fresh {
		s.mu.Lock()
		fresh = s.staged[h] > 0 || s.fresh[h] > 0
		s.mu.Unlock()
	}
	f, err := os.Open(s.objectPath(h))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	n := int(fi.Size())
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	if !fresh && sha256.Sum256(buf) != h {
		s.drop(h)
		return nil, fmt.Errorf("publish: stored object %x does not hash to its name", h[:6])
	}
	return buf, nil
}

// pin keeps an object from Trim while something refers to it (an outbox
// item that sends it by reference); unpinOne releases one pin.
func (s *Store) pin(h [32]byte) {
	s.mu.Lock()
	s.pins[h]++
	s.mu.Unlock()
}

func (s *Store) unpinOne(h [32]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pins[h]--; s.pins[h] <= 0 {
		delete(s.pins, h)
	}
}

// pinFresh is pin that also carries the object's freshness when a stage of
// this process marks it now, or another fresh pin does: it reports whether
// it did, for unpinFresh.
func (s *Store) pinFresh(h [32]byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pins[h]++
	if s.staged[h] > 0 || s.fresh[h] > 0 {
		s.fresh[h]++
		return true
	}
	return false
}

// unpinFresh releases a pin pinFresh took.
func (s *Store) unpinFresh(h [32]byte, fresh bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pins[h]--; s.pins[h] <= 0 {
		delete(s.pins, h)
	}
	// Freshness the object lost with its file (forgetLocked) is not taken
	// from a pin made since: at worst that pin's bytes are hashed again.
	if fresh && s.fresh[h] > 0 {
		if s.fresh[h]--; s.fresh[h] <= 0 {
			delete(s.fresh, h)
			s.dropLeavesLocked(h)
		}
	}
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

// Grown is the bytes written into the store since the last Trim began. A
// store is trimmed on a clock; under a heavy ingest it can take in many
// times its bound between two trims, and a caller that sees Grown pass a
// share of the bound trims then instead of waiting.
func (s *Store) Grown() int64 { return s.grown.Load() }

// Trim removes objects no closure names that are older than orphanAge
// (what a put cut short left), then the least recently used closures
// until the objects held take at most max bytes; never a busy closure, a
// pinned object, or an object another remaining closure names. It returns
// the bytes held after.
func (s *Store) Trim(max int64) (int64, error) {
	s.grown.Store(0)
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
	// Orphans.
	cut := time.Now().Add(-orphanAge)
	_ = filepath.WalkDir(filepath.Join(s.dir, "objects"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		h, perr := parseHash(d.Name())
		if perr != nil || names[h] > 0 {
			return nil
		}
		if info, err := d.Info(); err == nil && info.ModTime().Before(cut) {
			s.remove(h)
		}
		return nil
	})
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
			total -= s.remove(o)
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
// then renamed into place, the rename synced: one write of a group commit
// (commit.go).
func writeFileSync(tmp, path string, b []byte) error {
	return commits.write(tmp, path, b)
}
