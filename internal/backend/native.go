package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// MaxObject is the largest CHIRP object a native store holds: a Profile 1
// blob of 4 MiB. A node is at most 65,536 bytes.
const MaxObject = 4 << 20

// Native is the native backend: a content-addressed store on local disk,
// one file per object at objects/<first two hex digits>/<64 hex digits>,
// written whole to a temporary file, synced and renamed into place, so a
// crash never leaves a partial object under its name. Every path is made
// from a hash's hex digits alone, so no input names a file outside the
// store.
//
// Advertise is the front's: it leases the closure in its own records and
// mints the advertisement, and the native backend hands the call to it.
type Native struct {
	dir     string
	baseURL string
	// Advertiser mints the advertisement for a native backend (the front).
	Advertiser func(ctx context.Context, root [32]byte, until uint64) (*Advert, error)

	mu sync.Mutex // serializes writes of one store
}

var _ Backend = (*Native)(nil)

// OpenNative opens (creating) a native store under dir, serving at
// baseURL: the provider's public URL, below which /chirp/v1/ routes are
// served.
func OpenNative(dir, baseURL string) (*Native, error) {
	for _, d := range []string{dir, filepath.Join(dir, "objects"), filepath.Join(dir, "tmp")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}
	// A temporary file a crash left behind is never an object.
	tmp := filepath.Join(dir, "tmp")
	if ents, err := os.ReadDir(tmp); err == nil {
		for _, e := range ents {
			_ = os.Remove(filepath.Join(tmp, e.Name()))
		}
	}
	return &Native{dir: dir, baseURL: baseURL}, nil
}

// Path is where an object's file is.
func (n *Native) Path(hash [32]byte) string {
	h := hex.EncodeToString(hash[:])
	return filepath.Join(n.dir, "objects", h[:2], h)
}

// Put stores data under hash.
func (n *Native) Put(hash [32]byte, data []byte) error {
	if len(data) > MaxObject {
		return ErrTooLarge
	}
	if sha256.Sum256(data) != hash {
		return ErrHash
	}
	p := n.Path(hash)
	if st, err := os.Stat(p); err == nil && st.Size() == int64(len(data)) {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Join(n.dir, "tmp"), "put-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o444); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		return err
	}
	return syncDir(filepath.Dir(p))
}

// Has reports whether the object's file is there.
func (n *Native) Has(hash [32]byte) bool {
	st, err := os.Stat(n.Path(hash))
	return err == nil && st.Mode().IsRegular()
}

// Get reads length bytes from offset.
func (n *Native) Get(hash [32]byte, offset, length uint64) ([]byte, error) {
	f, err := n.Open(hash)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := uint64(st.Size())
	if offset > size || length > size-offset || length > MaxObject {
		return nil, ErrRange
	}
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, int64(offset)); err != nil && !(errors.Is(err, io.EOF) && length == 0) {
		return nil, err
	}
	return buf, nil
}

// Open opens an object's file for reading, for a route that streams it.
func (n *Native) Open(hash [32]byte) (*os.File, error) {
	f, err := os.Open(n.Path(hash))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrMissing
	}
	return f, err
}

// Size is an object's length, or -1 when it is not held.
func (n *Native) Size(hash [32]byte) int64 {
	st, err := os.Stat(n.Path(hash))
	if err != nil {
		return -1
	}
	return st.Size()
}

// Delete removes an object. The front decides that nothing it advertises
// still names it.
func (n *Native) Delete(hash [32]byte) error {
	err := os.Remove(n.Path(hash))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// Walk calls f for every object file the store holds, with its size.
func (n *Native) Walk(f func(hash [32]byte, size int64)) error {
	return filepath.WalkDir(filepath.Join(n.dir, "objects"), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := hex.DecodeString(d.Name())
		if err != nil || len(b) != 32 {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		f([32]byte(b), info.Size())
		return nil
	})
}

// ServeURL is the root object's route at the provider's public URL.
func (n *Native) ServeURL(root [32]byte) string { return ServeURL(n.baseURL, root) }

// Advertise hands the call to the front, which leases and mints.
func (n *Native) Advertise(ctx context.Context, root [32]byte, until uint64) (*Advert, error) {
	if n.Advertiser == nil {
		return nil, errors.New("backend: the native backend has no advertiser")
	}
	return n.Advertiser(ctx, root, until)
}

// Export is a read-only view of the store for an NFS export (design 3.6):
// <dir>/<root hex>/<index> for each object of a held closure, the root
// node at index 0 and the rest in the closure's order, each a hard link to
// the object's file so its handle is stable while the object is kept. The
// provider is its one writer; the operator exports the directory read-only
// with the system's own NFS server. The directory must be on the store's
// file system.
type Export struct {
	dir   string
	store *Native
}

// OpenExport opens (creating) an export directory over store.
func OpenExport(dir string, store *Native) (*Export, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Export{dir: dir, store: store}, nil
}

// Add links a held closure's objects under its root, replacing what was
// there; the directory is read-only once complete.
func (e *Export) Add(root [32]byte, objects [][32]byte) error {
	name := hex.EncodeToString(root[:])
	final := filepath.Join(e.dir, name)
	tmp := filepath.Join(e.dir, "."+name+".tmp")
	_ = makeWritable(tmp)
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	if err := os.Mkdir(tmp, 0o755); err != nil {
		return err
	}
	for i, h := range objects {
		if err := os.Link(e.store.Path(h), filepath.Join(tmp, strconv.Itoa(i))); err != nil {
			_ = os.RemoveAll(tmp)
			return fmt.Errorf("backend: export link (the export must be on the store's file system): %w", err)
		}
	}
	if err := os.Chmod(tmp, 0o555); err != nil {
		return err
	}
	if err := e.Remove(root); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

// Remove takes a root's view away.
func (e *Export) Remove(root [32]byte) error {
	p := filepath.Join(e.dir, hex.EncodeToString(root[:]))
	_ = makeWritable(p)
	return os.RemoveAll(p)
}

func makeWritable(p string) error { return os.Chmod(p, 0o755) }

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ServeURL is a root's BRC-167 root object route below base:
// <base>/chirp/v1/<root identifier>/objects/<root identifier>.
func ServeURL(base string, root [32]byte) string {
	return base + chirpSuffix(root)
}

func chirpSuffix(root [32]byte) string { return storerec.CHIRPURLSuffix(root) }
