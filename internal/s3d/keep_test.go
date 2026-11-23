package s3d

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/publish"
)

// A closure held whole gets a manifest once; one that has a manifest is
// marked used, and not more often than touchEvery.
func TestKeepStoreClosure(t *testing.T) {
	dir := t.TempDir()
	st, err := publish.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	k := &keepStore{Store: st}
	blob, root := []byte("a blob"), []byte("a root node")
	bh, rh := sha256.Sum256(blob), sha256.Sum256(root)
	if k.Held(bh) {
		t.Fatal("an empty store holds an object")
	}
	for h, b := range map[[32]byte][]byte{bh: blob, rh: root} {
		if err := k.Keep(h, b); err != nil {
			t.Fatal(err)
		}
	}
	if !k.Held(bh) || !k.Held(rh) {
		t.Fatal("a kept object is not held")
	}
	if err := k.Closure(rh, uint64(len(blob)), nil, [][32]byte{bh}); err != nil {
		t.Fatal(err)
	}
	m, err := st.Manifest(rh)
	if err != nil || m.Size != uint64(len(blob)) || len(m.Blobs) != 1 || len(m.Branches) != 0 {
		t.Fatalf("manifest %+v: %v", m, err)
	}
	// Age the manifest; a telling inside touchEvery leaves it, one after
	// marks it used.
	ents, _ := filepath.Glob(filepath.Join(dir, "roots", "*.json"))
	if len(ents) != 1 {
		t.Fatalf("%d manifest(s)", len(ents))
	}
	old := time.Now().Add(-time.Hour)
	_ = os.Chtimes(ents[0], old, old)
	if err := k.Closure(rh, uint64(len(blob)), nil, [][32]byte{bh}); err != nil {
		t.Fatal(err)
	}
	if info, _ := os.Stat(ents[0]); !info.ModTime().Equal(old) {
		t.Fatal("a telling inside touchEvery touched the manifest")
	}
	k.mu.Lock()
	k.touched[rh] = time.Now().Add(-2 * touchEvery)
	k.mu.Unlock()
	if err := k.Closure(rh, uint64(len(blob)), nil, [][32]byte{bh}); err != nil {
		t.Fatal(err)
	}
	if info, _ := os.Stat(ents[0]); !info.ModTime().After(old) {
		t.Fatal("a telling after touchEvery did not mark the closure used")
	}
}
