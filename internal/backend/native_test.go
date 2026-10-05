package backend

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeStore(t *testing.T) {
	dir := t.TempDir()
	n, err := OpenNative(dir, "https://provider.example/store")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello, segment")
	h := sha256.Sum256(data)
	if err := n.Put(sha256.Sum256([]byte("other")), data); !errors.Is(err, ErrHash) {
		t.Fatalf("mismatch: %v", err)
	}
	if n.Has(h) {
		t.Fatal("held before put")
	}
	for range 2 {
		if err := n.Put(h, data); err != nil {
			t.Fatal(err)
		}
	}
	got, err := n.Get(h, 7, 7)
	if err != nil || string(got) != "segment" {
		t.Fatalf("get %q %v", got, err)
	}
	if _, err := n.Get(h, 10, 10); !errors.Is(err, ErrRange) {
		t.Fatalf("range %v", err)
	}
	if _, err := n.Get(sha256.Sum256(nil), 0, 0); !errors.Is(err, ErrMissing) {
		t.Fatalf("missing %v", err)
	}
	if u := n.ServeURL(h); !strings.HasPrefix(u, "https://provider.example/store/chirp/v1/") {
		t.Fatalf("url %s", u)
	}
	count := 0
	_ = n.Walk(func(x [32]byte, size int64) {
		if x == h && size == int64(len(data)) {
			count++
		}
	})
	if count != 1 {
		t.Fatal("walk")
	}
	// A path is the hash's digits alone.
	if rel, _ := filepath.Rel(dir, n.Path(h)); strings.Contains(rel, "..") {
		t.Fatal("path escapes")
	}

	ex, err := OpenExport(filepath.Join(dir, "export"), n)
	if err != nil {
		t.Fatal(err)
	}
	if err := ex.Add(h, [][32]byte{h, h}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "export", hexOf(h), "1"))
	if err != nil || string(b) != string(data) {
		t.Fatalf("export read %q %v", b, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "export", hexOf(h), "x"), nil, 0o644); err == nil && os.Getuid() != 0 {
		t.Fatal("the export is writable")
	}
	if err := ex.Remove(h); err != nil {
		t.Fatal(err)
	}
	if err := n.Delete(h); err != nil || n.Has(h) {
		t.Fatal("delete")
	}
}

func hexOf(h [32]byte) string {
	const d = "0123456789abcdef"
	out := make([]byte, 64)
	for i, c := range h {
		out[2*i], out[2*i+1] = d[c>>4], d[c&15]
	}
	return string(out)
}
