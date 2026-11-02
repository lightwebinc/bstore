package backend

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

// TestNativePutConcurrent has writers race on one object and on objects of
// their own: every object ends whole under its name, and no temporary file
// is left behind.
func TestNativePutConcurrent(t *testing.T) {
	dir := t.TempDir()
	n, err := OpenNative(dir, "https://provider.example/store")
	if err != nil {
		t.Fatal(err)
	}
	shared := []byte("one object, many writers")
	sh := sha256.Sum256(shared)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			own := []byte(fmt.Sprintf("object %d", i))
			if err := n.Put(sha256.Sum256(own), own); err != nil {
				t.Error(err)
			}
			if err := n.PutHashed(sh, shared); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for i := range 16 {
		own := []byte(fmt.Sprintf("object %d", i))
		got, err := n.Get(sha256.Sum256(own), 0, uint64(len(own)))
		if err != nil || string(got) != string(own) {
			t.Fatalf("object %d: %q, %v", i, got, err)
		}
	}
	got, err := n.Get(sh, 0, uint64(len(shared)))
	if err != nil || string(got) != string(shared) {
		t.Fatalf("shared object: %q, %v", got, err)
	}
	if ents, err := os.ReadDir(filepath.Join(dir, "tmp")); err != nil || len(ents) != 0 {
		t.Fatalf("temporary files left: %d, %v", len(ents), err)
	}
}

// BenchmarkNativePut measures the store's write path at one and at eight
// writers, on the directory BENCH_DIR names (the disk under test) or a
// temporary one: the case that matters is a disk whose fsync is slow.
func BenchmarkNativePut(b *testing.B) {
	for _, writers := range []int{1, 8} {
		b.Run(fmt.Sprintf("writers-%d", writers), func(b *testing.B) {
			dir := os.Getenv("BENCH_DIR")
			if dir == "" {
				dir = b.TempDir()
			} else {
				var err error
				if dir, err = os.MkdirTemp(dir, "nativeput-"); err != nil {
					b.Fatal(err)
				}
				defer os.RemoveAll(dir)
			}
			n, err := OpenNative(dir, "http://example.invalid")
			if err != nil {
				b.Fatal(err)
			}
			data := make([]byte, 4<<20)
			if _, err := rand.Read(data); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(data)))
			var seq atomic.Uint64
			work := make(chan int)
			var wg sync.WaitGroup
			for range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range work {
						var h [32]byte
						binary.BigEndian.PutUint64(h[:], seq.Add(1))
						if err := n.PutHashed(h, data); err != nil {
							b.Error(err)
						}
					}
				}()
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				work <- i
			}
			close(work)
			wg.Wait()
		})
	}
}
