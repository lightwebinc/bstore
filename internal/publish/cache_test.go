package publish

import (
	"bytes"
	"crypto/sha256"
	"os"
	"testing"
)

// A small object Get held to its hash is read from memory after, and the
// cache lets it go with its file.
func TestCacheServesAndForgets(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := []byte("a small object")
	h, err := s.Put(b)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(h); err != nil || !bytes.Equal(got, b) {
		t.Fatalf("first read: %q, %v", got, err)
	}
	// Gone from disk behind the store's back: the cache still answers.
	if err := os.Remove(s.objectPath(h)); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(h); err != nil || !bytes.Equal(got, b) {
		t.Fatalf("cached read: %q, %v", got, err)
	}
	// Removed by the store: the cache forgets it too.
	if _, err := s.Put(b); err != nil {
		t.Fatal(err)
	}
	if n := s.remove(h); n == 0 {
		t.Fatal("remove freed nothing")
	}
	if _, err := s.Get(h); err == nil {
		t.Fatal("a removed object was still read")
	}
}

// An object that does not hash to its name is never cached, and dropping it
// clears what the cache held.
func TestCacheNeverHoldsBadBytes(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := []byte("held to its hash")
	h, _ := s.Put(b)
	if err := os.WriteFile(s.objectPath(h), []byte("other bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(h); err == nil {
		t.Fatal("bad bytes returned")
	}
	if s.cache.get(h) != nil {
		t.Fatal("bad bytes cached")
	}
	h2, _ := s.Put([]byte("second object"))
	if _, err := s.Get(h2); err != nil {
		t.Fatal(err)
	}
	s.drop(h2)
	if s.cache.get(h2) != nil {
		t.Fatal("a dropped object stayed cached")
	}
}

// The cache stays within its bound, keeps no object past cacheObject, and
// keeps an object read since the hand last passed over one that was not.
func TestCacheBound(t *testing.T) {
	c := newBlobCache(4 << 10)
	key := func(i int) [32]byte { return sha256.Sum256([]byte{byte(i), byte(i >> 8)}) }
	for i := 0; i < 100; i++ {
		c.add(key(i), make([]byte, 1<<10))
		c.get(key(0)) // read on every turn: survives
		if c.held > c.max {
			t.Fatalf("held %d past %d", c.held, c.max)
		}
	}
	if c.get(key(0)) == nil {
		t.Fatal("the object read on every turn was evicted")
	}
	if len(c.m) != 4 {
		t.Fatalf("%d entries, want 4", len(c.m))
	}
	big := newBlobCache(8 << 20)
	big.add(key(1), make([]byte, cacheObject+1))
	if big.held != 0 {
		t.Fatal("an object past cacheObject was cached")
	}
	for i := 0; i < 1000; i++ {
		c.forget(key(i))
	}
	if c.held != 0 || len(c.m) != 0 || len(c.ring) > 64 {
		t.Fatalf("after forgetting all: held %d, %d entries, ring %d", c.held, len(c.m), len(c.ring))
	}
}

func BenchmarkGetSmall(b *testing.B) {
	s, err := OpenStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	h, _ := s.Put(make([]byte, 1<<10))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := s.Get(h); err != nil {
				b.Fatal(err)
			}
		}
	})
}
