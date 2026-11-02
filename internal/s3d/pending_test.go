package s3d

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/storerec"
)

func addN(t testing.TB, s *state, n, writers int) {
	t.Helper()
	var wg sync.WaitGroup
	next := make(chan int)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				e := &storerec.Entry{Bucket: "b", Key: fmt.Sprintf("k%d", i), Created: uint64(i), Kind: storerec.KindPut}
				c := sha256.Sum256([]byte(e.Key))
				if err := s.add(e, []byte(e.Key), c); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
}

// Every add that returned is in the log, whichever write carried it: a
// state opened from what is on disk holds them all.
func TestPendingGroupCommitDurable(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	addN(t, s, 500, 32)
	raw, err := os.ReadFile(filepath.Join(dir, "pending.log"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), "\n"); n != 500 {
		t.Fatalf("%d lines in the log", n)
	}
	again, err := openState(dir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.byC) != 500 {
		t.Fatalf("%d entries reopened", len(again.byC))
	}
}

// A rewrite of the log, whichever add it falls on, loses no line: the
// writers run across several of them.
func TestPendingCompactionDuringAdds(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	// One live entry and a long log: the next lines cross the compaction
	// bound while other adds wait on a write.
	s.mu.Lock()
	s.lines = 4*len(s.byC) + 1024 - 8
	s.mu.Unlock()
	addN(t, s, 200, 32)
	again, err := openState(dir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.byC) != 200 {
		t.Fatalf("%d entries reopened", len(again.byC))
	}
}

// A write that fails answers every line it carried with the error, and the
// next write works again.
func TestPendingWriteFailureAnswers(t *testing.T) {
	dir := t.TempDir()
	s, err := openState(dir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "pending.log"))
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.log = f // read-only: the write fails
	s.mu.Unlock()
	e := &storerec.Entry{Bucket: "b", Key: "k", Created: 1, Kind: storerec.KindPut}
	if err := s.add(e, []byte("k"), sha256.Sum256([]byte("k"))); err == nil {
		t.Fatal("a failed write answered nil")
	}
	s.mu.Lock()
	s.log = nil
	s.mu.Unlock()
	e2 := &storerec.Entry{Bucket: "b", Key: "k2", Created: 2, Kind: storerec.KindPut}
	if err := s.add(e2, []byte("k2"), sha256.Sum256([]byte("k2"))); err != nil {
		t.Fatalf("the next write: %v", err)
	}
}

// BenchmarkPendingAdd is a put's log line, at eight writers, on BENCH_DIR
// (the disk under test) or a temporary directory.
func BenchmarkPendingAdd(b *testing.B) {
	dir := os.Getenv("BENCH_DIR")
	if dir == "" {
		dir = b.TempDir()
	} else {
		d, err := os.MkdirTemp(dir, "pending-")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(d)
		dir = d
	}
	s, err := openState(dir, time.Now)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	addN(b, s, b.N, 8)
}
