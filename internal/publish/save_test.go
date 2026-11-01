package publish

import (
	"fmt"
	"sync"
	"testing"
)

// Concurrent changes are all durable when their saves return, whatever
// batches the saver made of them.
func TestSavesAreGroupCommitted(t *testing.T) {
	dir := t.TempDir()
	p := &Publisher{cfg: Config{Dir: dir}, st: &state{}}
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.noteBucket(fmt.Sprintf("bucket-%02d", i)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	st, err := loadState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Buckets) != 40 {
		t.Fatalf("%d buckets on disk, want 40", len(st.Buckets))
	}
	p.flushSaves()
	if err := p.noteBucket("late"); err == nil {
		t.Fatal("a save after the saver stopped did not fail")
	}
}

// The error of a failed write reaches every change it covered and no
// later one.
func TestSaveErrorReachesItsBatch(t *testing.T) {
	p := &Publisher{cfg: Config{Dir: "/proc/no-such-dir/x"}, st: &state{}}
	if err := p.noteBucket("a"); err == nil {
		t.Fatal("a save into an unwritable directory did not fail")
	}
	p.cfg.Dir = t.TempDir()
	if err := p.noteBucket("b"); err != nil {
		t.Fatalf("a save after the directory was fixed failed: %v", err)
	}
}
