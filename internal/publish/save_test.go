package publish

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lightwebinc/bcommon/funding"
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

// wrote is what the last write that succeeded wrote: hot and cold bytes.
func (s *saver) wrote() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hotBytes, s.coldBytes
}

// A put-sized change writes the hot part and nothing else: no cold file is
// written again, the hot part is a few bytes a hold output, and it does not
// grow with the transactions kept.
func TestPutSizedChangeWritesTheHotPartOnly(t *testing.T) {
	var hot [2]int
	for i, more := range []int{0, 32} {
		st := benchState(t, 4, 1024, 0)
		for j := 0; j < more; j++ {
			txid, raw := benchTx(t, 1023, byte(0x80+j))
			st.Piece.Trees = append(st.Piece.Trees, funding.Tree{Txid: txid, RawHex: raw, Count: 1023, Next: 100})
		}
		p := &Publisher{cfg: Config{Dir: t.TempDir(), Logf: t.Logf}, st: st}
		if err := p.save(); err != nil {
			t.Fatal(err)
		}
		if _, cold := p.sv.wrote(); cold < (8+more)*100_000 {
			t.Fatalf("the first save wrote %d cold bytes for %d transactions of 100 KB", cold, 8+more)
		}
		p.mu.Lock()
		putSized(p.st, 1)
		err := p.saveLocked()
		p.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		var cold int
		hot[i], cold = p.sv.wrote()
		if cold != 0 {
			t.Fatalf("a put-sized change wrote %d cold bytes", cold)
		}
		if hot[i] > 10*len(st.Holds)+8192 {
			t.Fatalf("the hot part is %d bytes for %d holds", hot[i], len(st.Holds))
		}
		p.flushSaves()
	}
	if hot[1]-hot[0] > 32*400 {
		t.Fatalf("the hot part grew from %d to %d bytes with 32 more transactions kept", hot[0], hot[1])
	}
}

// The encode alone of a put-sized change, the state of BenchmarkSave.
func BenchmarkEncodeState(b *testing.B) {
	p := &Publisher{st: benchState(b, 4, 1024, 1024)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		putSized(p.st, i%1024)
		if _, err := p.sv.encode(p.st); err != nil {
			b.Fatal(err)
		}
	}
}
