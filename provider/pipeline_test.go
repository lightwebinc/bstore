package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/testwriter"
)

// pipelineOnly is a provider with no entries: each root it is asked about
// has nothing to verify and nothing to decide, so the pipeline alone is
// under test.
func pipelineOnly(workers int) *Provider {
	return &Provider{cfg: &Config{Limits: Limits{VerifyWorkers: workers}}, st: &State{Holdings: map[string]*Holding{}},
		index: newEntryIndex(nil), queue: make(chan string, 4096), queued: map[string]uint8{},
		opts: Options{Logf: func(string, ...any) {}}}
}

// pipelineLog is what the hooks saw: the roots taken from the queue and
// decided, in order, the most verifications at once, and a root taken
// again before its decision.
type pipelineLog struct {
	mu        sync.Mutex
	taken     []string
	decided   []string
	inPipe    map[string]bool
	verifying int
	peak      int
	twice     string
}

func (l *pipelineLog) hooks(slow func(root string)) pipelineHooks {
	l.inPipe = map[string]bool{}
	return pipelineHooks{
		taken: func(r string) {
			l.mu.Lock()
			defer l.mu.Unlock()
			if l.inPipe[r] {
				l.twice = r
			}
			l.inPipe[r] = true
			l.taken = append(l.taken, r)
		},
		verify: func(r string) {
			l.mu.Lock()
			l.verifying++
			l.peak = max(l.peak, l.verifying)
			l.mu.Unlock()
			slow(r)
			l.mu.Lock()
			l.verifying--
			l.mu.Unlock()
		},
		decided: func(r string) {
			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.inPipe, r)
			l.decided = append(l.decided, r)
		},
	}
}

func (l *pipelineLog) snapshot() (taken, decided []string, peak int, twice string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.taken), slices.Clone(l.decided), l.peak, l.twice
}

func testRoots(n int) []string {
	out := make([]string, n)
	for i := range out {
		h := sha256.Sum256([]byte{byte(i), byte(i >> 8)})
		out[i] = hex.EncodeToString(h[:])
	}
	return out
}

// Verifications overlap and finish out of order; every root is taken in
// the order queued and decided once, and a root asked for again while it
// is in the pipeline is looked at once more after its decision, never
// twice at once.
func TestPipelineDecidesEveryRoot(t *testing.T) {
	const workers = 4
	p := pipelineOnly(workers)
	roots := testRoots(200)
	pos := map[string]int{}
	for i, r := range roots {
		pos[r] = i
	}
	var log pipelineLog
	var again sync.Once
	p.hooks = log.hooks(func(r string) {
		i := pos[r]
		if i == 10 {
			again.Do(func() { p.enqueue(r) })
		}
		if i%7 == 0 {
			time.Sleep(20 * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.worker(ctx) }()
	for _, r := range roots {
		p.enqueue(r)
	}
	end := time.Now().Add(20 * time.Second)
	for {
		_, decided, _, _ := log.snapshot()
		if len(decided) == len(roots)+1 {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("%d of %d decided", len(decided), len(roots)+1)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	taken, decided, peak, twice := log.snapshot()
	sorted := func(s []string) []string { s = slices.Clone(s); slices.Sort(s); return s }
	if !slices.Equal(sorted(taken), sorted(decided)) {
		t.Fatal("not every root taken was decided")
	}
	// The queue's order is the order asked, the root asked for again
	// once more after its decision.
	i := slices.Index(taken[11:], roots[10]) + 11
	if i < 11 || !slices.Equal(slices.Delete(slices.Clone(taken), i, i+1), roots) {
		t.Fatalf("taken in another order than queued (again at %d)", i)
	}
	if twice != "" {
		t.Fatalf("root %s taken again before its decision", short(twice))
	}
	if peak < 2 || peak > workers {
		t.Fatalf("%d verifications at once, want 2 to %d", peak, workers)
	}
	if len(p.queued) != 0 {
		t.Fatalf("%d roots left marked queued", len(p.queued))
	}
}

// A stop while verifications wait leaves no goroutine of the pipeline
// behind and decides nothing more.
func TestPipelineStopsCleanly(t *testing.T) {
	const workers = 4
	p := pipelineOnly(workers)
	ctx, cancel := context.WithCancel(context.Background())
	var log pipelineLog
	p.hooks = log.hooks(func(string) { <-ctx.Done() })
	done := make(chan struct{})
	go func() { defer close(done); p.worker(ctx) }()
	for _, r := range testRoots(50) {
		p.enqueue(r)
	}
	end := time.Now().Add(10 * time.Second)
	for {
		taken, _, peak, _ := log.snapshot()
		// Every verifier busy and one more root waiting for a verifier.
		if peak == workers && len(taken) == workers+1 {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the pipeline did not fill: %d taken, %d verifying", len(taken), peak)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop")
	}
	if _, decided, _, _ := log.snapshot(); len(decided) != 0 {
		t.Fatalf("%d decided with every verification waiting", len(decided))
	}
}

// Real closures through the real worker, their verifications held until
// all are in and then slowed at random: every one is held and advertised,
// each root decided once.
func TestPipelineHoldsEveryRoot(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.Limits.VerifyWorkers = 4
	gate := make(chan struct{})
	var log pipelineLog
	p := r.startHooked(cfg, true, Options{FreshSpends: true}, func(p *Provider) {
		p.hooks = log.hooks(func(string) {
			<-gate
			time.Sleep(time.Duration(rand.IntN(30)) * time.Millisecond)
		})
	})
	var puts []*testwriter.Put
	for i := range 12 {
		size := 1024 + i
		if i%4 == 3 {
			size = 2*1024*1024 + i // a closure with more to read
		}
		puts = append(puts, r.put(ctx, []*Provider{p}, content(size, byte(40+i)), "order/"+string(rune('a'+i)), bounty))
	}
	close(gate)
	for _, put := range puts {
		root := put.Object.Built.RootHash
		r.eventually("advertised", 60*time.Second, func() bool { return p.liveAd(root) != nil })
	}
	taken, decided, peak, twice := log.snapshot()
	if len(decided) > len(taken) {
		t.Fatalf("%d decided of %d taken", len(decided), len(taken))
	}
	if twice != "" {
		t.Fatalf("root %s taken again before its decision", short(twice))
	}
	if peak < 2 {
		t.Fatalf("%d verifications at once", peak)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, put := range puts {
		if e := p.st.Entries[put.C]; e == nil || e.Decision != decisionHeld {
			t.Fatalf("entry %s: %+v", short(put.C), e)
		}
	}
}
