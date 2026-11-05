package provider

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// holdRun is N small unique puts through the submit route, their pieces
// first and then their entries, timed from the first entry to the last
// decision held. It is the lab's put-rate.py as the provider sees it.
type holdRun struct {
	r    *rig
	p    *Provider
	puts []*testwriter.Put
}

// rateTerms are bounty's terms with a deadline the test chain, which mines
// a block a transaction, does not reach while the run is prepared.
var rateTerms = testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 1 << 20, CheckC: 30}

// chainDelay is how long the rig's chain takes to answer: a node or
// header service across a network, not the same process.
const chainDelay = 2 * time.Millisecond

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// prepareHoldRun builds n seven-byte objects, each with its own hold
// output and entry, before anything is submitted.
func prepareHoldRun(t *testing.T, r *rig, p *Provider, n int) *holdRun {
	t.Helper()
	ctx := context.Background()
	run := &holdRun{r: r, p: p}
	for i := range n {
		o, err := r.writer.Build(ctx, []byte(fmt.Sprintf("%07d", i)))
		if err != nil {
			t.Fatal(err)
		}
		put, err := r.writer.Entry(ctx, o, "rate/"+strconv.Itoa(i), rateTerms)
		if err != nil {
			t.Fatal(err)
		}
		run.puts = append(run.puts, put)
	}
	return run
}

func (run *holdRun) submit(t *testing.T, topic string, beef []byte) {
	t.Helper()
	ans, err := run.r.writer.Submit(context.Background(), "https://"+run.p.Addr(), topic, beef)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, why := testwriter.Admitted(ans, topic); !ok {
		t.Errorf("not taken: %s", why)
	}
}

// run makes the submissions, 8 at a time as a plane listener would hand
// them in, and returns the entries held a second.
func (run *holdRun) run(t *testing.T) float64 {
	t.Helper()
	p := run.p
	blob, recs := storerec.BlobTopic(testBucket), storerec.RecordsTopic(testBucket)
	each := func(f func(put *testwriter.Put)) {
		var wg sync.WaitGroup
		slots := make(chan struct{}, 8)
		for _, put := range run.puts {
			slots <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				f(put)
			}()
		}
		wg.Wait()
	}
	each(func(put *testwriter.Put) {
		for _, pc := range put.Object.Pieces {
			run.submit(t, blob, pc)
		}
	})
	start := time.Now()
	each(func(put *testwriter.Put) { run.submit(t, recs, put.BEEF) })
	submitted := time.Since(start)
	held := func() int {
		p.mu.Lock()
		defer p.mu.Unlock()
		n := 0
		for _, put := range run.puts {
			if e := p.st.Entries[put.C]; e != nil && e.Decision == decisionHeld {
				n++
			}
		}
		return n
	}
	end := time.Now().Add(5 * time.Minute)
	for held() < len(run.puts) {
		if time.Now().After(end) {
			t.Fatalf("%d of %d held\n%s", held(), len(run.puts), run.r.said())
		}
		time.Sleep(5 * time.Millisecond)
	}
	took := time.Since(start)
	rate := float64(len(run.puts)) / took.Seconds()
	t.Logf("%d entries held in %v (%.0f a second; submitted in %v)", len(run.puts), took.Round(time.Millisecond), rate, submitted.Round(time.Millisecond))
	return rate
}

func rateConfig(t *testing.T, r *rig) *Config {
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.Limits.SubmitsPerAddrPerSec, cfg.Limits.SubmitBurst = 1e9, 1e9
	cfg.Limits.InFlightPerAddr = 64
	cfg.TreeCount = 1023
	cfg.Limits.VerifyWorkers = envInt("BSTORE_HOLD_WORKERS", 0)
	return cfg
}

// Small entries whose pieces are already here are held at a rate set by
// the provider's own pipeline: several hundred a second, not one at a time
// behind a network round trip and a sync each.
func TestHoldRate(t *testing.T) {
	n := envInt("BSTORE_HOLD_N", 300)
	r := newSlowRig(t, chainDelay)
	cfg := rateConfig(t, r)
	p := r.startWith(cfg, true, Options{})
	run := prepareHoldRun(t, r, p, n)
	rate := run.run(t)
	if floor := float64(envInt("BSTORE_HOLD_FLOOR", 100)); rate < floor && !raceDetector {
		t.Fatalf("%.0f entries held a second, want at least %.0f", rate, floor)
	}
}

// backlog fills a provider's state, before it starts, with pending
// entries whose closures never come, holdings and pending advertisements:
// what a burst far ahead of the chain leaves behind.
func backlog(p *Provider, n int) {
	now := p.now()
	for i := range n {
		root := fmt.Sprintf("%064x", i+1)
		c := fmt.Sprintf("%064x", 1<<40+i)
		p.st.Entries[c] = &EntryRec{C: c, Bucket: testBucket, Key: "backlog/" + strconv.Itoa(i), Writer: hex.EncodeToString(p.signer.Identity.Compressed()),
			Root: root, Size: 7, ContentHash: root, ProofRoot: root, Created: uint64(now.UnixMilli()), At: now.UnixNano(),
			Hold: &HoldRec{Txid: fmt.Sprintf("%064x", 1<<41+i), Vout: 0, Sats: 5000, T: 1, K: 2, Retention: 86400, Deadline: 1e6}}
		p.index.add(p.st.Entries[c])
		hr := fmt.Sprintf("%064x", 1<<42+i)
		p.st.Holdings[hr] = &Holding{Root: hr, At: now.Unix(), Verified: true, Size: 7, Objects: 1, Blobs: 1, Lease: uint64(now.Unix()) + 86400}
		p.st.PendingAds = append(p.st.PendingAds, PendingAd{Root: hr, Txid: fmt.Sprintf("%064x", 1<<43+i), Tree: fmt.Sprintf("%064x", 1<<44),
			Vout: uint32(i), Expiry: uint64(now.Unix()) + 86400, At: now.Unix(), Sent: true})
	}
}

// The rate does not fall with the backlog: with thousands of entries
// waiting, holdings and advertisements pending, an entry costs what it
// costs on an empty provider.
func TestHoldRateWithBacklog(t *testing.T) {
	n, pending := envInt("BSTORE_HOLD_N", 300), envInt("BSTORE_HOLD_BACKLOG", 8000)
	r := newSlowRig(t, chainDelay)
	cfg := rateConfig(t, r)
	p := r.startWith(cfg, true, Options{})
	empty := prepareHoldRun(t, r, p, n).run(t)

	r2 := newSlowRig(t, chainDelay)
	cfg2 := rateConfig(t, r2)
	p2 := r2.startHooked(cfg2, true, Options{}, func(p *Provider) { backlog(p, pending) })
	loaded := prepareHoldRun(t, r2, p2, n).run(t)
	t.Logf("empty %.0f a second, with %d pending %.0f a second", empty, pending, loaded)
	if loaded < empty/2 && !raceDetector {
		t.Fatalf("the backlog halves the rate: %.0f against %.0f a second", loaded, empty)
	}
}

// An entry is held whichever comes first, it or its pieces.
func TestHeldWhicheverArrivesFirst(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	p := r.start(r.config(filepath.Join(t.TempDir(), "p")), true)
	base := "https://" + p.Addr()
	blob, recs := storerec.BlobTopic(testBucket), storerec.RecordsTopic(testBucket)
	var puts []*testwriter.Put
	for i, piecesFirst := range []bool{true, false} {
		o, err := r.writer.Build(ctx, content(1000+i, byte(90+i)))
		if err != nil {
			t.Fatal(err)
		}
		put, err := r.writer.Entry(ctx, o, "order/"+strconv.Itoa(i), bounty)
		if err != nil {
			t.Fatal(err)
		}
		send := func(topic string, beef []byte) {
			if ans, err := r.writer.Submit(ctx, base, topic, beef); err != nil {
				t.Fatal(err)
			} else if ok, why := testwriter.Admitted(ans, topic); !ok {
				t.Fatalf("not taken: %s", why)
			}
		}
		if !piecesFirst {
			send(recs, put.BEEF)
		}
		for _, pc := range o.Pieces {
			send(blob, pc)
		}
		if piecesFirst {
			send(recs, put.BEEF)
		}
		puts = append(puts, put)
	}
	for _, put := range puts {
		r.eventually("advertised", 30*time.Second, func() bool { return p.liveAd(put.Object.Built.RootHash) != nil })
	}
}

// An entry a writer was answered for is on disk before the answer, as a
// journal row of its own: a crash before the next save loses none of
// them, whatever else the state had not written yet.
func TestEntriesAnsweredSurviveACrash(t *testing.T) {
	r := newRig(t)
	cfg := rateConfig(t, r)
	// No background pass saves the state during the test.
	p := r.startWith(cfg, true, Options{Tick: time.Hour})
	run := prepareHoldRun(t, r, p, 20)
	run.run(t)
	st, err := loadState(cfg.Data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.replayAds(p.adDir, func(string, ...any) {}); err != nil {
		t.Fatal(err)
	}
	for _, put := range run.puts {
		if st.Entries[put.C] == nil {
			t.Fatalf("entry %s answered 200 is not on disk", short(put.C))
		}
	}
}
