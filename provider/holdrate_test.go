package provider

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// order is what goes first: "pieces" (then the entries), "entries"
	// (then the pieces), or "mixed" (each put's entry and then its pieces,
	// puts side by side, as the plane hands them in).
	order string
	// lat is how long each submission took to answer.
	lmu sync.Mutex
	lat []time.Duration
}

// latency is the slowest submission and the 99th percentile.
func (run *holdRun) latency() (slowest, p99 time.Duration) {
	run.lmu.Lock()
	defer run.lmu.Unlock()
	if len(run.lat) == 0 {
		return 0, 0
	}
	s := slices.Clone(run.lat)
	slices.Sort(s)
	return s[len(s)-1], s[len(s)*99/100]
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
	at := time.Now()
	ans, err := run.r.writer.Submit(context.Background(), "https://"+run.p.Addr(), topic, beef)
	run.lmu.Lock()
	run.lat = append(run.lat, time.Since(at))
	run.lmu.Unlock()
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
	pieces := func(put *testwriter.Put) {
		for _, pc := range put.Object.Pieces {
			run.submit(t, blob, pc)
		}
	}
	entry := func(put *testwriter.Put) { run.submit(t, recs, put.BEEF) }
	var start time.Time
	switch run.order {
	case "entries":
		start = time.Now()
		each(entry)
		each(pieces)
	case "mixed":
		start = time.Now()
		each(func(put *testwriter.Put) { entry(put); pieces(put) })
	default:
		each(pieces)
		start = time.Now()
		each(entry)
	}
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
	t.Logf("%d entries held in %v (%.0f a second; %s first, submitted in %v)", len(run.puts), took.Round(time.Millisecond), rate, cmp.Or(run.order, "pieces"), submitted.Round(time.Millisecond))
	return rate
}

func rateConfig(t *testing.T, r *rig) *Config {
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.Limits.SubmitsPerAddrPerSec, cfg.Limits.SubmitBurst = 1e9, 1e9
	cfg.Limits.InFlightPerAddr = 64
	cfg.TreeCount = 1023
	cfg.Limits.VerifyWorkers = envInt("BSTORE_HOLD_WORKERS", 0)
	if os.Getenv("BSTORE_HOLD_FEEDER") != "" {
		// The plane's path: answered before the take, takes 8 at a time.
		cfg.Feeders = []string{"127.0.0.1/32"}
	}
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
	run.order = os.Getenv("BSTORE_HOLD_ORDER")
	rate := run.run(t)
	if floor := float64(envInt("BSTORE_HOLD_FLOOR", 100)); rate < floor && !raceDetector {
		t.Fatalf("%.0f entries held a second, want at least %.0f", rate, floor)
	}
}

// The same with the entries before their pieces, and with each put's entry
// and pieces side by side: an entry whose pieces come after it is held when
// the last of them lands, not when a pass looks again.
func TestHoldRateEntriesFirst(t *testing.T) {
	for _, order := range []string{"entries", "mixed"} {
		t.Run(order, func(t *testing.T) {
			n := envInt("BSTORE_HOLD_N", 300)
			r := newSlowRig(t, chainDelay)
			cfg := rateConfig(t, r)
			p := r.startWith(cfg, true, Options{})
			run := prepareHoldRun(t, r, p, n)
			run.order = order
			rate := run.run(t)
			if floor := float64(envInt("BSTORE_HOLD_FLOOR", 100)); rate < floor && !raceDetector {
				t.Fatalf("%.0f entries held a second, want at least %.0f", rate, floor)
			}
		})
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
	st := loadSaved(t, cfg.Data)
	for _, put := range run.puts {
		if st.Entries[put.C] == nil {
			t.Fatalf("entry %s answered 200 is not on disk", short(put.C))
		}
	}
}

// post is one submission as the gateway makes it: the answer's status,
// the seconds it asked to wait on a 429 or 503, and whether the topic was
// taken.
func (run *holdRun) post(t *testing.T, topic string, beef []byte) (code, wait int, ok bool) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "https://"+run.p.Addr()+"/submit", bytes.NewReader(beef))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-topics", topic)
	res, err := run.r.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		wait, _ = strconv.Atoi(res.Header.Get("Retry-After"))
		return res.StatusCode, wait, false
	}
	var ans map[string]any
	if err := json.Unmarshal(body, &ans); err != nil {
		t.Fatal(err)
	}
	ok, _ = testwriter.Admitted(ans, topic)
	return res.StatusCode, 0, ok
}

// gatewaySubmit submits as the gateway's Delivery.submit does: a 429 or
// 503 is tried again after Retry-After (at least 100 ms), ten times, and
// then given up on, for the outbox to send again later.
func (run *holdRun) gatewaySubmit(t *testing.T, topic string, beef []byte, busy *atomic.Int64) bool {
	for try := 0; ; try++ {
		code, wait, ok := run.post(t, topic, beef)
		if code == http.StatusOK {
			return ok
		}
		if code != http.StatusTooManyRequests && code != http.StatusServiceUnavailable {
			t.Errorf("submit: %d", code)
			return false
		}
		busy.Add(1)
		if try >= 10 {
			return false
		}
		time.Sleep(max(time.Duration(wait)*time.Second, 100*time.Millisecond))
	}
}

// runAsGateway sends every put as the gateway does: a goroutine a put,
// each sending its pieces at once and then its entry, and whatever a
// target would not take in ten tries sent again afterwards, one at a time
// (the outbox's retry). It returns the entries held a second.
func (run *holdRun) runAsGateway(t *testing.T) float64 {
	t.Helper()
	blob, recs := storerec.BlobTopic(testBucket), storerec.RecordsTopic(testBucket)
	type item struct {
		topic string
		beef  []byte
	}
	var busy atomic.Int64
	var lmu sync.Mutex
	var left []item
	start := time.Now()
	var wg sync.WaitGroup
	for _, put := range run.puts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var pw sync.WaitGroup
			for _, pc := range put.Object.Pieces {
				pw.Add(1)
				go func() {
					defer pw.Done()
					if !run.gatewaySubmit(t, blob, pc, &busy) {
						lmu.Lock()
						left = append(left, item{blob, pc})
						lmu.Unlock()
					}
				}()
			}
			pw.Wait()
			if !run.gatewaySubmit(t, recs, put.BEEF, &busy) {
				lmu.Lock()
				left = append(left, item{recs, put.BEEF})
				lmu.Unlock()
			}
		}()
	}
	wg.Wait()
	firstPass := time.Since(start)
	for _, it := range left {
		for !run.gatewaySubmit(t, it.topic, it.beef, &busy) {
		}
	}
	took := run.waitHeld(t, start)
	rate := float64(len(run.puts)) / took.Seconds()
	t.Logf("%d entries held in %v (%.0f a second) as the gateway sends: first pass %v, %d busy answers, %d sent again", len(run.puts), took.Round(time.Millisecond), rate, firstPass.Round(time.Millisecond), busy.Load(), len(left))
	return rate
}

// waitHeld waits for every put to be held and returns the time since start.
func (run *holdRun) waitHeld(t *testing.T, start time.Time) time.Duration {
	t.Helper()
	p := run.p
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
	return time.Since(start)
}

// The gateway sends unicast with a goroutine a put and sixteen piece
// workers each, from one address: more in flight than
// limits.in_flight_per_addr allows. A submission past the allowance waits
// its turn here rather than being sent away for a second, so the rate is
// the provider's, not the backoff's.
func TestHoldRateAsGatewaySends(t *testing.T) {
	n := envInt("BSTORE_HOLD_N", 300)
	r := newSlowRig(t, chainDelay)
	cfg := rateConfig(t, r)
	cfg.Limits.InFlightPerAddr = 32
	p := r.startWith(cfg, true, Options{})
	run := prepareHoldRun(t, r, p, n)
	rate := run.runAsGateway(t)
	if floor := float64(envInt("BSTORE_HOLD_FLOOR", 100)); rate < floor && !raceDetector {
		t.Fatalf("%.0f entries held a second, want at least %.0f", rate, floor)
	}
}

// labShape fills a provider's state, before it starts, as a lab provider's
// stands after a week of small puts: held holdings with their
// advertisements and entries, and pending advertisements whose BEEF
// carries a long chain of unmined funding trees (142 KB each, as sampled
// on the lab on 2026-11-05).
func labShape(p *Provider, holdings, pending int) {
	now := p.now()
	writer := hex.EncodeToString(p.signer.Identity.Compressed())
	raw := strings.Repeat("ab", 300)
	bump := strings.Repeat("cd", 100)
	for i := range holdings {
		root := fmt.Sprintf("%064x", 1<<45+i)
		c := fmt.Sprintf("%064x", 1<<46+i)
		p.st.Entries[c] = &EntryRec{C: c, Bucket: testBucket, Key: "held/" + strconv.Itoa(i), Writer: writer, Root: root, Size: 7,
			ContentHash: root, ProofRoot: root, Created: uint64(now.UnixMilli()), At: now.UnixNano(), Decision: decisionHeld,
			Hold: &HoldRec{Txid: fmt.Sprintf("%064x", 1<<47+i/1024), Vout: uint32(i % 1024), Sats: 5000, T: 1, K: 2, Retention: 86400, Deadline: 1e6}}
		p.index.add(p.st.Entries[c])
		p.st.Holdings[root] = &Holding{Root: root, At: now.Unix(), Verified: true, Size: 7, Objects: 1, Blobs: 1, Bytes: 7, Buckets: []string{testBucket},
			Entries: []string{c}, Lease: uint64(now.Unix()) + 86400, Ad: &AdRec{Txid: fmt.Sprintf("%064x", 1<<48+i), Expiry: uint64(now.Unix()) + 86400, Height: 700, RawHex: raw, Bump: bump}}
	}
	// Neither is hex, so resumeAds skips them without a log line: a row
	// before RawHex carried a BEEF of 142 KB, one since carries the
	// transaction alone.
	chain, alone := strings.Repeat("zz", 71012), strings.Repeat("zz", 300)
	for i := range pending {
		root := fmt.Sprintf("%064x", 1<<49+i)
		p.st.Holdings[root] = &Holding{Root: root, At: now.Unix(), Verified: true, Size: 7, Objects: 1, Blobs: 1, Bytes: 7, Lease: uint64(now.Unix()) + 86400}
		pa := PendingAd{Root: root, Txid: fmt.Sprintf("%064x", 1<<50+i), Tree: fmt.Sprintf("%064x", 1<<51+i/16),
			Vout: uint32(i % 16), Expiry: uint64(now.Unix()) + 86400, At: now.Unix(), Sent: true, RawHex: alone}
		if os.Getenv("BSTORE_HOLD_LEGACY") != "" {
			pa.RawHex, pa.BeefHex = "", chain
		}
		p.st.PendingAds = append(p.st.PendingAds, pa)
	}
}

// A lab provider's shape: 25,000 holdings held, 1,700 advertisements
// pending with long BEEFs (a 250 MB state), funding trees of 16 outputs,
// the background pass every 5 s. What a hold costs there: the state saves
// on the path, their size and time, and the mint lock's hold per
// advertisement. With BSTORE_HOLD_CHECKPOINTS set, checkpoints of the
// whole state run back to back through the run, as the lab's passes did:
// the feed must answer within milliseconds and the rate must not fall.
func TestHoldRateLabShape(t *testing.T) {
	n := envInt("BSTORE_HOLD_N", 300)
	holdings, pending := envInt("BSTORE_HOLD_HOLDINGS", 25000), envInt("BSTORE_HOLD_PENDING", 1700)
	r := newSlowRig(t, chainDelay)
	cfg := rateConfig(t, r)
	cfg.TreeCount = 16
	p := r.startHooked(cfg, true, Options{Tick: 5 * time.Second}, func(p *Provider) { labShape(p, holdings, pending) })
	run := prepareHoldRun(t, r, p, n)
	saves0, saveSecs0 := p.metrics.get("bstore_provider_state_saves_total"), p.metrics.get("bstore_provider_state_save_seconds_total")
	copy0 := p.metrics.get("bstore_provider_state_copy_seconds_total")
	stop := make(chan struct{})
	var checkpoints sync.WaitGroup
	if os.Getenv("BSTORE_HOLD_CHECKPOINTS") != "" {
		checkpoints.Add(1)
		go func() {
			defer checkpoints.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				p.mu.Lock()
				_ = p.save()
				p.mu.Unlock()
			}
		}()
	}
	rate := run.run(t)
	close(stop)
	checkpoints.Wait()
	saves := p.metrics.get("bstore_provider_state_saves_total") - saves0
	saveSecs := p.metrics.get("bstore_provider_state_save_seconds_total") - saveSecs0
	copySecs := p.metrics.get("bstore_provider_state_copy_seconds_total") - copy0
	locks, lockSecs := p.metrics.get("bstore_provider_mint_lock_total"), p.metrics.get("bstore_provider_mint_lock_seconds_total")
	slowest, p99 := run.latency()
	// One save of this state, for its size and time.
	at := time.Now()
	p.mu.Lock()
	err := p.save()
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	one := time.Since(at)
	st, _ := os.Stat(filepath.Join(cfg.Data, "state.json"))
	var size int64
	if st != nil {
		size = st.Size()
	}
	t.Logf("during the run %.0f saves, %.2f s in all (%.0f ms each, %.0f ms of it copying under p.mu); submissions answered in %v at the 99th percentile, %v at most; mint lock held %.1f ms an advertisement over %.0f; one save of this state: %d MB in %v", saves, saveSecs, 1000*saveSecs/max(saves, 1), 1000*copySecs/max(saves, 1), p99.Round(time.Millisecond), slowest.Round(time.Millisecond), 1000*lockSecs/max(locks, 1), locks, size>>20, one.Round(time.Millisecond))
	if floor := float64(envInt("BSTORE_HOLD_FLOOR", 100)); rate < floor && !raceDetector {
		t.Fatalf("%.0f entries held a second, want at least %.0f", rate, floor)
	}
}

// What a run leaves in the log brings the holdings back on a start that
// had no checkpoint: a copy of the data directory taken while the provider
// runs, as a crash would leave it, loads with every entry held and every
// holding verified.
func TestLogBringsBackHoldingsAfterACrash(t *testing.T) {
	r := newRig(t)
	cfg := rateConfig(t, r)
	p := r.startWith(cfg, true, Options{Tick: time.Hour})
	run := prepareHoldRun(t, r, p, 30)
	run.run(t)
	// No checkpoint was written: the log alone has the run.
	if _, err := os.Stat(filepath.Join(cfg.Data, "state.json")); err == nil {
		if st := loadSaved(t, cfg.Data); st.LogGen != 0 {
			t.Fatalf("a checkpoint was written during the run (generation %d)", st.LogGen)
		}
	}
	st := loadSaved(t, cfg.Data)
	for _, put := range run.puts {
		e := st.Entries[put.C]
		if e == nil || e.Decision != decisionHeld {
			t.Fatalf("entry %s: %+v", short(put.C), e)
		}
		h := st.Holdings[hex.EncodeToString(put.Object.Built.RootHash[:])]
		if h == nil || !h.Verified || !slices.Contains(h.Entries, put.C) {
			t.Fatalf("holding for %s: %+v", short(put.C), h)
		}
		if h.Ad == nil && !slices.ContainsFunc(st.PendingAds, func(pa PendingAd) bool { return pa.Root == h.Root }) {
			t.Fatalf("root %s: neither advertised nor pending", short(h.Root))
		}
	}
	// A second provider on a copy of the directory, as a restart after a
	// crash, holds the same.
	dir := filepath.Join(t.TempDir(), "copy")
	if out, err := exec.Command("cp", "-r", cfg.Data, dir).CombinedOutput(); err != nil {
		t.Fatalf("cp: %v: %s", err, out)
	}
	cfg2 := r.config(dir)
	if err := cfg2.Check(); err != nil {
		t.Fatal(err)
	}
	q, err := New(context.Background(), cfg2, Options{Logf: r.logf})
	if err != nil {
		t.Fatal(err)
	}
	defer q.log.close()
	held := 0
	for _, put := range run.puts {
		if e := q.st.Entries[put.C]; e != nil && e.Decision == decisionHeld {
			held++
		}
	}
	if held != len(run.puts) {
		t.Fatalf("%d of %d held after the restart", held, len(run.puts))
	}
}
