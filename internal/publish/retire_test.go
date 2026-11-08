package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/headers"

	"github.com/lightwebinc/bstore/storerec"
)

// A delete closes the key's open race, and only that key's: with fewer
// than t passed nothing is paid, the race is never stepped again, its hold
// output is reclaimed once the deadline comes, and the race then moves to
// races/done/ with its hold record gone from the state. The deleted key's
// put leaves entries/; its delete and every other key's entries stay.
func TestDeleteRetiresTheKeysRaces(t *testing.T) {
	r := newRaceRig(t)
	gone := r.put("gone", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 20, CheckC: 30})
	kept := r.put("kept", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	del, err := r.p.Delete(r.ctx, testBucket, sha256.Sum256([]byte("id")), "gone", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.p.retirePath()); err != nil {
		t.Fatal("the delete is not recorded for the next pass")
	}
	o := RaceOptions{Window: time.Hour}
	r.p.RaceTick(r.ctx, o)
	g, err := r.p.Race(gone.Entry)
	if err != nil || g.State != RaceClosed || g.Retired == "" {
		t.Fatalf("the deleted key's race after a pass: %v %s %q", err, g.State, g.Retired)
	}
	if k, _ := r.p.Race(kept.Entry); k.State != RaceOpen || k.Retired != "" {
		t.Fatalf("another key's race was touched: %s %q", k.State, k.Retired)
	}
	if _, err := os.Stat(r.p.retirePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a delete the pass applied is still recorded")
	}
	for txid, want := range map[string]bool{gone.Entry: false, del.Txid: true, kept.Entry: true} {
		for _, ext := range []string{".json", ".beef"} {
			_, err := os.Stat(filepath.Join(r.p.entriesDir(), txid+ext))
			if (err == nil) != want {
				t.Fatalf("entry %s%s: present %v, want %v", short(txid), ext, err == nil, want)
			}
		}
	}
	before, _ := os.ReadFile(filepath.Join(r.p.racesDir(), gone.Entry+".json"))
	r.p.RaceTick(r.ctx, o)
	after, _ := os.ReadFile(filepath.Join(r.p.racesDir(), gone.Entry+".json"))
	if !bytes.Equal(before, after) {
		t.Fatal("a retired race was stepped again")
	}
	// The deadline comes: reclaimed, then over and moved out of races/.
	r.blocks(25)
	r.p.RaceTick(r.ctx, o)
	r.blocks(1)
	r.p.RaceTick(r.ctx, o)
	g, err = r.p.Race(gone.Entry)
	if err != nil || g.State != RaceReclaimd {
		t.Fatalf("the retired race after its deadline: %v %s", err, g.State)
	}
	if _, err := os.Stat(filepath.Join(r.p.doneDir(), gone.Entry+".json")); err != nil {
		t.Fatal("a reclaimed race is not in races/done/")
	}
	live, _ := r.p.Races()
	for _, x := range live {
		if x.Entry == gone.Entry {
			t.Fatal("a race that is over is still listed live")
		}
	}
	for _, h := range r.p.Holds() {
		if h.Entry == gone.Entry {
			t.Fatal("the hold record of a race that is over is kept")
		}
	}
	if r.p.DoneRaces() != 1 {
		t.Fatalf("%d races in races/done/", r.p.DoneRaces())
	}
}

// A delete that comes after t advertisers passed settles the race by rank:
// those providers held the object and passed.
func TestDeleteSettlesARaceWithTPassed(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 10_000, T: 1, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a := r.provider("a")
	passed(race, a, r.advertise(a, 2_000_000_000, nil), 1)
	if err := r.p.saveRace(race); err != nil {
		t.Fatal(err)
	}
	if _, err := r.p.Delete(r.ctx, testBucket, sha256.Sum256([]byte("id")), "k", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	r.p.RaceTick(r.ctx, RaceOptions{Window: time.Hour})
	got, _ := r.p.Race(race.Entry)
	if got.Retired == "" || got.Settlement == nil || (got.State != RaceSettling && got.State != RaceSettled) {
		t.Fatalf("after the delete: %s %q settlement %v", got.State, got.Retired, got.Settlement != nil)
	}
	if len(got.Settlement.Payees) != 1 || got.Settlement.Payees[0].Satoshis != 10_000 {
		t.Fatalf("payees %+v", got.Settlement.Payees)
	}
}

// A put made after the delete is not touched by it.
func TestDeleteLeavesALaterPut(t *testing.T) {
	r := newRaceRig(t)
	if _, err := r.p.Delete(r.ctx, testBucket, sha256.Sum256([]byte("id")), "k", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	race := r.put("k", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	r.p.RaceTick(r.ctx, RaceOptions{Window: time.Hour})
	if got, _ := r.p.Race(race.Entry); got.State != RaceOpen || got.Retired != "" {
		t.Fatalf("a put made after the delete: %s %q", got.State, got.Retired)
	}
	if _, err := os.Stat(filepath.Join(r.p.entriesDir(), race.Entry+".json")); err != nil {
		t.Fatal("a put made after the delete left entries/")
	}
}

// A race that cannot be judged (its closure served by nobody) waits a
// minute after its first failure, two after its second, and is not stepped
// meanwhile; only those failures are logged.
func TestUnjudgeableRaceBacksOff(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	race.Cands = append(race.Cands, &Cand{Advertiser: "02" + strings.Repeat("ab", 32), First: strings.Repeat("cd", 32),
		FirstSeen: 1, URL: "https://provider.example/store" + storerec.CHIRPURLSuffix(race.entry.Root), Expiry: 2_000_000_000, Status: CandSeen})
	if err := r.p.saveRace(race); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(r.store.manifestPath(race.entry.Root)); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var logged []string
	r.p.cfg.Logf = func(f string, a ...any) {
		mu.Lock()
		logged = append(logged, fmt.Sprintf(f, a...))
		mu.Unlock()
	}
	failures := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, l := range logged {
			if strings.Contains(l, "the closure's blobs") {
				n++
			}
		}
		return n
	}
	o := RaceOptions{Window: time.Hour}
	r.p.RaceTick(r.ctx, o)
	got, _ := r.p.Race(race.Entry)
	if got.Fails != 1 || got.RetryAt < time.Now().Add(50*time.Second).Unix() {
		t.Fatalf("after one failure: fails %d, retry in %ds", got.Fails, got.RetryAt-time.Now().Unix())
	}
	r.p.RaceTick(r.ctx, o)
	if again, _ := r.p.Race(race.Entry); again.Fails != 1 {
		t.Fatalf("stepped before its retry time: fails %d", again.Fails)
	}
	got.RetryAt = time.Now().Add(-time.Second).Unix()
	if err := r.p.saveRace(got); err != nil {
		t.Fatal(err)
	}
	// The test rewrote a race the pass has cached, which no path of the
	// gateway does; within one tick of the file system's clock, at the same
	// size, the pass would take the file for unchanged.
	delete(r.p.raceCache, got.Entry)
	r.p.RaceTick(r.ctx, o)
	got, _ = r.p.Race(race.Entry)
	if got.Fails != 2 || got.RetryAt < time.Now().Add(110*time.Second).Unix() {
		t.Fatalf("after two failures: fails %d, retry in %ds", got.Fails, got.RetryAt-time.Now().Unix())
	}
	if n := failures(); n != 2 {
		t.Fatalf("%d failures logged, want 2", n)
	}
	for _, l := range logged {
		if strings.Count(l, "race "+short(race.Entry)) > 1 {
			t.Fatalf("a line names the race twice: %s", l)
		}
	}
}

// countTip counts the reads of the tip.
type countTip struct{ n atomic.Int64 }

func (c *countTip) CurrentHeight(context.Context) (uint32, error) {
	c.n.Add(1)
	return 100, nil
}

func (c *countTip) HeaderAt(context.Context, uint32) (*headers.Header, error) { return nil, nil }

// A pass reads the tip once for all its races, not once a race.
func TestRacePassReadsTheTipOnce(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	tip := &countTip{}
	p := &Publisher{cfg: Config{Dir: dir, Tip: tip, Logf: t.Logf, Now: time.Now}, st: &state{}}
	var races []*Race
	for i := range 20 {
		races = append(races, &Race{Entry: strings.Repeat(fmt.Sprintf("%02x", i), 32), State: RaceOpen,
			entry: &storerec.Entry{Hold: &storerec.Hold{Deadline: 50}}})
	}
	p.stepOpen(withTickTip(context.Background()), races, RaceOptions{Parallel: 4})
	if n := tip.n.Load(); n != 1 {
		t.Fatalf("%d reads of the tip for 20 races", n)
	}
	for _, r := range races {
		if r.State != RaceLapsed {
			t.Fatalf("race %s: %s", short(r.Entry), r.State)
		}
	}
	// Outside a pass every caller reads the tip itself.
	p.stepOpen(context.Background(), []*Race{{Entry: strings.Repeat("ee", 32), State: RaceOpen,
		entry: &storerec.Entry{Hold: &storerec.Hold{Deadline: 50}}}}, RaceOptions{})
	if n := tip.n.Load(); n != 2 {
		t.Fatalf("%d reads of the tip after a step outside a pass", n)
	}
}

// retryDelay: a minute, doubling, at most an hour.
func TestRetryDelay(t *testing.T) {
	for n, want := range map[int]time.Duration{0: 0, 1: time.Minute, 2: 2 * time.Minute, 3: 4 * time.Minute, 7: time.Hour, 8: time.Hour, 99: time.Hour} {
		if got := retryDelay(n); got != want {
			t.Fatalf("retryDelay(%d) = %s, want %s", n, got, want)
		}
	}
}

// A retire whose save fails leaves the race as its file says, and the
// delete waits for the next pass.
func TestRetireRollsBackWhenTheSaveFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through a read-only directory")
	}
	r := newRaceRig(t)
	race := r.put("gone", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	if _, err := r.p.Delete(r.ctx, testBucket, sha256.Sum256([]byte("id")), "gone", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	races, err := r.p.racesForTick()
	if err != nil || len(races) != 1 {
		t.Fatalf("races: %v %d", err, len(races))
	}
	if err := os.Chmod(r.p.racesDir(), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(r.p.racesDir(), 0o700) })
	r.p.applyRetires(races)
	if got := races[0]; got.Retired != "" || got.State != RaceOpen || got.Closing {
		t.Fatalf("a retire that was not saved changed the race: %q %s %v", got.Retired, got.State, got.Closing)
	}
	if len(r.p.retires) != 1 {
		t.Fatalf("%d deletes left for the next pass, want 1", len(r.p.retires))
	}
	if err := os.Chmod(r.p.racesDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	r.p.applyRetires(races)
	if got, _ := r.p.Race(race.Entry); got.Retired == "" || got.State != RaceClosed {
		t.Fatalf("the next pass did not retire it: %q %s", got.Retired, got.State)
	}
	if len(r.p.retires) != 0 {
		t.Fatal("an applied delete is still kept")
	}
}
