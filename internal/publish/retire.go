package publish

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bstore/storerec"
)

// A delete ends what the writer asks of the providers for the key's earlier
// puts. The providers release the closure (spec section 12.4), so a
// settlement challenge of it can only fail, and a race that kept judging
// would ask every host and advertiser on every pass until its deadline. The
// race window is the writer's own policy (spec section 9.4); a delete
// closes it. A race with at least t passed when the delete came settles by
// rank, as any closed race does: those providers held the object and
// passed. One with fewer is closed with nothing paid, and its hold output
// is reclaimed after the deadline (section 9.6), never spent before it.
//
// Delete records the delete in retire.jsonl, synced, before it returns;
// the next RaceTick applies it to every open race of the key made before
// it, drops the key's earlier entries from entries/ (a deleted key's puts
// are never replayed or synced again), and forgets it. A put made after
// the delete is not touched.

// retireNote is a delete not yet applied to the races.
type retireNote struct {
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
	Created uint64 `json:"created"` // the delete entry's created, Unix milliseconds
	Entry   string `json:"entry"`   // the delete entry's txid
}

func (n retireNote) key() string { return n.Bucket + "\x00" + n.Key }

func (p *Publisher) retirePath() string { return filepath.Join(p.cfg.Dir, "retire.jsonl") }

// loadRetires reads the deletes not yet applied, at Open. A last line cut
// short by a crash is skipped: its Delete did not return.
func (p *Publisher) loadRetires() error {
	f, err := os.Open(p.retirePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var n retireNote
		if json.Unmarshal(sc.Bytes(), &n) == nil && n.Entry != "" {
			p.retires = append(p.retires, n)
		}
	}
	return sc.Err()
}

// noteDelete records a delete entry for the next RaceTick, synced.
func (p *Publisher) noteDelete(e *storerec.Entry, txid string) error {
	raw, err := json.Marshal(retireNote{Bucket: e.Bucket, Key: e.Key, Created: e.Created, Entry: txid})
	if err != nil {
		return err
	}
	p.retireMu.Lock()
	defer p.retireMu.Unlock()
	f, err := os.OpenFile(p.retirePath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	p.retires = append(p.retires, retireNote{Bucket: e.Bucket, Key: e.Key, Created: e.Created, Entry: txid})
	return nil
}

// applyRetires applies the deletes recorded so far to races (the pass's
// races, every one of them) and to entries/, then forgets them. A delete
// that arrives meanwhile stays for the next pass.
func (p *Publisher) applyRetires(races []*Race) {
	p.retireMu.Lock()
	notes := slices.Clone(p.retires)
	p.retireMu.Unlock()
	if len(notes) == 0 {
		return
	}
	newest := map[string]retireNote{}
	for _, n := range notes {
		if cur, ok := newest[n.key()]; !ok || n.Created > cur.Created {
			newest[n.key()] = n
		}
	}
	for _, r := range races {
		if r.entry == nil || r.Retired != "" || r.State != RaceOpen || r.Settlement != nil {
			continue
		}
		n, ok := newest[r.Bucket+"\x00"+r.entry.Key]
		if !ok || r.entry.Created >= n.Created {
			continue
		}
		if err := p.retire(r, n); err != nil {
			// Left in the note list: the next pass tries again.
			p.cfg.Logf("race %s: retire: %v", short(r.Entry), err)
			return
		}
	}
	for _, n := range newest {
		p.pruneEarlierEntries(n)
	}
	p.retireMu.Lock()
	defer p.retireMu.Unlock()
	p.retires = slices.Clone(p.retires[len(notes):])
	if err := p.writeRetiresLocked(); err != nil {
		p.cfg.Logf("retire: %v", err)
	}
}

// writeRetiresLocked rewrites retire.jsonl as the notes not yet applied.
func (p *Publisher) writeRetiresLocked() error {
	if len(p.retires) == 0 {
		err := os.Remove(p.retirePath())
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var b strings.Builder
	for _, n := range p.retires {
		raw, err := json.Marshal(n)
		if err != nil {
			return err
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	return writeFileSync(filepath.Join(p.cfg.Dir, "tmp"), p.retirePath(), []byte(b.String()))
}

// retire closes an open race whose key was deleted: for settlement by rank
// when at least t passed, else with nothing paid and the hold output left
// for the reclaim after the deadline. It saves the race.
func (p *Publisher) retire(r *Race, n retireNote) error {
	passed := uint64(len(r.Passed()))
	why := fmt.Sprintf("the key was deleted (entry %s)", short(n.Entry))
	r.Retired = why
	r.RetryAt, r.Fails = 0, 0
	if passed > 0 && passed >= r.entry.Hold.T {
		r.Closing = true
		r.Note = fmt.Sprintf("closed: %s with %d passed: settled by rank", why, passed)
	} else {
		r.State = RaceClosed
		r.Note = fmt.Sprintf("closed: %s with %d passed, fewer than t = %d: nothing is paid; the hold output is reclaimed after the deadline",
			why, passed, r.entry.Hold.T)
	}
	if err := p.saveRace(r); err != nil {
		return err
	}
	p.cfg.Logf("race %s: %s", short(r.Entry), r.Note)
	return nil
}

// entryRef is one entry of this identity, as the key index keeps it.
type entryRef struct {
	Txid    string
	Created uint64
}

// buildEntryIndex reads entries/ into the index of this identity's entries
// by bucket and key, once, at the first delete a pass applies; logEntry
// keeps it after. The directory is read without entryMu, so puts never
// wait for it: the entries logged meanwhile are kept aside and merged.
func (p *Publisher) buildEntryIndex() {
	p.entryMu.Lock()
	if p.entryIdx != nil || p.entryBuilding {
		p.entryMu.Unlock()
		return
	}
	p.entryBuilding, p.entryLate = true, nil
	p.entryMu.Unlock()
	idx := map[string][]entryRef{}
	seen := map[string]bool{}
	for _, l := range p.Entries() {
		k := l.Bucket + "\x00" + l.Key
		idx[k] = append(idx[k], entryRef{Txid: l.Txid, Created: l.Created})
		seen[l.Txid] = true
	}
	p.entryMu.Lock()
	defer p.entryMu.Unlock()
	for _, late := range p.entryLate {
		if !seen[late.ref.Txid] {
			idx[late.key] = append(idx[late.key], late.ref)
		}
	}
	p.entryIdx, p.entryBuilding, p.entryLate = idx, false, nil
}

// indexEntry adds an entry just logged to the key index.
func (p *Publisher) indexEntry(e *storerec.Entry, txid string) {
	p.entryMu.Lock()
	defer p.entryMu.Unlock()
	k, ref := e.Bucket+"\x00"+e.Key, entryRef{Txid: txid, Created: e.Created}
	switch {
	case p.entryIdx != nil:
		p.entryIdx[k] = append(p.entryIdx[k], ref)
	case p.entryBuilding:
		p.entryLate = append(p.entryLate, lateEntry{key: k, ref: ref})
	}
}

// lateEntry is an entry logged while the index was being read.
type lateEntry struct {
	key string
	ref entryRef
}

// pruneEarlierEntries removes from entries/ the key's entries made before
// the delete n: the puts it removed and any earlier delete. The delete
// itself stays, for sync and fill, until a token names it. Nothing else
// reads them: a race keeps its own copy of its entry.
func (p *Publisher) pruneEarlierEntries(n retireNote) {
	p.buildEntryIndex()
	p.entryMu.Lock()
	defer p.entryMu.Unlock()
	idx := p.entryIdx
	if idx == nil {
		return
	}
	list := idx[n.key()]
	keep := list[:0]
	for _, ref := range list {
		if ref.Txid == n.Entry || ref.Created >= n.Created {
			keep = append(keep, ref)
			continue
		}
		for _, ext := range []string{".json", ".beef"} {
			if err := os.Remove(filepath.Join(p.entriesDir(), ref.Txid+ext)); err != nil && !errors.Is(err, os.ErrNotExist) {
				p.cfg.Logf("entry %s: %v", short(ref.Txid), err)
			}
		}
	}
	if len(keep) == 0 {
		delete(idx, n.key())
	} else {
		idx[n.key()] = slices.Clone(keep)
	}
}

// The race pass keeps the races it parsed, by entry: a pass re-reads only
// the files that changed since. Only RaceTick's goroutine uses the cache,
// so the races it hands out are never shared with another caller of Races.
type cachedRace struct {
	r    *Race
	size int64
	mod  time.Time
}

// racesForTick is Races for the background pass.
func (p *Publisher) racesForTick() ([]*Race, error) {
	ents, err := os.ReadDir(p.racesDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if p.raceCache == nil {
		p.raceCache = map[string]cachedRace{}
	}
	seen := make(map[string]bool, len(ents))
	var out []*Race
	for _, d := range ents {
		name := d.Name()
		if d.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		entry := strings.TrimSuffix(name, ".json")
		info, err := d.Info()
		if err != nil {
			continue
		}
		seen[entry] = true
		if c, ok := p.raceCache[entry]; ok && c.size == info.Size() && c.mod.Equal(info.ModTime()) {
			out = append(out, c.r)
			continue
		}
		r, err := p.liveRace(entry)
		if err != nil {
			p.cfg.Logf("race %s: %v", name, err)
			continue
		}
		p.raceCache[entry] = cachedRace{r: r, size: info.Size(), mod: info.ModTime()}
		out = append(out, r)
	}
	for k := range p.raceCache {
		if !seen[k] {
			delete(p.raceCache, k)
		}
	}
	sortRaces(out)
	return out, nil
}

func sortRaces(out []*Race) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Started != out[j].Started {
			return out[i].Started < out[j].Started
		}
		return out[i].Entry < out[j].Entry
	})
}

// A race whose story is over (settled with every payee told, reclaimed, or
// its hold output spent elsewhere) moves to races/done/: the pass and the
// state's pruning read only the races still live, so their cost follows
// the open races, not every bounty this identity ever set. Its hold
// records go from the state at the same time, as PruneHolds would drop
// them. Race still finds it by entry.
func (p *Publisher) doneDir() string { return filepath.Join(p.racesDir(), "done") }

// over reports whether the race needs nothing more.
func (r *Race) over() bool {
	switch r.State {
	case RaceReclaimd, RaceSpent:
		return true
	case RaceSettled:
		return r.Remitted()
	}
	return false
}

// archiveRace moves a race that is over to races/done/ and drops its hold
// records from the state.
func (p *Publisher) archiveRace(r *Race) error {
	p.mu.Lock()
	keep := p.st.Holds[:0]
	n := 0
	for _, h := range p.st.Holds {
		if h.Used && h.Entry == r.Entry {
			n++
			continue
		}
		keep = append(keep, h)
	}
	var err error
	if n > 0 {
		p.st.Holds = slices.Clone(keep)
		err = p.saveLocked()
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(p.doneDir(), 0o700); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(p.racesDir(), r.Entry+".json"), filepath.Join(p.doneDir(), r.Entry+".json")); err != nil {
		return err
	}
	delete(p.raceCache, r.Entry)
	return nil
}

// DoneRaces counts the races moved to races/done/.
func (p *Publisher) DoneRaces() int {
	ents, err := os.ReadDir(p.doneDir())
	if err != nil {
		return 0
	}
	n := 0
	for _, d := range ents {
		if strings.HasSuffix(d.Name(), ".json") {
			n++
		}
	}
	return n
}

// retryDelay is how long a race whose step failed waits before its next
// step: a minute, doubling with each failure in a row, at most an hour.
func retryDelay(fails int) time.Duration {
	if fails < 1 {
		return 0
	}
	if fails > 7 {
		return time.Hour
	}
	return min(time.Hour, time.Minute<<(fails-1))
}

// tickTip is the tip one race pass works at: read once, and again after
// tickTipAge, so that a pass over many races asks the header source a few
// times, not once a race. It travels in the pass's context only.
type tickTip struct {
	mu sync.Mutex
	h  uint32
	at time.Time
	ok bool
}

const tickTipAge = 10 * time.Second

type tickTipKey struct{}

func withTickTip(ctx context.Context) context.Context {
	return context.WithValue(ctx, tickTipKey{}, &tickTip{})
}
