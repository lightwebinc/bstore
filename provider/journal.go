package provider

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/lightwebinc/bcommon/payee"
)

// The state on disk is a checkpoint and a log. The checkpoint is
// state.json, the whole State, written whole; the log is state.log.<gen>,
// one JSON row a line, each a change to the state appended as it is made
// and synced in groups. A start loads the checkpoint and applies, in
// order, every log whose generation is the checkpoint's or later; a
// checkpoint is written every checkpointEvery when something changed, or
// when the log passes checkpointBytes, and on a stop, never on the
// decision path or the feed (2026-11-05: the whole state, 150 to 250 MB on
// a lab provider, was copied under p.mu and written for every save, and
// the saves ran back to back; the feed and the workers stalled behind
// them).
//
// A row is appended under p.mu (record), so the rows and the checkpoint
// agree on their order: a checkpoint rotates the log and copies the state
// in one critical section, and a row appended after that goes to the next
// generation. A change a writer is answered for waits for the sync that
// covers its row (wait); any other row is synced with the next one that
// waits, or by the background pass. Every row is idempotent: one applied
// twice leaves what one application leaves.
//
// Rows before this log were files of their own under ads/, one per
// advertisement, entry or funding tree (adRow); a start still reads them,
// and the first checkpoint drops them.

// logRow is one change. A row names what it sets; a field left empty sets
// nothing.
type logRow struct {
	// Ad is a pending advertisement signed and journaled before it reaches
	// the settlement leg, with the holding and the undecided entries it
	// answers for (the funding output it spends is marked used with it).
	// AdDone is a pending advertisement finished or dropped, by txid.
	Ad     *PendingAd `json:"ad,omitempty"`
	AdDone string     `json:"adDone,omitempty"`
	// Hold is a holding as it stands; Dropped a holding dropped, by root.
	Hold    *Holding `json:"holding,omitempty"`
	Dropped string   `json:"dropped,omitempty"`
	// Entries are entries as they stand.
	Entries []EntryRec `json:"entries,omitempty"`
	// Tree is a funding tree signed for coin and not yet adopted; Spent a
	// funding output used.
	Tree  *PendingTree `json:"tree,omitempty"`
	Spent *spentRow    `json:"spent,omitempty"`
	// Pieces are unclaimed pieces as they stand, by object hash; Claimed
	// are unclaimed records gone (claimed by a closure, released or evicted).
	Pieces  map[string]*Unclaimed `json:"pieces,omitempty"`
	Claimed []string              `json:"claimed,omitempty"`
	// Deletes are delete entries seen, by bucket/key.
	Deletes map[string]*DeleteRec `json:"deletes,omitempty"`
	// Out are outbox rows queued; OutDone outbox rows sent (host, topic,
	// txid).
	Out     []Outgoing `json:"out,omitempty"`
	OutDone []Outgoing `json:"outDone,omitempty"`
	// Payouts are settlements found, by entry; MadeGood lost remittances
	// made good, by txid.
	Payouts  map[string]*Payout `json:"payouts,omitempty"`
	MadeGood map[string]string  `json:"madeGood,omitempty"`
	// Held are payments held for confirmation; Unheld those ended. Free
	// are free challenge counts, by hold outpoint (zero forgets one). Owed
	// replaces the renewals owed. Settled and Unsettleable are the payee
	// book's additions.
	Held         map[string]HeldRec   `json:"held,omitempty"`
	Unheld       []string             `json:"unheld,omitempty"`
	Free         map[string]int       `json:"free,omitempty"`
	Owed         *[]Owed              `json:"owed,omitempty"`
	Settled      []string             `json:"settled,omitempty"`
	Unsettleable []payee.Unsettleable `json:"unsettleable,omitempty"`
}

type spentRow struct {
	Tree string `json:"tree"`
	Vout uint32 `json:"vout"`
}

// adRow is a row as the files under ads/ kept it before the log.
type adRow struct {
	Ad      PendingAd    `json:"ad"`
	Holding Holding      `json:"holding"`
	Entries []EntryRec   `json:"entries,omitempty"`
	Tree    *PendingTree `json:"tree,omitempty"`
}

// apply applies a row to st, in a fixed order.
func (st *State) apply(r *logRow) {
	if r.Tree != nil {
		id := r.Tree.Tree.Txid
		if st.pendingTree(id) == nil && !st.adopted(id) && st.mintedAhead(id) == nil {
			st.PendingTrees = append(st.PendingTrees, *r.Tree)
		}
	}
	if r.Spent != nil {
		st.markSpent(r.Spent.Tree, r.Spent.Vout)
	}
	if pa := r.Ad; pa != nil && pa.Txid != "" {
		st.markSpent(pa.Tree, pa.Vout)
		if !slices.ContainsFunc(st.PendingAds, func(x PendingAd) bool { return x.Txid == pa.Txid }) {
			st.PendingAds = append(st.PendingAds, *pa)
		}
	}
	if r.AdDone != "" {
		st.dropPendingAd(r.AdDone)
	}
	if h := r.Hold; h != nil && h.Root != "" {
		c := *h
		st.Holdings[c.Root] = &c
	}
	if r.Dropped != "" {
		delete(st.Holdings, r.Dropped)
	}
	for i := range r.Entries {
		e := r.Entries[i]
		st.Entries[e.C] = &e
	}
	for k, u := range r.Pieces {
		st.Unclaimed[k] = u
	}
	for _, k := range r.Claimed {
		delete(st.Unclaimed, k)
	}
	for k, d := range r.Deletes {
		st.Deletes[k] = d
	}
	for _, o := range r.Out {
		if !slices.ContainsFunc(st.Outbox, func(x Outgoing) bool { return x.Host == o.Host && x.Topic == o.Topic && x.Txid == o.Txid }) {
			st.Outbox = append(st.Outbox, o)
		}
	}
	for _, o := range r.OutDone {
		st.Outbox = slices.DeleteFunc(st.Outbox, func(x Outgoing) bool { return x.Host == o.Host && x.Topic == o.Topic && x.Txid == o.Txid })
	}
	for k, v := range r.Payouts {
		st.Payouts[k] = v
	}
	for k, v := range r.MadeGood {
		if st.MadeGood == nil {
			st.MadeGood = map[string]string{}
		}
		st.MadeGood[k] = v
	}
	for k, v := range r.Held {
		if st.Held == nil {
			st.Held = map[string]HeldRec{}
		}
		st.Held[k] = v
	}
	for _, k := range r.Unheld {
		delete(st.Held, k)
	}
	for k, v := range r.Free {
		if v == 0 {
			delete(st.Free, k)
		} else {
			st.Free[k] = v
		}
	}
	if r.Owed != nil {
		st.Owed = slices.Clone(*r.Owed)
	}
	for _, id := range r.Settled {
		st.AddSettled(id)
	}
	for _, u := range r.Unsettleable {
		st.AddUnsettleable(u.Txid, u.Why)
	}
}

// stateLog is the log's current file and what of it is synced.
type stateLog struct {
	dir string

	mu      sync.Mutex
	f       *os.File
	gen     uint64
	size    int64
	seq     uint64 // rows written
	synced  uint64 // rows synced
	syncing bool
	done    *sync.Cond
	err     error
}

func logPath(dir string, gen uint64) string {
	return filepath.Join(dir, "state.log."+strconv.FormatUint(gen, 10))
}

// logGens are the generations of the logs in dir, ascending.
func logGens(dir string) ([]uint64, error) {
	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var gens []uint64
	for _, n := range names {
		if g, ok := strings.CutPrefix(n.Name(), "state.log."); ok {
			if v, err := strconv.ParseUint(g, 10, 64); err == nil {
				gens = append(gens, v)
			}
		}
	}
	slices.Sort(gens)
	return gens, nil
}

// openLog opens generation gen for appending.
func openLog(dir string, gen uint64) (*stateLog, error) {
	l := &stateLog{dir: dir}
	l.done = sync.NewCond(&l.mu)
	if err := l.open(gen); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *stateLog) open(gen uint64) error {
	f, err := os.OpenFile(logPath(l.dir, gen), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	l.f, l.gen, l.size = f, gen, st.Size()
	return nil
}

// append writes a row and returns its sequence; the caller holds p.mu.
func (l *stateLog) append(r *logRow) (uint64, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	raw = append(raw, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, l.err
	}
	if _, err := l.f.Write(raw); err != nil {
		l.err = err
		return 0, err
	}
	l.size += int64(len(raw))
	l.seq++
	return l.seq, nil
}

// wait returns once the row seq is synced: the waiters share each sync.
func (l *stateLog) wait(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.synced < seq && l.err == nil {
		if l.syncing {
			l.done.Wait()
			continue
		}
		l.syncing = true
		f, target := l.f, l.seq
		l.mu.Unlock()
		err := f.Sync()
		l.mu.Lock()
		l.syncing = false
		if err != nil {
			l.err = err
		} else if l.synced < target {
			l.synced = target
		}
		l.done.Broadcast()
	}
	return l.err
}

// flush syncs every row written so far.
func (l *stateLog) flush() error {
	l.mu.Lock()
	seq := l.seq
	l.mu.Unlock()
	return l.wait(seq)
}

// rotate syncs and closes the current generation and opens the next; the
// caller holds p.mu, so no row lands between the rotation and the
// checkpoint's copy of the state.
func (l *stateLog) rotate() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.syncing {
		l.done.Wait()
	}
	if err := l.f.Sync(); err != nil {
		l.err = err
		return 0, err
	}
	l.synced = l.seq
	_ = l.f.Close()
	if err := l.open(l.gen + 1); err != nil {
		l.err = err
		return 0, err
	}
	l.done.Broadcast()
	return l.gen, nil
}

// bytes is the current generation's size.
func (l *stateLog) bytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// prune removes the generations before gen: a checkpoint has them.
func (l *stateLog) prune(gen uint64) {
	gens, err := logGens(l.dir)
	if err != nil {
		return
	}
	for _, g := range gens {
		if g < gen {
			_ = os.Remove(logPath(l.dir, g))
		}
	}
}

func (l *stateLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// maxLogRow bounds one row's line.
const maxLogRow = 64 << 20

// replayLog applies a log's rows to st, in order. A last line a crash cut
// short is passed over.
func (st *State) replayLog(path string) (int, error) {
	f, err := os.Open(path) //nolint:gosec // the provider's own data directory
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), maxLogRow)
	n := 0
	for sc.Scan() {
		var r logRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			// The line a crash cut short is the last; anything else is a
			// log this provider did not write.
			break
		}
		st.apply(&r)
		n++
	}
	if err := sc.Err(); err != nil {
		return n, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return n, nil
}

// replayLogs applies every generation from the checkpoint's on, and
// returns the generation after the last.
func (st *State) replayLogs(dir string, logf func(string, ...any)) (uint64, error) {
	gens, err := logGens(dir)
	if err != nil {
		return 0, err
	}
	next := st.LogGen
	for _, g := range gens {
		if g < st.LogGen {
			continue
		}
		n, err := st.replayLog(logPath(dir, g))
		if err != nil {
			return 0, err
		}
		if n > 0 {
			logf("bstore-provider: state log %d: %d rows applied", g, n)
		}
		next = g + 1
	}
	return next, nil
}

// record appends a row; the caller holds p.mu. It returns the row's
// sequence for wait, and 0 when the row could not be written (the
// change stands in memory and the next checkpoint has it).
func (p *Provider) record(r *logRow) uint64 {
	p.dirty = true
	seq, err := p.log.append(r)
	if err != nil {
		p.logf("state log: %v", err)
		return 0
	}
	return seq
}

// wait returns once the row seq is durable; the caller does not hold p.mu.
func (p *Provider) wait(seq uint64) error {
	if seq == 0 {
		return errors.New("the state log could not be written")
	}
	return p.log.wait(seq)
}

// journalAd records pa as pending, with the holding and undecided entries
// it answers for, and returns once the row is durable.
func (p *Provider) journalAd(pa PendingAd) error {
	p.mu.Lock()
	row := &logRow{Ad: &pa}
	if h := p.st.Holdings[pa.Root]; h != nil {
		c := *h
		row.Hold = &c
		for _, c := range h.Entries {
			if e := p.st.Entries[c]; e != nil && e.Decision == "" {
				row.Entries = append(row.Entries, *e)
			}
		}
	}
	p.st.markSpent(pa.Tree, pa.Vout)
	p.st.PendingAds = append(p.st.PendingAds, pa)
	seq := p.record(row)
	p.mu.Unlock()
	return p.wait(seq)
}

// journalEntry records entry c as it stands, for a writer answered that
// the entry is taken, and returns once the row is durable.
func (p *Provider) journalEntry(c string) error {
	p.mu.Lock()
	e := p.st.Entries[c]
	if e == nil {
		p.mu.Unlock()
		return nil
	}
	seq := p.record(&logRow{Entries: []EntryRec{*e}})
	p.mu.Unlock()
	return p.wait(seq)
}

// journalTree records a pending tree, for the tree to reach the leg.
func (p *Provider) journalTree(pt PendingTree) error {
	p.mu.Lock()
	seq := p.record(&logRow{Tree: &pt})
	p.mu.Unlock()
	return p.wait(seq)
}

// holdRow is a holding as a row; the caller holds p.mu.
func holdRow(h *Holding) *logRow {
	c := *h
	return &logRow{Hold: &c}
}

// replayAds brings back what the state lacks of the rows a stopped run
// left as files under ads/ (before the log), and returns their files, for
// the next checkpoint to cover.
func (st *State) replayAds(dir string, logf func(string, ...any)) ([]string, error) {
	names, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []string
	for _, n := range names {
		file := filepath.Join(dir, n.Name())
		if !strings.HasSuffix(n.Name(), ".json") {
			continue // a write a crash cut off before its rename
		}
		raw, err := os.ReadFile(file) //nolint:gosec // the provider's own data directory
		if err != nil {
			return nil, err
		}
		var row adRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		files = append(files, file)
		st.replayAd(&row)
		logf("bstore-provider: advertisement journal row %s read", n.Name())
	}
	return files, nil
}

func (st *State) replayAd(row *adRow) {
	pa := row.Ad
	if row.Tree != nil {
		st.apply(&logRow{Tree: row.Tree})
		return
	}
	if pa.Txid == "" {
		// An entry's row: the entry, if the state lost it.
		for i := range row.Entries {
			e := row.Entries[i]
			if st.Entries[e.C] == nil {
				st.Entries[e.C] = &e
			}
		}
		return
	}
	for i := range st.PendingAds {
		if st.PendingAds[i].Txid == pa.Txid {
			return
		}
	}
	h := st.Holdings[pa.Root]
	if h != nil && h.Ad != nil && h.Ad.Expiry >= pa.Expiry {
		return
	}
	if h == nil {
		c := row.Holding
		h = &c
		st.Holdings[pa.Root] = h
	}
	h.Lease = max(h.Lease, pa.Expiry)
	for i := range row.Entries {
		e := row.Entries[i]
		if st.Entries[e.C] == nil {
			st.Entries[e.C] = &e
		}
	}
	st.markSpent(pa.Tree, pa.Vout)
	st.PendingAds = append(st.PendingAds, pa)
}

// dropJournal removes the files a checkpoint covered.
func dropJournal(files []string) {
	for _, f := range files {
		_ = os.Remove(f)
	}
}
