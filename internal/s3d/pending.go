package s3d

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bstore/storerec"
)

// The gateway's own pending entries: what it wrote that no token names
// yet, served at once (read your writes, spec sections 4.4 and 16.3). An
// entry leaves when a token the reader walks names its commitment (as a
// put, a removal or in consumed), when an anchored entry for its key is
// the same or newer (by created, then commitment), or after pendingKeep.
//
// They are kept in pending.log, one line appended (and synced) per change,
// and rewritten whole only when it holds four times the live entries; the
// tokens the reader holds are in state.json, rewritten when one moves.

const (
	pendingKeep = 7 * 24 * time.Hour
	maxPending  = 10_000
)

type pendingEntry struct {
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
	C       string `json:"c"` // hash byte order, hex
	Created uint64 `json:"created"`
	Kind    uint64 `json:"kind"`
	Record  []byte `json:"record"`
	Seen    int64  `json:"seen"` // Unix seconds written
	entry   *storerec.Entry
}

func (p *pendingEntry) decoded() *storerec.Entry {
	if p.entry == nil {
		e, err := storerec.DecodeEntry(p.Record)
		if err == nil {
			p.entry = e
		}
	}
	return p.entry
}

// newer reports whether (created, c) sorts after (oc, occ).
func newer(created uint64, c string, oc uint64, occ string) bool {
	return created > oc || created == oc && c > occ
}

type logLine struct {
	Add  *pendingEntry `json:"add,omitempty"`
	Drop string        `json:"drop,omitempty"`
}

// state is the gateway's state: the tokens held and the pending index.
type state struct {
	dir  string
	now  func() time.Time
	mu   sync.Mutex
	held map[string]storerec.Held
	// byKey indexes pending entries: bucket -> key -> entries; byC by
	// commitment.
	byKey map[string]map[string][]*pendingEntry
	byC   map[string]*pendingEntry
	log   *os.File
	lines int
	// unsent are the entries a put answered 503 for (made, not yet taken
	// by a quorum), by commitment, with when: the only ones a retry of the
	// same write is answered from. Kept in memory.
	unsent map[string]time.Time
}

func openState(dir string, now func() time.Time) (*state, error) {
	s := &state{dir: dir, now: now, held: map[string]storerec.Held{}, byKey: map[string]map[string][]*pendingEntry{},
		byC: map[string]*pendingEntry{}}
	raw, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		var st struct {
			Held map[string]storerec.Held `json:"held"`
		}
		if err := json.Unmarshal(raw, &st); err != nil {
			return nil, err
		}
		if st.Held != nil {
			s.held = st.Held
		}
	}
	if f, err := os.Open(filepath.Join(dir, "pending.log")); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			var l logLine
			if json.Unmarshal(sc.Bytes(), &l) != nil {
				continue // a last line cut short by a crash
			}
			if l.Add != nil {
				s.index(l.Add)
			} else if l.Drop != "" {
				s.unindex(l.Drop)
			}
		}
		f.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	s.mu.Lock()
	s.expireLocked(false)
	err = s.compactLocked()
	s.mu.Unlock()
	return s, err
}

func (s *state) index(p *pendingEntry) {
	if _, ok := s.byC[p.C]; ok {
		return
	}
	m := s.byKey[p.Bucket]
	if m == nil {
		m = map[string][]*pendingEntry{}
		s.byKey[p.Bucket] = m
	}
	m[p.Key] = append(m[p.Key], p)
	s.byC[p.C] = p
}

func (s *state) unindex(c string) bool {
	p, ok := s.byC[c]
	if !ok {
		return false
	}
	delete(s.byC, c)
	list := s.byKey[p.Bucket][p.Key]
	keep := list[:0]
	for _, x := range list {
		if x.C != c {
			keep = append(keep, x)
		}
	}
	if len(keep) == 0 {
		delete(s.byKey[p.Bucket], p.Key)
		if len(s.byKey[p.Bucket]) == 0 {
			delete(s.byKey, p.Bucket)
		}
	} else {
		s.byKey[p.Bucket][p.Key] = keep
	}
	return true
}

// appendLocked writes one line to the log and syncs it.
func (s *state) appendLocked(l logLine) error {
	if s.log == nil {
		f, err := os.OpenFile(filepath.Join(s.dir, "pending.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		s.log = f
	}
	raw, err := json.Marshal(l)
	if err != nil {
		return err
	}
	if _, err := s.log.Write(append(raw, '\n')); err != nil {
		return err
	}
	s.lines++
	if s.lines > 4*len(s.byC)+1024 {
		return s.compactLocked()
	}
	return s.log.Sync()
}

// compactLocked rewrites the log as the live entries.
func (s *state) compactLocked() error {
	if s.log != nil {
		s.log.Close()
		s.log = nil
	}
	var b strings.Builder
	for _, p := range s.byC {
		raw, err := json.Marshal(logLine{Add: p})
		if err != nil {
			return err
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	if err := writeSynced(filepath.Join(s.dir, "pending.log"), []byte(b.String())); err != nil {
		return err
	}
	s.lines = len(s.byC)
	return nil
}

// Held and SetHeld are the reader's HeldStore.
func (s *state) Held(chain string) *storerec.Held {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.held[chain]
	if !ok {
		return nil
	}
	return &h
}

func (s *state) SetHeld(chain string, h storerec.Held) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.held[chain]; ok && cur.Seq > h.Seq {
		return nil
	}
	s.held[chain] = h
	raw, err := json.Marshal(map[string]any{"held": s.held})
	if err != nil {
		return err
	}
	return writeSynced(filepath.Join(s.dir, "state.json"), raw)
}

// add records an entry the gateway wrote.
func (s *state) add(e *storerec.Entry, rec []byte, c [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.byC) >= maxPending {
		s.expireLocked(true)
		if err := s.compactLocked(); err != nil {
			return err
		}
	}
	p := &pendingEntry{Bucket: e.Bucket, Key: e.Key, C: hex.EncodeToString(c[:]), Created: e.Created, Kind: e.Kind,
		Record: rec, Seen: s.now().Unix(), entry: e}
	s.index(p)
	return s.appendLocked(logLine{Add: p})
}

// named drops the pending entries a token names (its changes' commitments
// and its consumed): the map decides those keys from now on.
func (s *state) named(t *storerec.Bucket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	drop := func(c [32]byte) {
		x := hex.EncodeToString(c[:])
		if s.unindex(x) {
			_ = s.appendLocked(logLine{Drop: x})
		}
	}
	for _, ch := range t.Changes {
		drop(ch.C)
	}
	for _, c := range t.Consumed {
		drop(c)
	}
}

// newest is the newest pending entry for key, or nil.
func (s *state) newest(bucket, key string) *pendingEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *pendingEntry
	for _, p := range s.byKey[bucket][key] {
		if best == nil || newer(p.Created, p.C, best.Created, best.C) {
			best = p
		}
	}
	return best
}

// settle drops the pending entries for key that an anchored entry
// (created, c) is the same as or newer than.
func (s *state) settle(bucket, key string, created uint64, c string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var drop []string
	for _, p := range s.byKey[bucket][key] {
		if p.C == c || !newer(p.Created, p.C, created, c) {
			drop = append(drop, p.C)
		}
	}
	for _, x := range drop {
		if s.unindex(x) {
			_ = s.appendLocked(logLine{Drop: x})
		}
	}
}

// inRange is the newest pending entry of every key of bucket that starts
// with prefix and sorts after after, ascending by key.
func (s *state) inRange(bucket, prefix, after string) []*pendingEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*pendingEntry
	for k, list := range s.byKey[bucket] {
		if !strings.HasPrefix(k, prefix) || k <= after {
			continue
		}
		var best *pendingEntry
		for _, p := range list {
			if best == nil || newer(p.Created, p.C, best.Created, best.C) {
				best = p
			}
		}
		out = append(out, best)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// expire drops entries older than pendingKeep.
func (s *state) expire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expireLocked(false) > 0 {
		_ = s.compactLocked()
	}
}

// expireLocked drops entries older than pendingKeep (and, with halve, the
// older half), returning how many.
func (s *state) expireLocked(halve bool) int {
	cut := s.now().Add(-pendingKeep).Unix()
	if halve {
		var seen []int64
		for _, p := range s.byC {
			seen = append(seen, p.Seen)
		}
		sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })
		if len(seen) > 0 {
			cut = max(cut, seen[len(seen)/2])
		}
	}
	var drop []string
	for c, p := range s.byC {
		if p.Seen <= cut {
			drop = append(drop, c)
		}
	}
	for _, c := range drop {
		s.unindex(c)
	}
	return len(drop)
}
