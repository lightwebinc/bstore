package s3d

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// The gateway's own pending entries: what it wrote that no token anchors
// yet, served at once (read your writes, spec sections 4.4 and 16.3). They
// are kept in state.json with the tokens the reader holds, so a restart
// keeps both. An entry leaves when an anchored entry for its key is the
// same or newer (by created, then commitment), or after pendingKeep.

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

type gwState struct {
	Held    map[string]storerec.Held `json:"held"`
	Pending []*pendingEntry          `json:"pending"`
}

// state is the gateway's state file and the pending index.
type state struct {
	path string
	now  func() time.Time
	mu   sync.Mutex
	st   gwState
	// byKey indexes pending entries: bucket -> key -> entries.
	byKey map[string]map[string][]*pendingEntry
}

func openState(dir string, now func() time.Time) (*state, error) {
	s := &state{path: filepath.Join(dir, "state.json"), now: now, byKey: map[string]map[string][]*pendingEntry{}}
	raw, err := os.ReadFile(s.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		if err := json.Unmarshal(raw, &s.st); err != nil {
			return nil, err
		}
	}
	if s.st.Held == nil {
		s.st.Held = map[string]storerec.Held{}
	}
	for _, p := range s.st.Pending {
		s.index(p)
	}
	return s, nil
}

func (s *state) index(p *pendingEntry) {
	m := s.byKey[p.Bucket]
	if m == nil {
		m = map[string][]*pendingEntry{}
		s.byKey[p.Bucket] = m
	}
	m[p.Key] = append(m[p.Key], p)
}

func (s *state) saveLocked() error {
	s.st.Pending = s.st.Pending[:0]
	for _, m := range s.byKey {
		for _, list := range m {
			s.st.Pending = append(s.st.Pending, list...)
		}
	}
	sort.Slice(s.st.Pending, func(i, j int) bool { return s.st.Pending[i].C < s.st.Pending[j].C })
	raw, err := json.Marshal(&s.st)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
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
	return os.Rename(tmp, s.path)
}

// Held and SetHeld are the reader's HeldStore.
func (s *state) Held(chain string) *storerec.Held {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.st.Held[chain]
	if !ok {
		return nil
	}
	return &h
}

func (s *state) SetHeld(chain string, h storerec.Held) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.st.Held[chain]; ok && cur.Seq > h.Seq {
		return nil
	}
	s.st.Held[chain] = h
	return s.saveLocked()
}

// add records an entry the gateway wrote.
func (s *state) add(e *storerec.Entry, rec []byte, c [32]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.byKey {
		for _, l := range m {
			n += len(l)
		}
	}
	if n >= maxPending {
		s.expireLocked(true)
	}
	s.index(&pendingEntry{Bucket: e.Bucket, Key: e.Key, C: hex.EncodeToString(c[:]), Created: e.Created, Kind: e.Kind,
		Record: rec, Seen: s.now().Unix(), entry: e})
	return s.saveLocked()
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
	list := s.byKey[bucket][key]
	keep := list[:0]
	for _, p := range list {
		if p.C == c || !newer(p.Created, p.C, created, c) {
			continue
		}
		keep = append(keep, p)
	}
	if len(keep) == len(list) {
		return
	}
	if len(keep) == 0 {
		delete(s.byKey[bucket], key)
	} else {
		s.byKey[bucket][key] = keep
	}
	_ = s.saveLocked()
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

// expire drops entries older than pendingKeep (and, when over the bound,
// the oldest half).
func (s *state) expire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(false)
	_ = s.saveLocked()
}

func (s *state) expireLocked(halve bool) {
	cut := s.now().Add(-pendingKeep).Unix()
	if halve {
		var seen []int64
		for _, m := range s.byKey {
			for _, l := range m {
				for _, p := range l {
					seen = append(seen, p.Seen)
				}
			}
		}
		sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })
		if len(seen) > 0 {
			cut = max(cut, seen[len(seen)/2])
		}
	}
	for b, m := range s.byKey {
		for k, l := range m {
			keep := l[:0]
			for _, p := range l {
				if p.Seen > cut {
					keep = append(keep, p)
				}
			}
			if len(keep) == 0 {
				delete(m, k)
			} else {
				m[k] = keep
			}
		}
		if len(m) == 0 {
			delete(s.byKey, b)
		}
	}
}
