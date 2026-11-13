package module

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	bmodule "github.com/lightwebinc/bcommon/module"
	"github.com/lightwebinc/bcommon/replay"

	"github.com/lightwebinc/bstore/storerec"
)

// Replay by position. A peer that answers since (the Go module from this
// version on) is asked for what it took after the position this host last
// reached with it, in the peer's order, in place of the unanchored, mined
// and attached phases: those ask one question for every root and carrier
// the host holds and page every unanchored entry again at each walk,
// whether or not anything is missing. A peer that refuses the class is
// walked that way, as before.
//
// The position reached is kept per bucket and peer, in memory and in
// PositionsFile, so a restart asks for nothing it was already answered. It
// moves only over pages the walk answered nothing from: an object answered
// is admitted after the call, so the next walk reads its page again, finds
// it applied, and moves on. An object answered in SinceTries walks and
// still not applied is passed over, so that one the host never admits does
// not hold the position for good.

// PositionsFile is the file of positions reached, in the state directory.
const PositionsFile = "bstore-replay.json"

// SinceTries is how many walks answer an object from a since page before
// the position moves past it unapplied.
const SinceTries = 3

// The keys a walk keeps for its later calls and phases (replay.Walk.Find).
const (
	sinceRefused = "since|refused"
	sinceDirty   = "since|dirty"
)

// Positions keeps the positions replay reached, by <bucket>|<peer>.
type Positions interface {
	Load() (map[string]string, error)
	Save(map[string]string) error
}

// FilePositions is Positions in one JSON file, replaced atomically.
type FilePositions struct {
	Path string
	mu   sync.Mutex
}

// Load implements Positions; a file that does not exist holds none.
func (f *FilePositions) Load() (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	if err := json.Unmarshal(raw, &out); err != nil {
		// A file cut short by a crash costs one full reading of each peer.
		return map[string]string{}, nil
	}
	return out, nil
}

// Save implements Positions.
func (f *FilePositions) Save(m map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(f.Path), 0o700); err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}

// reachedAt is the position reached with a peer for a bucket, "" for none.
func (s *Service) reachedAt(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reached[key]
}

// sinceTry is how often walks answered an object from a since page, and
// the position it was answered at.
type sinceTry struct {
	n   int
	pos string
}

// reach moves the position reached and forgets the tries at or behind it:
// those ahead keep their counts, so an object the host never admits is
// passed over once, not offered again each time an earlier page clears.
func (s *Service) reach(key, pos string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reached[key] = pos
	s.positionsDirty = true
	m := s.sinceTries[key]
	for txid, t := range m {
		if t.pos <= pos {
			delete(m, txid)
		}
	}
	if len(m) == 0 {
		delete(s.sinceTries, key)
	}
}

// tried counts one more walk that answered txid from a since page at pos,
// and reports whether it had been answered SinceTries times already.
func (s *Service) tried(key string, txid chainhash.Hash, pos string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.sinceTries[key]
	if m == nil {
		m = map[chainhash.Hash]sinceTry{}
		s.sinceTries[key] = m
	}
	t := m[txid]
	if t.n >= SinceTries {
		return true
	}
	m[txid] = sinceTry{n: t.n + 1, pos: pos}
	return false
}

// listedPrefix marks, in the saved positions, where since lists an output
// it listed anywhere but at its first sight: listed|<bucket>|<txid>.<vout>.
const listedPrefix = "listed|"

// loadPositions reads saved positions: those replay reached, by
// <bucket>|<peer>, and those since lists outputs at.
func (s *Service) loadPositions(m map[string]string) {
	for k, v := range m {
		if rest, ok := strings.CutPrefix(k, listedPrefix); ok {
			if pos, err := strconv.ParseInt(v, 10, 64); err == nil && pos > 0 {
				s.relisted[rest] = pos
			}
			continue
		}
		if storerec.SincePosition(v) {
			s.reached[k] = v
		}
	}
}

// savePositions writes the positions reached and listed, outside the
// index's lock.
func (s *Service) savePositions() {
	if s.positions == nil {
		return
	}
	s.mu.Lock()
	m := make(map[string]string, len(s.reached)+len(s.relisted))
	for k, v := range s.reached {
		m[k] = v
	}
	for k, pos := range s.relisted {
		m[listedPrefix+k] = strconv.FormatInt(pos, 10)
	}
	s.positionsDirty = false
	s.mu.Unlock()
	if err := s.positions.Save(m); err != nil {
		s.host.Log("ls_bstore could not save the positions replay reached", map[string]any{"err": err.Error()})
		s.mu.Lock()
		s.positionsDirty = true
		s.mu.Unlock()
	}
}

// MemoryPositions is Positions in memory, for tests and for a host that
// accepts reading every peer in full after a restart.
type MemoryPositions struct {
	mu sync.Mutex
	m  map[string]string
}

// Load implements Positions.
func (p *MemoryPositions) Load() (map[string]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]string, len(p.m))
	for k, v := range p.m {
		out[k] = v
	}
	return out, nil
}

// Save implements Positions.
func (p *MemoryPositions) Save(m map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.m = make(map[string]string, len(m))
	for k, v := range m {
		p.m[k] = v
	}
	return nil
}

// sinceKind is the kind of an output a since page answered, "" for one no
// free class answers.
func sinceKind(o *replay.Output) string {
	if int(o.Vout) >= len(o.Tx.Outputs) || o.Tx.Outputs[o.Vout] == nil || o.Tx.Outputs[o.Vout].LockingScript == nil {
		return ""
	}
	script := []byte(*o.Tx.Outputs[o.Vout].LockingScript)
	switch storerec.Classify(script) {
	case storerec.ClaimEntry:
		return "entry"
	case storerec.ClaimOutcome:
		return "outcome"
	case storerec.ClaimFront:
		return "front"
	case storerec.ClaimHold:
		return "hold"
	case storerec.ClaimToken:
		return ""
	case storerec.ClaimFunding:
		if o.Vout == 0 {
			return "sweep"
		}
		return ""
	}
	if _, err := storerec.DecodeAdvertisement(script); err == nil {
		return "ad"
	}
	return ""
}

// replaySince is the phase that asks a peer for what it took after the
// position reached. refused is set by the walk's asker when the peer
// refuses a question.
func (s *Service) replaySince(ctx context.Context, b, peer string, refused *bool) replay.Phase {
	key := b + "|" + peer
	fresh := s.freshMined(ctx)
	return replay.Phase{Name: "since",
		Items: func(context.Context, *replay.Walk) ([]string, error) { return []string{"since"}, nil },
		Do: func(ctx context.Context, w *replay.Walk, _ string, page *string) (bool, error) {
			cur := *page
			if cur == "" {
				cur = s.reachedAt(key)
				*page = cur
			}
			dirty := len(w.Found(sinceDirty)) > 0
			moved := false
			defer func() {
				if moved {
					s.savePositions()
				}
			}()
			for {
				*refused = false
				outs, err := w.Ask(ctx, storerec.LookupService, map[string]string{"bucket": b, "since": cur})
				if err != nil {
					return false, err
				}
				if *refused {
					w.Find(sinceRefused)
					return true, nil
				}
				last := cur
				for _, o := range outs {
					pos := string(o.Context)
					if !storerec.SincePosition(pos) || pos <= last {
						w.Gap("the peer answered since with no position, or with positions out of order")
						return true, nil
					}
					last = pos
					kind := sinceKind(o)
					var keep bool
					switch kind {
					case "entry":
						keep = s.replayable(b, o.Tx, s.pendingWindow())
					case "outcome", "front":
						keep = s.replayable(b, o.Tx, s.otherWindow())
					case "ad", "hold":
						keep = fresh(o)
					case "sweep":
						keep = true
					}
					if !keep {
						continue
					}
					want, err := w.Wanted(ctx, o.Txid)
					if err != nil {
						return false, err
					}
					if !want {
						continue
					}
					// Answered in this walk already, by an earlier output of
					// its transaction or an earlier call: the position waits.
					if w.Sent(o.Txid) {
						dirty = true
						continue
					}
					if s.tried(key, o.Txid, pos) {
						continue
					}
					if err := w.Emit(bmodule.ReplayObject{Txid: o.Txid, BEEF: o.BEEF, Kind: kind}); err != nil {
						return false, err
					}
					dirty = true
					w.Find(sinceDirty)
				}
				if len(outs) == 0 {
					return true, nil
				}
				cur = last
				*page = cur
				if !dirty {
					s.reach(key, cur)
					moved = true
				}
				if len(outs) < PageLimit {
					return true, nil
				}
			}
		}}
}

// unlessSince is a phase of the walk as it was before since, run only
// against a peer that refused the class.
func unlessSince(p replay.Phase) replay.Phase {
	items := p.Items
	p.Items = func(ctx context.Context, w *replay.Walk) ([]string, error) {
		if len(w.Found(sinceRefused)) == 0 {
			return nil, nil
		}
		return items(ctx, w)
	}
	return p
}
