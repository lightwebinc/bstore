package publish

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/lightwebinc/bcommon/funding"
)

// repairProofs cuts every proof the state keeps to the minimal path: a
// record made from a node's proof by an earlier build may flag a sibling
// leaf as a transaction id too, and the hosts refuse every carrier on such
// a tree. It returns how many records changed; the caller saves. Called
// before anything spends.
func (p *Publisher) repairProofs() int {
	n := 0
	fix := func(txid, raw string, bump *string) {
		if *bump == "" {
			return
		}
		tx, err := funding.Rebuild(raw, *bump, "")
		if err != nil || tx.MerklePath == nil || tx.TxID().String() != txid {
			return
		}
		if m := funding.BumpHex(tx.MerklePath); m != *bump {
			*bump = m
			n++
		}
	}
	for _, s := range []*treeSet{&p.st.Piece, &p.st.Entry, &p.st.Outcome} {
		if s.Tree != nil {
			fix(s.Tree.Txid, s.Tree.RawHex, &s.Tree.BumpHex)
		}
		for i := range s.Trees {
			fix(s.Trees[i].Txid, s.Trees[i].RawHex, &s.Trees[i].BumpHex)
		}
		for i := range s.Ahead {
			fix(s.Ahead[i].Txid, s.Ahead[i].RawHex, &s.Ahead[i].BumpHex)
		}
	}
	for i := range p.st.Pending {
		fix(p.st.Pending[i].Tree.Txid, p.st.Pending[i].Tree.RawHex, &p.st.Pending[i].Tree.BumpHex)
	}
	for i := range p.st.Holds {
		fix(p.st.Holds[i].Txid, p.st.Holds[i].RawHex, &p.st.Holds[i].BumpHex)
	}
	return n
}

// Prune drops what the state no longer needs and says so: the hold
// records whose story is over and the trees spent out. The state is saved
// under the same lock every take holds, so it is kept small.
func (p *Publisher) Prune() {
	holds, err := p.PruneHolds()
	if err != nil {
		p.cfg.Logf("publisher state: %v", err)
	}
	trees, err := p.pruneTrees()
	if err != nil {
		p.cfg.Logf("publisher state: %v", err)
	}
	if holds > 0 || trees > 0 {
		p.cfg.Logf("publisher state: %d hold record(s) and %d spent tree(s) dropped", holds, trees)
	}
}

// PruneHolds drops the hold records whose story is over: reclaimed and
// seen mined, spent by something else, or named by an entry whose race is
// settled, reclaimed or spent. It returns how many were dropped.
func (p *Publisher) PruneHolds() (int, error) {
	over, err := p.racesOver()
	if err != nil {
		return 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	keep := p.st.Holds[:0]
	for _, h := range p.st.Holds {
		switch {
		case h.Reclaim != nil && h.Reclaim.Height > 0,
			h.Entry == "spent elsewhere",
			h.Used && over[h.Entry]:
			n++
		default:
			keep = append(keep, h)
		}
	}
	if n == 0 {
		return 0, nil
	}
	p.st.Holds = slices.Clone(keep)
	return n, p.saveLocked()
}

// racesOver is the entry of every race that ended: settled, reclaimed or
// spent. Only the race files' state is read, not their entries.
func (p *Publisher) racesOver() (map[string]bool, error) {
	ents, err := os.ReadDir(p.racesDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	over := map[string]bool{}
	for _, d := range ents {
		if !strings.HasSuffix(d.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(p.racesDir(), d.Name()))
		if err != nil {
			continue
		}
		var r struct {
			Entry string `json:"entry"`
			State string `json:"state"`
		}
		if json.Unmarshal(raw, &r) != nil {
			continue
		}
		switch r.State {
		case RaceSettled, RaceReclaimd, RaceSpent:
			over[r.Entry] = true
		}
	}
	return over, nil
}

// pruneTrees drops from each kind's history the trees spent out that are
// not the current one: nothing is left on them for a sweep to take, and
// no carrier is minted on them again. It returns how many were dropped.
func (p *Publisher) pruneTrees() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, s := range []*treeSet{&p.st.Piece, &p.st.Entry, &p.st.Outcome} {
		keep := s.Trees[:0]
		for _, t := range s.Trees {
			if t.Remaining() == 0 && (s.Tree == nil || s.Tree.Txid != t.Txid) {
				n++
				continue
			}
			keep = append(keep, t)
		}
		s.Trees = slices.Clone(keep)
	}
	if n == 0 {
		return 0, nil
	}
	return n, p.saveLocked()
}
