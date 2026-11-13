package publish

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

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
// no carrier is minted on them again. A tree the outbox still names (a
// piece to mint later spends it) is kept until that mint. It returns how
// many were dropped.
func (p *Publisher) pruneTrees() (int, error) {
	named := p.outbox.treesNamed()
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock()
	n := 0
	for _, s := range []*treeSet{&p.st.Piece, &p.st.Entry, &p.st.Outcome} {
		keep := s.Trees[:0]
		for _, t := range s.Trees {
			if at, ok := p.spentAt[t.Txid]; p.pinned[t.Txid] > 0 || (ok && now.Sub(at) < treeGrace) {
				keep = append(keep, t)
				continue
			}
			if t.Remaining() == 0 && !named[t.Txid] && (s.Tree == nil || s.Tree.Txid != t.Txid) {
				n++
				continue
			}
			keep = append(keep, t)
		}
		s.Trees = slices.Clone(keep)
	}
	for txid, at := range p.spentAt {
		if now.Sub(at) >= treeGrace {
			delete(p.spentAt, txid)
		}
	}
	if n == 0 {
		return 0, nil
	}
	return n, p.saveLocked()
}

// A put reserves outputs of a tree and writes the pieces to mint on them
// to the outbox afterwards, and the outbox is read here before the state is
// locked: a tree whose last outputs a put in flight had just taken looked
// spent out and unnamed, was dropped, and the pieces on those outputs could
// never be minted; and the batch a put writes them to is not listed until
// the put's first sends are done. So a spend pins its tree (pinTree, inside
// the spend's own hold of mu) until its caller has minted on the outputs,
// or failed, or the batch naming the tree has closed (releaseTree), however
// long that takes; and a tree is kept treeGrace longer, for a reader of the
// outbox that started before the close. Past that the outbox is the record: it names
// the tree of every piece still to mint.
const treeGrace = 10 * time.Minute

// pinTree pins a tree for a spend; the caller holds mu.
func (p *Publisher) pinTree(txid string) {
	if p.pinned == nil {
		p.pinned = map[string]int{}
	}
	p.pinned[txid]++
}

// releaseTree ends a spend's pin of its tree. A nil tree is no spend.
func (p *Publisher) releaseTree(tree *transaction.Transaction) {
	if tree == nil {
		return
	}
	txid := tree.TxID().String()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pinned[txid] > 1 {
		p.pinned[txid]--
	} else {
		delete(p.pinned, txid)
	}
	if p.spentAt == nil {
		p.spentAt = map[string]time.Time{}
	}
	p.spentAt[txid] = p.clock()
}

// clock is the publisher's time.
func (p *Publisher) clock() time.Time {
	if p.cfg.Now != nil {
		return p.cfg.Now()
	}
	return time.Now()
}
