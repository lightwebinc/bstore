// Package anchor is the bucket owner's tooling (spec sections 3.4 to 3.6
// and 4.4): it creates a bucket chain, and anchors the entries of the
// owner and its writers into the chain's next tokens, exactly as frozen.
//
// The set of commitments the chain has named is rebuilt from the chain
// itself, every run, from the rooted tokens the tooling verifies against
// its own headers (bucket, then bucket-seq back to seq 0), never from a
// local cache; the fold of those tokens must be consistent. Each writer's
// unnamed entries are walked with unanchored at every host, every page
// from the start; every entry an answer returns must verify (carrier rules
// and the funding tree's proof) and carry the writer, bucket and id asked
// for, and its funding outpoint must be unspent by positive evidence from
// the spend source. The batch is folded against the current map before
// anything is minted: one key's entries ordered by created then
// commitment, the last deciding and the rest consumed; a delete of a key
// the map does not hold, and a put of the value a key already names, go to
// consumed; nothing a token of the chain named is applied again. The
// changes are split over several tokens when they do not fit one record of
// 65,536 bytes (or 1024 changes, or 1024 consumed).
package anchor

import (
	"bytes"
	"errors"
	"sort"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Step is one token's worth of a batch: its changes, ascending by key, its
// consumed commitments, ascending, and the map after it.
type Step struct {
	Changes  []storerec.Change
	Consumed [][32]byte
	Map      storerec.Map
}

// Plan is a folded batch, split into the tokens that carry it.
type Plan struct {
	Steps []Step
	// Deferred are the keys left for a later run because the map is at its
	// bound of 1,048,576 keys.
	Deferred []string
}

// keyItem is one key's resolution: its change (nil when it changes nothing)
// and the entries consumed with it.
type keyItem struct {
	key      string
	change   *storerec.Change
	consumed [][32]byte
}

// PlanBatch folds batch against the current map and the chain's named set,
// as spec section 3.4 has it, and splits the result over tokens whose
// records, as size measures them, fit the bounds. Each key's change is
// carried by exactly one token; a key whose consumed entries do not fit
// one token spills them into tokens of consumed alone before it.
func PlanBatch(current storerec.Map, named map[[32]byte]bool, batch []storerec.Applicable, size func([]storerec.Change, [][32]byte) (int, error)) (*Plan, error) {
	// One key's entries, each once, none named, puts and deletes only.
	byKey := map[string][]storerec.Applicable{}
	taken := map[[32]byte]bool{}
	for _, e := range batch {
		if named[e.C] || taken[e.C] || (e.Kind != storerec.KindPut && e.Kind != storerec.KindDelete) {
			continue
		}
		taken[e.C] = true
		byKey[e.Key] = append(byKey[e.Key], e)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m := current.Clone()
	var items []keyItem
	plan := &Plan{}
	for _, k := range keys {
		es := byKey[k]
		sort.Slice(es, func(i, j int) bool {
			if es[i].Created != es[j].Created {
				return es[i].Created < es[j].Created
			}
			return bytes.Compare(es[i].C[:], es[j].C[:]) < 0
		})
		it := keyItem{key: k}
		for _, e := range es[:len(es)-1] {
			it.consumed = append(it.consumed, e.C)
		}
		last := es[len(es)-1]
		cur, ok := m[k]
		switch {
		case last.Kind == storerec.KindDelete && !ok, last.Kind == storerec.KindPut && ok && cur == last.C:
			it.consumed = append(it.consumed, last.C)
		case last.Kind == storerec.KindDelete:
			it.change = &storerec.Change{Key: k, C: last.C, Removal: true}
			delete(m, k)
		default:
			if !ok && len(m) >= storerec.MaxKeysInMap {
				plan.Deferred = append(plan.Deferred, k)
				continue
			}
			it.change = &storerec.Change{Key: k, C: last.C}
			m[k] = last.C
		}
		items = append(items, it)
	}
	// Pack the items into tokens, in key order.
	cur := Step{}
	state := current.Clone()
	flush := func() {
		if len(cur.Changes) == 0 && len(cur.Consumed) == 0 {
			return
		}
		for _, c := range cur.Changes {
			if c.Removal {
				delete(state, c.Key)
			} else {
				state[c.Key] = c.C
			}
		}
		sortStep(&cur)
		cur.Map = state.Clone()
		plan.Steps = append(plan.Steps, cur)
		cur = Step{}
	}
	base, err := size(nil, nil)
	if err != nil {
		return nil, err
	}
	fits := func(s Step) (bool, error) {
		if len(s.Changes) > storerec.MaxChanges || len(s.Consumed) > storerec.MaxConsumed {
			return false, nil
		}
		// An upper bound first: a change is at most an array head, a text
		// head of 3 bytes, its key and two 34-byte strings; a consumed
		// commitment 34 bytes; each list's head at most 3 more bytes.
		est := base + 6 + 34*len(s.Consumed)
		for _, c := range s.Changes {
			est += 1 + 3 + len(c.Key) + 68
		}
		if est <= storerec.MaxBucketRecord {
			return true, nil
		}
		t := Step{Changes: append([]storerec.Change(nil), s.Changes...), Consumed: append([][32]byte(nil), s.Consumed...)}
		sortStep(&t)
		n, err := size(t.Changes, t.Consumed)
		if err != nil {
			return false, err
		}
		return n <= storerec.MaxBucketRecord, nil
	}
	for _, it := range items {
		try := Step{Changes: append([]storerec.Change(nil), cur.Changes...), Consumed: append([][32]byte(nil), cur.Consumed...)}
		if it.change != nil {
			try.Changes = append(try.Changes, *it.change)
		}
		try.Consumed = append(try.Consumed, it.consumed...)
		ok, err := fits(try)
		if err != nil {
			return nil, err
		}
		if ok {
			cur = try
			continue
		}
		flush()
		// The item alone, its consumed spilling into tokens of their own.
		rest := it.consumed
		for {
			one := Step{Consumed: append([][32]byte(nil), rest...)}
			if it.change != nil {
				one.Changes = []storerec.Change{*it.change}
			}
			ok, err := fits(one)
			if err != nil {
				return nil, err
			}
			if ok {
				cur = one
				break
			}
			if len(rest) == 0 {
				return nil, errors.New("anchor: one change does not fit a token")
			}
			// As many consumed as fit, alone.
			lo, hi := 0, len(rest)
			for lo < hi {
				mid := (lo + hi + 1) / 2
				ok, err := fits(Step{Consumed: append([][32]byte(nil), rest[:mid]...)})
				if err != nil {
					return nil, err
				}
				if ok {
					lo = mid
				} else {
					hi = mid - 1
				}
			}
			if lo == 0 {
				return nil, errors.New("anchor: a consumed commitment does not fit a token")
			}
			cur = Step{Consumed: append([][32]byte(nil), rest[:lo]...)}
			flush()
			rest = rest[lo:]
		}
	}
	flush()
	return plan, nil
}

func sortStep(s *Step) {
	sort.Slice(s.Changes, func(i, j int) bool { return s.Changes[i].Key < s.Changes[j].Key })
	sort.Slice(s.Consumed, func(i, j int) bool { return bytes.Compare(s.Consumed[i][:], s.Consumed[j][:]) < 0 })
}

// RecordSizer measures a token record of a chain whose other fields are
// base's: the exact encoded length.
func RecordSizer(base storerec.Bucket) func([]storerec.Change, [][32]byte) (int, error) {
	return func(changes []storerec.Change, consumed [][32]byte) (int, error) {
		b := base
		b.Changes, b.Consumed = changes, consumed
		if b.Changes == nil {
			b.Changes = []storerec.Change{}
		}
		if b.Consumed == nil {
			b.Consumed = [][32]byte{}
		}
		// A record over the bound is measured, not refused: the bound is
		// what is being tested.
		return b.EncodedSize()
	}
}
