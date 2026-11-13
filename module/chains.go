package module

import (
	"context"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// ---- chains (spec section 12.2)

func descends(prev, next *tokenEntry) bool {
	want := outpoint(prev.txid, prev.vout)
	n := 0
	for _, op := range next.inputs {
		if op == want {
			n++
		}
	}
	return n == 1 && next.prev == prev.txid && prev.seq+1 == next.seq
}

// rebuild folds the map of a consistent token again from seq 0 along its
// predecessors.
func (s *Service) rebuild(v *chainIdx, t *tokenEntry) (*mapState, error) {
	var path []*tokenEntry
	for cur := t; cur != nil; {
		path = append(path, cur)
		if cur.seq == 0 {
			break
		}
		cur = v.byTxid[cur.prev]
	}
	m := newMapState()
	for i := len(path) - 1; i >= 0; i-- {
		if r := m.apply(path[i].rec); r != "" {
			return nil, fmt.Errorf("ls_bstore: token %s folded consistent once and %s now", path[i].txid, r)
		}
	}
	return m, nil
}

// rootStep roots token x when its predecessor is rooted and it descends
// from it, folding it onto its branch's map for a served chain; it answers
// the tokens that may root after it, or nothing when x does not root now.
func (s *Service) rootStep(v *chainIdx, x *tokenEntry, count bool) []*tokenEntry {
	if x.rooted {
		return nil
	}
	var p *tokenEntry
	if x.seq == 0 {
		if !x.first {
			return nil
		}
	} else {
		p = v.byTxid[x.prev]
		if p == nil || !p.rooted || !descends(p, x) {
			return nil
		}
	}
	x.rooted = true
	switch {
	case !v.served:
		x.status = "rooted"
	case p != nil && p.status == "map-mismatch":
		// Sticky: everything after a break is broken.
		x.status, x.reason = "map-mismatch", "after-break"
	default:
		var m *mapState
		if p == nil {
			m = newMapState()
		} else {
			// The branch's map moves to x. A second child of p (a
			// reorganisation) finds it taken and folds p's map again from
			// seq 0: rare, and the one place the host folds a chain whole
			// outside a restore.
			held := v.branches[p.txid]
			delete(v.branches, p.txid)
			if held != nil {
				m = held
			} else if rb, err := s.rebuild(v, p); err == nil {
				m = rb
			} else {
				s.host.Log("ls_bstore could not fold a branch again", map[string]any{"txid": p.txid, "err": err.Error()})
			}
		}
		r := "rebuild"
		if m != nil {
			r = m.apply(x.rec)
		}
		if r == "" {
			x.status = "consistent"
			v.branches[x.txid] = m
			v.consistentSeqs.add(x.seq)
		} else {
			x.status, x.reason = "map-mismatch", r
			// The map stays its predecessor's: it is the map of the last consistent token.
			if p != nil && m != nil {
				v.branches[p.txid] = m
			}
		}
		if v.hist != nil {
			histAdd(v, x)
		}
	}
	v.rootedSeqs.add(x.seq)
	if int64(x.seq) > v.top { //nolint:gosec // a seq is at most 2^53 - 1
		v.top = int64(x.seq) //nolint:gosec // a seq is at most 2^53 - 1
	}
	s.moved(v)
	if count {
		s.counted(v, x)
	}
	if turned := v.turned[x.txid]; turned != nil {
		ids := make([]string, 0, len(turned))
		for id := range turned {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			s.rereads = append(s.rereads, reread{b: v.b, txid: id, vout: turned[id]})
		}
		delete(v.turned, x.txid)
	}
	ids := make([]string, 0, len(v.children[x.txid]))
	for id := range v.children[x.txid] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var next []*tokenEntry
	for _, id := range ids {
		if c := v.byTxid[id]; c != nil {
			next = append(next, c)
		}
	}
	return next
}

func (s *Service) rootFrom(v *chainIdx, t *tokenEntry, count bool) {
	work := []*tokenEntry{t}
	for len(work) > 0 {
		x := work[len(work)-1]
		work = work[:len(work)-1]
		work = append(work, s.rootStep(v, x, count)...)
	}
}

// moved says what changed under a chain's latest tokens: they are found
// again when next asked.
func (s *Service) moved(v *chainIdx) {
	v.latest, v.latestOK = nil, false
	v.head = nil
}

func (s *Service) counted(v *chainIdx, t *tokenEntry) {
	s.host.Metrics().Inc("bstore_token_status_total", map[string]string{"status": t.status}, 1)
	if t.status == "map-mismatch" {
		s.host.Log("ls_bstore token status", map[string]any{"bucket": v.b, "owner": v.owner, "id": v.id, "seq": t.seq, "txid": t.txid, "status": t.status, "reason": t.reason})
	}
}

func (s *Service) reroot(v *chainIdx) {
	v.branches = map[string]*mapState{}
	v.top = -1
	v.rootedSeqs.clear()
	v.consistentSeqs.clear()
	v.hist = nil
	s.moved(v)
	for _, t := range v.byTxid {
		t.rooted, t.status, t.reason = false, "pending", ""
	}
	for _, t := range v.bySeq[0] {
		s.rootFrom(v, t, false)
	}
}

// bound keeps at most PerSeq tokens of a chain at one seq: rooted first,
// then the lowest txids.
func (s *Service) bound(v *chainIdx, seq uint64) {
	at := v.bySeq[seq]
	if len(at) <= PerSeq {
		return
	}
	order := append([]*tokenEntry(nil), at...)
	sort.SliceStable(order, func(i, j int) bool {
		if order[i].rooted != order[j].rooted {
			return order[i].rooted
		}
		return order[i].txid < order[j].txid
	})
	again := false
	for _, t := range order[PerSeq:] {
		s.host.Log("ls_bstore kept no more tokens at this seq", map[string]any{"bucket": v.b, "owner": v.owner, "id": v.id, "txid": t.txid, "seq": seq})
		again = again || t.rooted
		if !t.rooted && t.prev != "" {
			m := v.turned[t.prev]
			if m == nil {
				m = map[string]uint32{}
				v.turned[t.prev] = m
			}
			m[t.txid] = t.vout
		}
		s.forgetToken(v, t, false)
	}
	if again {
		s.reroot(v)
	}
}

// candidatesAt are the tokens at seq that latest and heads consider:
// rooted and consistent (served), or rooted.
func candidatesAt(v *chainIdx, seq uint64) []*tokenEntry {
	var out []*tokenEntry
	for _, t := range v.bySeq[seq] {
		if t.rooted && (!v.served || t.status == "consistent") {
			out = append(out, t)
		}
	}
	return out
}

// latestOf is the chain's latest tokens, without asking the headers: those
// heads last found when it asked them within ChoiceTTL, else the rooted,
// consistent tokens at the highest seq that has one (served), or the
// rooted ones at the highest seq (any other chain).
func (s *Service) latestOf(v *chainIdx) []*tokenEntry {
	if v.head != nil && s.nowMs()-v.head.at < ChoiceTTL.Milliseconds() {
		return v.head.ts
	}
	if v.latestOK {
		return v.latest
	}
	idx := v.rootedSeqs
	if v.served {
		idx = v.consistentSeqs
	}
	v.latest, v.latestOK = nil, true
	if n := idx.sorted.size(); n > 0 {
		v.latest = candidatesAt(v, idx.sorted.a[n-1])
	}
	return v.latest
}

func (s *Service) verifies(ctx context.Context, t *tokenEntry) bool {
	if s.headers == nil || t.root == nil {
		return false
	}
	root := chainhash.Hash(*t.root)
	ok, err := s.headers.IsValidRootForHeight(ctx, &root, t.height)
	return err == nil && ok
}

// heads is the chain's latest tokens (spec section 12.2 and frozen.md row
// C8): walking down from the highest seq that holds a candidate, the
// candidates at the first seq where any token's proof verifies against the
// host's headers, only those; after a reorganisation that orphaned a
// longer branch, the tip of the branch on the current chain. When none
// verifies within ReorgDepth seqs (the header source does not answer), the
// candidates at the highest seq. Kept for ChoiceTTL.
func (s *Service) heads(ctx context.Context, v *chainIdx) []*tokenEntry {
	if v.head != nil && s.nowMs()-v.head.at < ChoiceTTL.Milliseconds() {
		return v.head.ts
	}
	idx := v.rootedSeqs
	if v.served {
		idx = v.consistentSeqs
	}
	var found []*tokenEntry
	ok := false
	for i, n := idx.sorted.size()-1, 0; s.headers != nil && i >= 0 && n < ReorgDepth; i, n = i-1, n+1 {
		var good []*tokenEntry
		for _, t := range candidatesAt(v, idx.sorted.a[i]) {
			if s.verifies(ctx, t) {
				good = append(good, t)
			}
		}
		if len(good) > 0 {
			found, ok = good, true
			break
		}
	}
	v.latest, v.latestOK = nil, false
	if !ok {
		found = s.latestOf(v)
	}
	v.head = &headChoice{ts: found, at: s.nowMs()}
	return found
}

// view is the token a served chain's answers are proven against (the
// lowest txid of its heads) and its map.
func (s *Service) view(ctx context.Context, v *chainIdx) (*tokenEntry, *mapState) {
	hs := append([]*tokenEntry(nil), s.heads(ctx, v)...)
	if len(hs) == 0 {
		return nil, nil
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].txid < hs[j].txid })
	t := hs[0]
	m := v.branches[t.txid]
	if m == nil {
		rb, err := s.rebuild(v, t)
		if err != nil {
			s.host.Log("ls_bstore could not fold a branch again", map[string]any{"txid": t.txid, "err": err.Error()})
			return nil, nil
		}
		m = rb
		v.branches[t.txid] = m
	}
	return t, m
}

// parties are the keys of a served chain's owner, known from
// configuration, and the writers of its latest consistent tokens.
func (s *Service) parties(sv *servedIdx) map[string]bool {
	out := map[string]bool{sv.owner: true}
	if v := s.chains[sv.chain]; v != nil {
		for _, t := range s.latestOf(v) {
			for _, w := range t.rec.Writers {
				out[hexOf(w)] = true
			}
		}
	}
	return out
}

// isParty is whether key is a served chain's owner or a writer of its
// latest consistent token, in bucket b.
func (s *Service) isParty(b, key string) bool {
	for _, sv := range s.servedOf(b) {
		if s.parties(sv)[key] {
			return true
		}
	}
	return false
}

// servedOf are the chains bucket b's configuration serves.
func (s *Service) servedOf(b string) []*servedIdx {
	if !s.servedBuckets[b] {
		return nil
	}
	var out []*servedIdx
	for _, k := range s.servedList {
		if sv := s.servedIdx[k]; sv != nil && sv.b == b {
			out = append(out, sv)
		}
	}
	return out
}

// servedChains are the served chains of bucket b the host holds a token
// of.
func (s *Service) servedChains(b string) []*chainIdx {
	var out []*chainIdx
	for _, sv := range s.servedOf(b) {
		if v := s.chains[sv.chain]; v != nil {
			out = append(out, v)
		}
	}
	return out
}

// namedByServed is whether a token of a served chain of bucket b names
// commitment c.
func (s *Service) namedByServed(b, c string) bool {
	for _, v := range s.servedChains(b) {
		if v.named[c] > 0 {
			return true
		}
	}
	return false
}

// ---- the served chains' indexes of unnamed entries

func (s *Service) servedFor(b, id string) []*servedIdx {
	var out []*servedIdx
	for _, k := range s.servedList {
		if sv := s.servedIdx[k]; sv != nil && sv.b == b && sv.id == id {
			out = append(out, sv)
		}
	}
	return out
}

func (s *Service) unnamedAdd(sv *servedIdx, e *carrierEntry) {
	byWriter := sv.pendingByKey[e.key]
	if byWriter == nil {
		byWriter = map[string]*sortedStrings{}
		sv.pendingByKey[e.key] = byWriter
	}
	sortedIn(byWriter, e.owner).insert(e.tkey)
	if s.nowMs()-e.at < UnanchoredDays*day {
		sortedIn(sv.unanchored, e.owner).insert(e.txid)
	}
	s.sinceEntry(e)
}

func (s *Service) unnamedRemove(sv *servedIdx, e *carrierEntry) {
	if byWriter := sv.pendingByKey[e.key]; byWriter != nil {
		sortedOut(byWriter, e.owner, e.tkey)
		if len(byWriter) == 0 {
			delete(sv.pendingByKey, e.key)
		}
	}
	sortedOut(sv.unanchored, e.owner, e.txid)
	s.sinceEntry(e)
}

// name counts a commitment a served chain's held tokens name one more
// time, or one fewer.
func (s *Service) name(v *chainIdx, c string, by int) {
	n := v.named[c] + by
	if n > 0 {
		v.named[c] = n
	} else {
		delete(v.named, c)
	}
	e, _ := s.carriers.get(c)
	sv := s.servedIdx[v.key]
	if e == nil || e.kind != "entry" || sv == nil || e.b != v.b || e.id != v.id {
		return
	}
	if by == 1 && n == 1 {
		s.unnamedRemove(sv, e)
	}
	if by == -1 && n == 0 {
		s.unnamedAdd(sv, e)
	}
}

// hist is a served chain's history index: by key, the rooted tokens whose
// changes set or remove it, ascending by seq then txid.
func (s *Service) hist(v *chainIdx) map[string][]histEntry {
	if v.hist == nil {
		v.hist = map[string][]histEntry{}
		var ts []*tokenEntry
		for _, t := range v.byTxid {
			if t.rooted {
				ts = append(ts, t)
			}
		}
		sort.Slice(ts, func(i, j int) bool {
			if ts[i].seq != ts[j].seq {
				return ts[i].seq < ts[j].seq
			}
			return ts[i].txid < ts[j].txid
		})
		for _, t := range ts {
			histAdd(v, t)
		}
	}
	return v.hist
}

func histAdd(v *chainIdx, t *tokenEntry) {
	for _, ch := range t.rec.Changes {
		list := v.hist[ch.Key]
		at := sort.Search(len(list), func(i int) bool {
			h := list[i].t
			return h.seq > t.seq || (h.seq == t.seq && h.txid > t.txid)
		})
		list = append(list, histEntry{})
		copy(list[at+1:], list[at:])
		list[at] = histEntry{t: t, c: hexOf(ch.C[:])}
		v.hist[ch.Key] = list
	}
}
