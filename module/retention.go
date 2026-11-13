package module

import (
	"context"
	"math"
	"sort"
)

// servedView is what retention reads of the served chains of one bucket,
// computed once a pass.
type servedView struct {
	// parties are the owners and the latest consistent tokens' writers.
	parties map[string]bool
	// anchored are the commitments the current maps hold.
	anchored map[string]bool
	// left are commitments a consistent token named that the current map no
	// longer holds, with when they left it.
	left map[string]int64
	// roots are the roots of the anchored entries the host holds.
	roots map[string]bool
	// holds are the hold outpoints anchored or pending entries name.
	holds map[string]bool
	// fronts are the front statements kept the long floor.
	fronts map[string]bool
}

// leftTimes replays the paths of a served chain's latest consistent tokens
// for when each named commitment left the map.
func (s *Service) leftTimes(v *chainIdx, into map[string]int64) {
	leave := func(c string, at int64) {
		if into[c] < at {
			into[c] = at
		}
		if _, ok := into[c]; !ok {
			into[c] = at
		}
	}
	for _, tip := range s.latestOf(v) {
		var path []*tokenEntry
		for cur := tip; cur != nil; {
			path = append(path, cur)
			if cur.seq == 0 {
				break
			}
			cur = v.byTxid[cur.prev]
		}
		m := map[string]string{}
		for i := len(path) - 1; i >= 0; i-- {
			t := path[i]
			for _, ch := range t.rec.Changes {
				if old, ok := m[ch.Key]; ok {
					leave(old, t.at)
				}
				if ch.Removal {
					delete(m, ch.Key)
					leave(hexOf(ch.C[:]), t.at)
				} else {
					m[ch.Key] = hexOf(ch.C[:])
				}
			}
			for _, c := range t.rec.Consumed {
				leave(hexOf(c[:]), t.at)
			}
		}
	}
}

// servedViewOf is what retention reads of bucket b's served chains.
func (s *Service) servedViewOf(b string) *servedView {
	sv := &servedView{parties: map[string]bool{}, anchored: map[string]bool{}, left: map[string]int64{}, roots: map[string]bool{}, holds: map[string]bool{}, fronts: map[string]bool{}}
	for _, x := range s.servedOf(b) {
		for p := range s.parties(x) {
			sv.parties[p] = true
		}
	}
	for _, v := range s.servedChains(b) {
		for _, t := range s.latestOf(v) {
			m := v.branches[t.txid]
			if m == nil {
				rb, err := s.rebuild(v, t)
				if err != nil {
					s.host.Log("ls_bstore could not fold a branch again", map[string]any{"txid": t.txid, "err": err.Error()})
					continue
				}
				m = rb
				v.branches[t.txid] = m
			}
			for _, c := range m.cs {
				sv.anchored[hexOf(c[:])] = true
			}
		}
		s.leftTimes(v, sv.left)
	}
	for c := range sv.anchored {
		delete(sv.left, c)
	}
	for _, e := range s.carriers.values() {
		if e.b != b || e.kind != "entry" {
			continue
		}
		if sv.anchored[e.c] {
			if e.root != "" {
				sv.roots[e.root] = true
			}
			if e.hold != "" {
				sv.holds[e.hold] = true
			}
		} else if _, left := sv.left[e.c]; e.hold != "" && !left && (sv.parties[e.owner] || s.namedByServed(b, e.c)) {
			// Kept as long as floorOf keeps its entry on the pending floor:
			// unnamed and a party's, or named only by tokens not rooted or
			// off the latest branch, which may yet anchor it.
			sv.holds[e.hold] = true
		}
	}
	// The first 16 front statements, by first sight, of each advertiser of a
	// root the maps name.
	advertisers := map[string]bool{}
	for _, ad := range s.ads.values() {
		if ad.b == b && !ad.anyone && sv.roots[ad.root] {
			advertisers[ad.advertiser] = true
		}
	}
	for a := range advertisers {
		list := s.frontsByAdvertiser[b+"|"+a]
		if list == nil {
			continue
		}
		var fs []*carrierEntry
		for _, txid := range list.a {
			e, _ := s.carriers.get(cOf(txid))
			if e == nil {
				continue
			}
			if r, ok := s.rows.get(e.op); ok && r.c == e.c {
				fs = append(fs, e)
			}
		}
		sort.Slice(fs, func(i, j int) bool {
			if fs[i].at != fs[j].at {
				return fs[i].at < fs[j].at
			}
			return fs[i].txid < fs[j].txid
		})
		for i, e := range fs {
			if i >= FrontsPerAdvertiser {
				break
			}
			sv.fronts[e.c] = true
		}
	}
	return sv
}

// floorOf is when a carrier's floor ends, Unix milliseconds; it is dropped
// at or after it.
func (s *Service) floorOf(e *carrierEntry, sv *servedView) int64 {
	r, ok := s.rows.get(e.op)
	first := ok && r.c == e.c
	named := s.namedByServed(e.b, e.c)
	// A later carrier on an outpoint has no floor unless a served chain names it.
	if !first && !named {
		return e.at
	}
	switch e.kind {
	case "entry":
		if sv.anchored[e.c] {
			return math.MaxInt64
		}
		if left, ok := sv.left[e.c]; ok {
			return max(left+s.keep, e.at+s.other)
		}
		// Named only by tokens off the latest branch, or not rooted: kept as an unnamed one is.
		if named || sv.parties[e.owner] {
			return e.at + s.pending
		}
		return e.at + s.other
	case "outcome":
		if sv.parties[e.owner] && sv.roots[e.oroot] {
			return e.at + s.keep
		}
		return e.at + s.other
	}
	if sv.fronts[e.c] {
		return e.at + s.keep
	}
	return e.at + s.other
}

// Prune applies retention at the host's time (spec section 12.3), never
// against a restore in flight: it drops every carrier, token,
// advertisement, hold output and sweep whose floor has passed, forgets the
// outpoints, marks and rows whose floor has passed, ages the windows
// unanchored, outcomes and ads answer from, and rewrites the journal. It
// answers how many objects were dropped.
func (s *Service) Prune(ctx context.Context) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return 0
	}
	// The latest tokens against the headers, so the floors follow the
	// branch on the current chain (frozen.md row C8).
	for _, k := range s.servedList {
		if v := s.chains[k]; v != nil {
			s.heads(ctx, v)
		}
	}
	now := s.nowMs()
	views := map[string]*servedView{}
	sv := func(b string) *servedView {
		x := views[b]
		if x == nil {
			x = s.servedViewOf(b)
			views[b] = x
		}
		return x
	}
	dropped := 0
	for _, e := range s.carriers.values() {
		mark, marked := s.marks.get(e.op)
		swept, isSwept := s.table.get(e.op)
		if marked || isSwept {
			at := swept.at
			if marked {
				at = mark.at
			}
			s.retract(e, at)
			dropped++
			continue
		}
		if now >= s.floorOf(e, sv(e.b)) {
			s.drop(e, "retention")
			dropped++
		}
	}
	keys := make([]string, 0, len(s.chains))
	for k := range s.chains {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := s.chains[k]
		// A served chain's tokens are kept for the host's life, those a full
		// seq turned away included.
		if v.served {
			continue
		}
		rooted := false
		ts := make([]*tokenEntry, 0, len(v.byTxid))
		for _, t := range v.byTxid {
			ts = append(ts, t)
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i].txid < ts[j].txid })
		for _, t := range ts {
			if now-t.at < s.other {
				continue
			}
			rooted = rooted || t.rooted
			s.forgetToken(v, t, false)
			s.queueDrop(v.b, t.txid, t.vout)
			s.write(Line{K: "tf", Txid: t.txid})
			s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": "retention"}, 1)
			dropped++
		}
		if rooted {
			s.reroot(v)
		}
		// Tokens a full seq turned away are still in storage: they go by the same rule.
		prevs := make([]string, 0, len(v.turned))
		for p := range v.turned {
			prevs = append(prevs, p)
		}
		sort.Strings(prevs)
		for _, prev := range prevs {
			m := v.turned[prev]
			ids := make([]string, 0, len(m))
			for id := range m {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, txid := range ids {
				seen := now
				if t, ok := s.tokenSeen.get(txid); ok {
					seen = t.at
				}
				if now-seen < s.other {
					continue
				}
				s.queueDrop(v.b, txid, m[txid])
				delete(m, txid)
				if s.tokenSeen.has(txid) {
					s.write(Line{K: "tf", Txid: txid})
				}
				s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": "retention"}, 1)
			}
			if len(m) == 0 {
				delete(v.turned, prev)
			}
		}
		if len(v.byTxid) == 0 && len(v.turned) == 0 {
			delete(s.chains, v.key)
		}
	}
	for _, ad := range s.ads.values() {
		exp := int64(ad.expiry) * 1000 //nolint:gosec // an expiry is a 32-bit time
		until := ad.at + s.other
		if !ad.anyone && sv(ad.b).roots[ad.root] {
			until = min(exp, ad.at+s.keep) + s.other
		}
		if now >= until {
			s.dropMined(ad.b, ad.txid, ad.vout)
			dropped++
		} else if exp <= now {
			s.adLive(ad, false)
		}
	}
	for _, h := range s.holds.values() {
		until := h.at + s.other
		if sv(h.b).holds[outpoint(h.txid, h.vout)] {
			until = h.at + s.keep
		}
		if now >= until {
			s.dropMined(h.b, h.txid, h.vout)
			dropped++
		}
	}
	for _, sw := range s.sweeps.values() {
		if now-sw.at < s.keep {
			continue
		}
		s.forgetSweep(sw)
		s.queueDrop(sw.b, sw.txid, 0)
		s.write(Line{K: "of", B: sw.b, Txid: sw.txid, Vout: 0})
		s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": "retention"}, 1)
		dropped++
	}
	for _, op := range s.table.keys() {
		if p, _ := s.table.get(op); now-p.at >= s.keep {
			s.write(Line{K: "pf", Op: op})
		}
	}
	servedBuckets := make([]string, 0, len(s.servedBuckets))
	for b := range s.servedBuckets {
		servedBuckets = append(servedBuckets, b)
	}
	sort.Strings(servedBuckets)
	for _, op := range s.marks.keys() {
		m, _ := s.marks.get(op)
		if m.life {
			continue
		}
		// A mark is kept for the host's life once its carrier's owner is a
		// served chain's party.
		party := false
		for _, b := range servedBuckets {
			party = party || sv(b).parties[m.key]
		}
		if party {
			s.write(Line{K: "xl", Op: op})
		} else if now-m.at >= s.keep {
			s.write(Line{K: "xf", Op: op})
		}
	}
	for _, op := range s.rows.keys() {
		if r, _ := s.rows.get(op); len(s.byOp[op]) == 0 && now-r.at >= s.other {
			s.write(Line{K: "cf", Op: op})
		}
	}
	// The windows the free classes answer from.
	for _, k := range s.servedList {
		x := s.servedIdx[k]
		for w, list := range x.unanchored {
			for _, txid := range append([]string(nil), list.a...) {
				e, ok := s.carriers.get(cOf(txid))
				if !ok || now-e.at >= UnanchoredDays*day {
					sortedOut(x.unanchored, w, txid)
				}
				if ok {
					s.sinceEntry(e, false)
				}
			}
		}
	}
	for _, e := range s.carriers.values() {
		if e.kind != "outcome" || now-e.at < OutcomeWindowDays*day {
			continue
		}
		for _, k := range append(outcomeKeys(e), outcomeBuyerKeys(e)...) {
			sortedOut(s.outcomesBy, k, e.tkey)
		}
		s.sinceOut(e.b, e.txid, 0, e.fpos)
		e.fpos = 0
	}
	s.countMaps(views)
	s.compact()
	s.flush(ctx)
	return dropped
}

func (s *Service) dropMined(b, txid string, vout uint32) {
	s.forget(txid, vout, b)
	s.queueDrop(b, txid, vout)
	s.write(Line{K: "of", B: b, Txid: txid, Vout: int64(vout)})
	s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": "retention"}, 1)
}

// countMaps counts the served maps the host holds whole, and the broken
// chains, for the gauges; it logs each served map that is incomplete or
// broken.
func (s *Service) countMaps(views map[string]*servedView) {
	t := tally{}
	bs := make([]string, 0, len(s.servedBuckets))
	for b := range s.servedBuckets {
		bs = append(bs, b)
	}
	sort.Strings(bs)
	for _, b := range bs {
		for _, v := range s.servedChains(b) {
			for _, x := range v.byTxid {
				if x.status == "map-mismatch" {
					t.ChainsBroken++
					s.host.Log("ls_bstore served chain is broken", map[string]any{"bucket": v.b, "owner": v.owner, "id": v.id})
					break
				}
			}
			sv := views[b]
			if sv == nil {
				sv = s.servedViewOf(b)
			}
			missing := 0
			for c := range sv.anchored {
				t.Anchored++
				if !s.carriers.has(c) {
					missing++
				}
			}
			if missing == 0 {
				t.MapsComplete++
			} else {
				t.MapsIncomplete++
				s.host.Log("ls_bstore served map is incomplete", map[string]any{"bucket": v.b, "owner": v.owner, "id": v.id, "missing": missing})
			}
		}
	}
	s.last = t
}
