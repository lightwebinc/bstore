package module

import (
	"fmt"
	"strconv"
)

// The since class (spec section 11.1, frozen.md amendment A2): what the
// free classes unanchored, ads, hold, fronts, outcomes and sweep answer for
// a bucket, in the order this host took it. A host that repairs its gaps
// from this one keeps the position of the last output it was answered and
// asks only for what came after, so a repair costs what the two hosts
// differ by and not what they hold.
//
// An output is listed while one of those classes would answer it, at a
// position that is the host's time when it was listed, never below a
// position already given: a clock that steps back does not put an output
// behind a position an asker already passed. Only positions before the
// host's present millisecond are answered, so nothing is ever listed at or
// below a position already answered. A restore lists every held output at
// its first sight; an output that was listed again later (an entry a
// reorganization left unnamed) is then back at its first sight, behind an
// asker that had passed it and not yet reached its later position. The
// classes above still answer it.

// sinceIdx is one bucket's listed outputs, by position.
type sinceIdx struct {
	list sortedStrings
	// last is the highest position given.
	last int64
}

// sinceKey is an output's position as answered and as sorted.
func sinceKey(pos int64, txid string, vout uint32) string {
	return fmt.Sprintf("%016d:%s:%010d", pos, txid, vout)
}

// sinceOutput is the output a position names.
func sinceOutput(k string) (txid string, vout uint32) {
	n, _ := strconv.ParseUint(k[82:], 10, 32)
	return k[17:81], uint32(n) //nolint:gosec // parsed as 32 bits
}

// sinceIn lists an output of bucket b, first seen at first, and answers
// the time of its position.
func (s *Service) sinceIn(b, txid string, vout uint32, first int64) int64 {
	x := s.since[b]
	if x == nil {
		x = &sinceIdx{}
		s.since[b] = x
	}
	pos := first
	if !s.restoring && pos < x.last {
		pos = x.last + 1
	}
	if pos > x.last {
		x.last = pos
	}
	x.list.insert(sinceKey(pos, txid, vout))
	return pos
}

// sinceOut takes an output listed at pos out of bucket b's list; pos 0 is
// an output that is not listed.
func (s *Service) sinceOut(b, txid string, vout uint32, pos int64) {
	if pos == 0 {
		return
	}
	if x := s.since[b]; x != nil {
		x.list.remove(sinceKey(pos, txid, vout))
	}
}

// unanchoredHas is whether a served chain's unanchored list holds entry e.
func (s *Service) unanchoredHas(e *carrierEntry) bool {
	for _, sv := range s.servedFor(e.b, e.id) {
		if list := sv.unanchored[e.owner]; list != nil && list.has(e.txid) {
			return true
		}
	}
	return false
}

// sinceEntry lists or unlists an entry as the unanchored lists hold it.
func (s *Service) sinceEntry(e *carrierEntry) {
	switch has := s.unanchoredHas(e); {
	case has && e.fpos == 0:
		e.fpos = s.sinceIn(e.b, e.txid, 0, e.at)
	case !has && e.fpos != 0:
		s.sinceOut(e.b, e.txid, 0, e.fpos)
		e.fpos = 0
	}
}

// sinceAnswers is whether a free class answers the listed output now: an
// entry a served chain's unanchored list holds, inside its window; an
// outcome inside its window; a front statement; a live advertisement; a
// hold output; a sweep.
func (s *Service) sinceAnswers(b, txid string, vout uint32, now int64) bool {
	if vout == 0 {
		if e, ok := s.carriers.get(cOf(txid)); ok && e.b == b {
			switch e.kind {
			case "entry":
				return now-e.at < UnanchoredDays*day && s.unanchoredHas(e)
			case "outcome":
				return now-e.at < OutcomeWindowDays*day
			}
			return true
		}
		if s.sweeps.has(b + "|" + txid) {
			return true
		}
	}
	k := heldKey(b, txid, vout)
	if ad, ok := s.ads.get(k); ok {
		return !ad.spent && !ad.anyone && int64(ad.expiry)*1000 > now //nolint:gosec // an expiry is a 32-bit time
	}
	return s.holds.has(k)
}

// sinceForget clears the position kept on the record of an output the list
// dropped as it met it.
func (s *Service) sinceForget(b, txid string, vout uint32) {
	if vout == 0 {
		if e, ok := s.carriers.get(cOf(txid)); ok && e.b == b {
			e.fpos = 0
			return
		}
		if sw, ok := s.sweeps.get(b + "|" + txid); ok {
			sw.fpos = 0
			return
		}
	}
	k := heldKey(b, txid, vout)
	if ad, ok := s.ads.get(k); ok {
		ad.fpos = 0
	}
	if h, ok := s.holds.get(k); ok {
		h.fpos = 0
	}
}

// answerSince answers a page of bucket b's listed outputs strictly after
// position after ("" from the start), each with its position as context.
// What left its class's window since the last retention pass is walked
// once, here, and leaves the list.
func (s *Service) answerSince(b, after string, now int64) []Formula {
	out := []Formula{}
	x := s.since[b]
	if x == nil {
		return out
	}
	i := 0
	if after != "" {
		i = x.list.upper(after)
	}
	// Positions of the present millisecond and later are not answered yet.
	limit := sinceKey(now, "", 0)[:17]
	var gone []string
	for ; i < x.list.size() && len(out) < PageLimit; i++ {
		k := x.list.a[i]
		if k >= limit {
			break
		}
		txid, vout := sinceOutput(k)
		if !s.sinceAnswers(b, txid, vout, now) {
			gone = append(gone, k)
			continue
		}
		out = append(out, Formula{Txid: txid, Vout: vout, Context: []byte(k)})
	}
	for _, k := range gone {
		txid, vout := sinceOutput(k)
		s.sinceForget(b, txid, vout)
		x.list.remove(k)
	}
	return out
}
