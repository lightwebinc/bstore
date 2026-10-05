package storerec

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math"
	"sort"
)

// Payouts is spec section 9.5's rule for k ranks and the hold output's
// value r: weight(i) = sqrt(k - i + 1) in IEEE-754 binary64, S their sum in
// rank order, payout(i) = floor(r * weight(i) / S), and the remainder to
// rank 1. A payout of 0 gets no output; the payouts always sum to r. r must
// be at most 2,100,000,000,000,000 (below 2^53, so binary64 holds it).
func Payouts(r uint64, k int) []uint64 {
	if k < 1 {
		return nil
	}
	w := make([]float64, k)
	s := 0.0
	for i := 1; i <= k; i++ {
		w[i-1] = math.Sqrt(float64(k - i + 1))
		s += w[i-1]
	}
	out := make([]uint64, k)
	var sum uint64
	rf := float64(r)
	for i := range out {
		out[i] = uint64(math.Floor(rf * w[i] / s))
		sum += out[i]
	}
	out[0] += r - sum
	return out
}

// DerivationPrefix is base64(C), standard padded base64 of the entry's
// commitment in hash byte order.
func DerivationPrefix(c [32]byte) string { return base64.StdEncoding.EncodeToString(c[:]) }

// DerivationSuffix is base64(uint16be(rank)).
func DerivationSuffix(rank int) string {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(rank))
	return base64.StdEncoding.EncodeToString(b[:])
}

// Ad is one advertisement as the writer's client knows it at settlement.
type Ad struct {
	Txid       [32]byte // hash byte order
	Advertiser []byte
	Root       [32]byte
	Expiry     uint64
	// SpentBy is the advertisements of the transaction that spends this
	// one, when it is spent: a renewal holds its successor among them.
	Spent   bool
	SpentBy []*Ad
}

// Live follows an advertisement's renewal successors (an advertisement of
// the same key for the same root, with a later expiry, in the transaction
// that spends it, or that successor's own) and reports whether the last is
// unspent: a renewal counts as unspent, a spend by anything else cancels.
func (a *Ad) Live() bool {
	for cur, hops := a, 0; hops < 1<<16; hops++ {
		if !cur.Spent {
			return true
		}
		var next *Ad
		for _, s := range cur.SpentBy {
			if bytes.Equal(s.Advertiser, cur.Advertiser) && s.Root == cur.Root && s.Expiry > cur.Expiry {
				next = s
				break
			}
		}
		if next == nil {
			return false
		}
		cur = next
	}
	return false
}

// Candidate is an eligible advertisement in the writer's race: when its
// client first saw it, and whether it passed its settlement challenge.
type Candidate struct {
	Ad        *Ad
	FirstSeen uint64
	Passed    bool
}

// displayLess orders two txids by their display order (the byte-reversed
// hex), which is how the contract orders txids in JSON and in answers.
func displayLess(a, b [32]byte) bool {
	for i := 31; i >= 0; i-- {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// Rank orders candidates as the writer's client does (spec section 9.4):
// by first sight, ties to the lower advertisement txid (display order);
// each advertiser counted once, at its first place; the anyone key never.
func Rank(cands []Candidate) []Candidate {
	out := append([]Candidate(nil), cands...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FirstSeen != out[j].FirstSeen {
			return out[i].FirstSeen < out[j].FirstSeen
		}
		return displayLess(out[i].Ad.Txid, out[j].Ad.Txid)
	})
	seen := map[string]bool{}
	var ranked []Candidate
	for _, c := range out {
		k := string(c.Ad.Advertiser)
		if seen[k] || bytes.Equal(c.Ad.Advertiser, AnyoneKey) {
			continue
		}
		seen[k] = true
		ranked = append(ranked, c)
	}
	return ranked
}

// Payee is one rank of a settlement.
type Payee struct {
	Rank       int
	Advertiser []byte
	Ad         [32]byte
	Satoshis   uint64
	Prefix     string
	Suffix     string
}

// Settlement is the writer's decision at its race's close.
type Settlement struct {
	Settled bool
	// Dropped are the advertisers whose passed advertisement was cancelled
	// before settlement, in rank order.
	Dropped [][]byte
	Payees  []Payee
}

// Settle applies spec sections 9.3 to 9.5: the ranked candidates that
// passed, the first K of them paid; a payee whose advertisement is
// cancelled (spent by anything but a renewal) is dropped, the ranks below
// move up and the next passed candidate fills the last place; the writer
// settles only if at least t remain, paying k = min(K, remaining) ranks of
// r by Payouts, each bound to entry commitment c and its rank. A rank whose
// payout is 0 is left out of Payees but keeps its number.
func Settle(c [32]byte, r uint64, t, K int, cands []Candidate) Settlement {
	var passed []Candidate
	for _, x := range Rank(cands) {
		if x.Passed {
			passed = append(passed, x)
		}
	}
	var s Settlement
	var keep []Candidate
	for _, x := range passed {
		if len(keep) == K {
			break
		}
		if !x.Ad.Live() {
			s.Dropped = append(s.Dropped, x.Ad.Advertiser)
			continue
		}
		keep = append(keep, x)
	}
	if len(keep) < t || len(keep) == 0 {
		return s
	}
	s.Settled = true
	pay := Payouts(r, len(keep))
	for i, x := range keep {
		if pay[i] == 0 {
			continue
		}
		s.Payees = append(s.Payees, Payee{Rank: i + 1, Advertiser: x.Ad.Advertiser, Ad: x.Ad.Txid, Satoshis: pay[i],
			Prefix: DerivationPrefix(c), Suffix: DerivationSuffix(i + 1)})
	}
	return s
}
