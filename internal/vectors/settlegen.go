package vectors

import (
	"fmt"

	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func genSettle(files map[string][]byte) error {
	// Payouts for k 1 to 16 and assorted R, BRC-178's examples among them.
	var payouts []map[string]any
	for _, r := range []uint64{1, 2, 3, 12, 20, 100, 1000, 10000, 99999, 123456789, storerec.MaxSatoshis} {
		for k := 1; k <= 16; k++ {
			p := storerec.Payouts(r, k)
			var sum uint64
			for _, x := range p {
				sum += x
			}
			if sum != r {
				return fmt.Errorf("payouts %d %d sum to %d", r, k, sum)
			}
			payouts = append(payouts, map[string]any{"R": r, "k": k, "payouts": p})
		}
	}
	for _, ex := range []struct {
		r    uint64
		k    int
		want []uint64
	}{{12, 5, []uint64{5, 2, 2, 2, 1}}, {20, 3, []uint64{10, 6, 4}}} {
		got := storerec.Payouts(ex.r, ex.k)
		if fmt.Sprint(got) != fmt.Sprint(ex.want) {
			return fmt.Errorf("BRC-178 example R=%d k=%d: %v, want %v", ex.r, ex.k, got, ex.want)
		}
	}

	// Derivation prefixes and suffixes, and the key BRC-29 derives for the
	// payee: the writer's wallet derives it for the advertiser, the payee's
	// for itself from the writer's key, and both agree.
	c := label("settled entry")
	var derivs []map[string]any
	for _, rank := range []int{1, 2, 5, 16, 256, 65535} {
		prefix, suffix := storerec.DerivationPrefix(c), storerec.DerivationSuffix(rank)
		keyID := prefix + " " + suffix
		writer := must(wallet.NewKeyDeriver(Alice.Priv).DerivePublicKey(storerec.SettleProtocol, keyID,
			wallet.Counterparty{Type: wallet.CounterpartyTypeOther, Counterparty: Bob.Priv.PubKey()}, false))
		payee := must(wallet.NewKeyDeriver(Bob.Priv).DerivePublicKey(storerec.SettleProtocol, keyID,
			wallet.Counterparty{Type: wallet.CounterpartyTypeOther, Counterparty: Alice.Priv.PubKey()}, true))
		if !writer.IsEqual(payee) {
			return fmt.Errorf("BRC-29 rank %d: writer and payee disagree", rank)
		}
		derivs = append(derivs, map[string]any{"commitment": h32(c), "rank": rank, "prefix": prefix, "suffix": suffix,
			"writer": hx(Alice.Pub()), "payee": hx(Bob.Pub()), "paymentKey": hx(writer.Compressed())})
	}

	// Rankings and settlements.
	root := label("settle root")
	ad := func(name string, who Party, expiry uint64) *storerec.Ad {
		return &storerec.Ad{Txid: label("ad %s", name), Advertiser: who.Pub(), Root: root, Expiry: expiry}
	}
	renewed := func(a *storerec.Ad, name string, expiry uint64) *storerec.Ad {
		s := &storerec.Ad{Txid: label("ad %s", name), Advertiser: a.Advertiser, Root: a.Root, Expiry: expiry}
		a.Spent, a.SpentBy = true, []*storerec.Ad{s}
		return s
	}
	cancel := func(a *storerec.Ad) { a.Spent = true }
	cand := func(a *storerec.Ad, seen uint64, passed bool) storerec.Candidate {
		return storerec.Candidate{Ad: a, FirstSeen: seen, Passed: passed}
	}
	type scen struct {
		name  string
		r     uint64
		t, K  int
		cands []storerec.Candidate
	}
	var scens []scen
	{
		a, b, cc, d := ad("a", Bob, 2000), ad("b", Carol, 2000), ad("c", Dave, 2000), ad("d", Erin, 2000)
		scens = append(scens, scen{"four pass, K 3: the first three by first sight", 10000, 2, 3,
			[]storerec.Candidate{cand(d, 40, true), cand(b, 20, true), cand(a, 10, true), cand(cc, 30, true)}})
	}
	{
		a, b := ad("a", Bob, 2000), ad("b", Carol, 2000)
		scens = append(scens, scen{"a tie in first sight goes to the lower txid, display order", 100, 1, 2,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 10, true)}})
	}
	{
		a, b, cc := ad("a", Bob, 2000), ad("b", Carol, 2000), ad("c", Dave, 2000)
		scens = append(scens, scen{"one fails its challenge, the next moves up", 1000, 2, 2,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, false), cand(cc, 30, true)}})
	}
	{
		a, b := ad("a", Bob, 2000), ad("b", Carol, 2000)
		scens = append(scens, scen{"fewer than t pass: nothing is paid", 1000, 3, 3,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, true)}})
	}
	{
		a, b, cc := ad("a", Bob, 2000), ad("b", Carol, 2000), ad("c", Dave, 2000)
		renewed(a, "a renewed", 3000)
		scens = append(scens, scen{"a renewed advertisement keeps its original first sight and is paid", 900, 2, 2,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, true), cand(cc, 30, true)}})
	}
	{
		a, b, cc := ad("a", Bob, 2000), ad("b", Carol, 2000), ad("c", Dave, 2000)
		s := renewed(a, "a renewed", 3000)
		renewed(s, "a renewed twice", 4000)
		scens = append(scens, scen{"a renewal's own renewal still counts as unspent", 900, 2, 2,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, true), cand(cc, 30, true)}})
	}
	{
		a, b, cc, d := ad("a", Bob, 2000), ad("b", Carol, 2000), ad("c", Dave, 2000), ad("d", Erin, 2000)
		cancel(b)
		scens = append(scens, scen{"a cancelled payee is dropped, the ranks below move up, the next passed fills the last place", 1000, 2, 3,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, true), cand(cc, 30, true), cand(d, 40, true)}})
	}
	{
		a, b, cc := ad("a", Bob, 2000), ad("b", Carol, 2000), ad("c", Dave, 2000)
		cancel(b)
		cancel(cc)
		scens = append(scens, scen{"cancellations leave fewer than t: t is counted again and nothing is paid", 1000, 2, 3,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, true), cand(cc, 30, true)}})
	}
	{
		a, b := ad("a", Bob, 2000), ad("b", Carol, 2000)
		x := &storerec.Ad{Txid: label("ad not a renewal"), Advertiser: Bob.Pub(), Root: root, Expiry: 1000}
		a.Spent, a.SpentBy = true, []*storerec.Ad{x}
		scens = append(scens, scen{"spent into an advertisement with an earlier expiry: a cancellation", 1000, 1, 2,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, true)}})
	}
	{
		a, b := ad("a", Bob, 2000), ad("b", Carol, 2000)
		y := &storerec.Ad{Txid: label("ad other key"), Advertiser: Dave.Pub(), Root: root, Expiry: 9000}
		a.Spent, a.SpentBy = true, []*storerec.Ad{y}
		scens = append(scens, scen{"spent into another key's advertisement: a cancellation", 1000, 1, 2,
			[]storerec.Candidate{cand(a, 10, true), cand(b, 20, true)}})
	}
	{
		a, a2, b := ad("a", Bob, 2000), ad("a2", Bob, 2500), ad("b", Carol, 2000)
		scens = append(scens, scen{"one advertiser counted once, at its first place", 1000, 2, 3,
			[]storerec.Candidate{cand(a2, 5, true), cand(a, 10, true), cand(b, 20, true)}})
	}
	{
		a := ad("a", Bob, 2000)
		z := &storerec.Ad{Txid: label("ad anyone"), Advertiser: anyonePK, Root: root, Expiry: 2000}
		scens = append(scens, scen{"the anyone key never counts", 1000, 1, 2,
			[]storerec.Candidate{cand(z, 1, true), cand(a, 10, true)}})
	}
	{
		var cs []storerec.Candidate
		for i := 0; i < 16; i++ {
			p := party(fmt.Sprintf("payee %d", i), byte(0x30+i))
			cs = append(cs, cand(&storerec.Ad{Txid: label("ad %d", i), Advertiser: p.Pub(), Root: root, Expiry: 2000}, uint64(100+i), true))
		}
		scens = append(scens, scen{"16 payees of 10 satoshis: ranks whose payout is 0 get no output", 10, 1, 16, cs})
	}
	var settle []map[string]any
	for _, s := range scens {
		res := storerec.Settle(c, s.r, s.t, s.K, s.cands)
		var cv []map[string]any
		for _, x := range s.cands {
			cv = append(cv, map[string]any{"ad": adView(x.Ad), "firstSeen": x.FirstSeen, "passed": x.Passed})
		}
		var pv []map[string]any
		for _, p := range res.Payees {
			pv = append(pv, map[string]any{"rank": p.Rank, "advertiser": hx(p.Advertiser), "ad": displayHex(p.Ad), "satoshis": p.Satoshis,
				"prefix": p.Prefix, "suffix": p.Suffix})
		}
		var dv []string
		for _, d := range res.Dropped {
			dv = append(dv, hx(d))
		}
		if pv == nil {
			pv = []map[string]any{}
		}
		if dv == nil {
			dv = []string{}
		}
		settle = append(settle, map[string]any{"name": s.name, "commitment": h32(c), "R": s.r, "t": s.t, "K": s.K, "candidates": cv,
			"settled": res.Settled, "dropped": dv, "payees": pv})
	}
	return put(files, "settle-v1.json", map[string]any{
		"note":        "payouts for k 1 to 16 and assorted R (BRC-178's two worked examples are R 12 k 5 and R 20 k 3); derivation prefixes, suffixes and the BRC-29 payment key; rankings and settlements: first sight, ties to the lower txid in display order, renewal successors, cancellations dropped with t counted again",
		"payouts":     payouts,
		"derivations": derivs,
		"settlements": settle,
	})
}

func adView(a *storerec.Ad) map[string]any {
	v := map[string]any{"txid": displayHex(a.Txid), "advertiser": hx(a.Advertiser), "root": h32(a.Root), "expiry": a.Expiry, "spent": a.Spent}
	var by []map[string]any
	for _, s := range a.SpentBy {
		by = append(by, adView(s))
	}
	if by != nil {
		v["spentBy"] = by
	}
	return v
}
