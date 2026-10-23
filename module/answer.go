package module

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"
	"github.com/lightwebinc/bcommon/termsroute"

	"github.com/lightwebinc/bstore/storerec"
)

// Formula is one output an answer names, with its context when the class
// gives one.
type Formula struct {
	Txid    string
	Vout    uint32
	Context []byte
}

// Classified is a question, validated, and whether this host prices it.
type Classified struct {
	Q      *storerec.Question
	Priced bool
}

// Classify validates a question and names its class, as the terms route
// and the engine ask it: exactly the members of one class, every value
// checked, a bucket this host carries, and a chain it serves for a map
// class; anything else is an error.
func (s *Service) Classify(service string, query []byte) (*Classified, error) {
	if service != storerec.LookupService {
		return nil, fmt.Errorf("ls_bstore: this service is %s, not %s", storerec.LookupService, service)
	}
	q, err := storerec.ParseQuery(query, func(b string) bool { return s.buckets[b] }, func(b, o, i string) bool { return s.served[b+"|"+o+"|"+i] })
	if err != nil {
		return nil, errors.New("ls_bstore: refused: " + err.Error())
	}
	free := true
	for _, c := range storerec.Classes {
		if c.Name == q.Class {
			free = c.Free
		}
	}
	return &Classified{Q: q, Priced: !free && s.priced[q.Class]}, nil
}

// heldEntry is whether the host holds the entry with commitment c in
// bucket b, so a map answer can carry it.
func (s *Service) heldEntry(b string, c [32]byte) bool {
	e, ok := s.carriers.get(hex.EncodeToString(c[:]))
	return ok && e.kind == "entry" && e.b == b
}

func tok(t *tokenEntry) Formula   { return Formula{Txid: t.txid, Vout: t.vout} }
func car(e *carrierEntry) Formula { return Formula{Txid: e.txid} }

// Answer answers a validated question. paid says the question was paid
// for (or asked where no payment is due); a class this host prices is
// refused otherwise.
func (s *Service) Answer(ctx context.Context, c *Classified, paid bool) ([]Formula, error) {
	if c.Priced && !paid {
		return nil, fmt.Errorf("ls_bstore: %s is priced on this host; ask its terms route's /lookup over BRC-104", c.Q.Class)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return nil, errors.New("ls_bstore: the index is not restored yet")
	}
	s.host.Metrics().Inc("bstore_lookups_total", map[string]string{"class": c.Q.Class}, 1)
	m := c.Q.Values
	b := m["bucket"]
	now := s.nowMs()
	notAfter := timeKey(uint64(now+1), "") //nolint:gosec // the host's clock is after 1970
	chainOf := func(owner, id string) *chainIdx { return s.chains[b+"|"+owner+"|"+id] }
	out := []Formula{}
	switch c.Q.Class {
	case "bucket":
		v := chainOf(m["owner"], m["id"])
		if v == nil {
			return out, nil
		}
		hs := append([]*tokenEntry(nil), s.heads(ctx, v)...)
		sort.Slice(hs, func(i, j int) bool { return hs[i].txid < hs[j].txid })
		for i, t := range hs {
			if i >= PerSeq {
				break
			}
			out = append(out, tok(t))
		}
		return out, nil
	case "bucket-seq":
		v := chainOf(m["owner"], m["id"])
		if v == nil {
			return out, nil
		}
		seq, _ := strconv.ParseUint(m["seq"], 10, 64)
		at := append([]*tokenEntry(nil), v.bySeq[seq]...)
		sort.SliceStable(at, func(i, j int) bool {
			if at[i].rooted != at[j].rooted {
				return at[i].rooted
			}
			return at[i].txid < at[j].txid
		})
		for i, t := range at {
			if i >= PerSeq {
				break
			}
			out = append(out, tok(t))
		}
		return out, nil
	case "object":
		v := chainOf(m["owner"], m["id"])
		if v == nil {
			return out, nil
		}
		t, ms := s.view(ctx, v)
		if t == nil {
			return out, nil
		}
		ectx, leaves := ms.proveObject(m["key"], func(c [32]byte) bool { return s.heldEntry(b, c) })
		tc, err := storerec.EncodeTokenContext(leaves)
		if err != nil {
			return nil, err
		}
		f := tok(t)
		f.Context = tc
		out = append(out, f)
		if ectx != nil {
			c, _ := ms.get(m["key"])
			e, _ := s.carriers.get(hex.EncodeToString(c[:]))
			ec, err := ectx.Encode()
			if err != nil {
				return nil, err
			}
			g := car(e)
			g.Context = ec
			out = append(out, g)
		}
		return out, nil
	case "list", "export":
		id, prefix, max := m["id"], m["prefix"], storerec.PageList
		if c.Q.Class == "export" {
			id, prefix, max = m["export"], "", storerec.PageExport
		}
		v := chainOf(m["owner"], id)
		if v == nil {
			return out, nil
		}
		t, ms := s.view(ctx, v)
		if t == nil {
			return out, nil
		}
		entries, leaves := ms.provePage(prefix, m["after"], max, func(c [32]byte) bool { return s.heldEntry(b, c) })
		tc, err := storerec.EncodeTokenContext(leaves)
		if err != nil {
			return nil, err
		}
		f := tok(t)
		f.Context = tc
		out = append(out, f)
		for _, x := range entries {
			e, _ := s.carriers.get(hex.EncodeToString(x.C[:]))
			ec, err := x.Ctx.Encode()
			if err != nil {
				return nil, err
			}
			g := car(e)
			g.Context = ec
			out = append(out, g)
		}
		return out, nil
	case "pending":
		v := chainOf(m["owner"], m["id"])
		sv := s.servedIdx[b+"|"+m["owner"]+"|"+m["id"]]
		if v == nil || sv == nil {
			return out, nil
		}
		who := []string{v.owner}
		if t, _ := s.view(ctx, v); t != nil {
			for _, w := range t.rec.Writers {
				who = append(who, hexOf(w))
			}
		}
		done := map[string]bool{}
		var found []string
		byWriter := sv.pendingByKey[m["pending"]]
		for _, w := range who {
			if done[w] {
				continue
			}
			done[w] = true
			list := byWriter[w]
			if list == nil {
				continue
			}
			for i, n := list.lower(notAfter)-1, 0; i >= 0 && n < PerSmall; i, n = i-1, n+1 {
				found = append(found, list.a[i])
			}
		}
		sort.Sort(sort.Reverse(sort.StringSlice(found)))
		for i, k := range found {
			if i >= PerSmall {
				break
			}
			if e, ok := s.carriers.get(cOf(txidOfKey(k))); ok {
				out = append(out, car(e))
			}
		}
		return out, nil
	case "unanchored":
		sv := s.servedIdx[b+"|"+m["owner"]+"|"+m["id"]]
		if sv == nil {
			return out, nil
		}
		list := sv.unanchored[m["writer"]]
		if list == nil {
			return out, nil
		}
		since := now - UnanchoredDays*day
		var aged []string
		i := 0
		if m["after"] != "" {
			i = list.upper(m["after"])
		}
		for ; i < list.size() && len(out) < PageLimit; i++ {
			if e, ok := s.carriers.get(cOf(list.a[i])); ok && e.at >= since {
				out = append(out, car(e))
			} else {
				aged = append(aged, list.a[i])
			}
		}
		// What aged out of the window since the last retention pass is walked
		// once, here, and leaves the index.
		for _, txid := range aged {
			list.remove(txid)
		}
		return out, nil
	case "entry":
		if e, ok := s.carriers.get(cOf(m["entry"])); ok && e.kind == "entry" && e.b == b {
			out = append(out, car(e))
		}
		return out, nil
	case "ads", "ad":
		key, cap, after := b+"|"+m["root"], PageLimit, m["after"]
		if c.Q.Class == "ad" {
			key, cap, after = b+"|"+m["root"]+"|"+m["advertiser"], PerSmall, ""
		}
		list := s.adsLive[key]
		if list == nil {
			return out, nil
		}
		var dead []*adEntry
		i := list.lower(afterTxid(after))
		for i < list.size() {
			// A page never splits a transaction: the next page asks strictly after its txid.
			txid := list.a[i][:64]
			var group []Formula
			for ; i < list.size() && strings.HasPrefix(list.a[i], txid+":"); i++ {
				n, _ := strconv.ParseUint(list.a[i][65:], 10, 32)
				vout := uint32(n)
				ad, ok := s.ads.get(heldKey(b, txid, vout))
				switch {
				case ok && !ad.spent && int64(ad.expiry)*1000 > now: //nolint:gosec // an expiry is a 32-bit time
					group = append(group, Formula{Txid: txid, Vout: vout})
				case ok:
					dead = append(dead, ad)
				}
			}
			// ad has no cursor and no next page: it answers the first eight, a
			// transaction split or not.
			if len(out)+len(group) > cap {
				if c.Q.Class == "ad" {
					out = append(out, group[:cap-len(out)]...)
				}
				break
			}
			out = append(out, group...)
		}
		// An advertisement that expired since the last retention pass is
		// walked once, here, and leaves the live index.
		for _, ad := range dead {
			s.adLive(ad, false)
		}
		return out, nil
	case "fronts":
		list := s.frontsByAdvertiser[b+"|"+m["advertiser"]]
		if list == nil {
			return out, nil
		}
		i := 0
		if m["after"] != "" {
			i = list.upper(m["after"])
		}
		for ; i < list.size() && len(out) < PerSmall; i++ {
			out = append(out, Formula{Txid: list.a[i]})
		}
		return out, nil
	case "front":
		for _, txid := range s.newest(s.frontsByPair[b+"|"+m["advertiser"]+"|"+m["front"]], notAfter, PerSmall, -1) {
			out = append(out, Formula{Txid: txid})
		}
		return out, nil
	case "hold":
		if h, ok := s.holds.get(b + "|" + m["hold"]); ok {
			out = append(out, Formula{Txid: h.txid, Vout: h.vout})
		}
		return out, nil
	case "outcomes", "outcomes-of", "outcomes-by", "outcomes-of-by":
		var k string
		switch c.Q.Class {
		case "outcomes":
			k = b + "|a|" + m["answerer"]
		case "outcomes-of":
			k = b + "|p|" + m["provider"]
		case "outcomes-by":
			k = b + "|a|" + m["answerer"] + "|" + m["buyer"]
		default:
			k = b + "|p|" + m["provider"] + "|" + m["buyer"]
		}
		for _, txid := range s.newest(s.outcomesBy[k], notAfter, PageLimit, now) {
			out = append(out, Formula{Txid: txid})
		}
		return out, nil
	case "outcome-history", "outcome-history-of":
		cursor := keyOfCursor(m["before"])
		k := b + "|a|" + m["outcome-history"]
		if c.Q.Class == "outcome-history-of" {
			k = b + "|p|" + m["outcome-history-of"]
		}
		below := notAfter
		if cursor < notAfter {
			below = cursor
		}
		for _, txid := range s.newest(s.outcomeHistory[k], below, PageLimit, -1) {
			out = append(out, Formula{Txid: txid})
		}
		return out, nil
	case "sweep":
		var sws []*sweepEntry
		for txid := range s.sweepsByOp[b+"|"+m["spent"]] {
			if sw, ok := s.sweeps.get(b + "|" + txid); ok {
				sws = append(sws, sw)
			}
		}
		sort.Slice(sws, func(i, j int) bool {
			x, y := sws[i], sws[j]
			if x.height != y.height {
				return x.height < y.height
			}
			if x.offset != y.offset {
				return x.offset < y.offset
			}
			return x.txid < y.txid
		})
		for i, sw := range sws {
			if i >= PerSmall {
				break
			}
			out = append(out, Formula{Txid: sw.txid})
		}
		return out, nil
	case "history":
		v := chainOf(m["owner"], m["id"])
		if v == nil {
			return out, nil
		}
		hist := s.hist(v)[m["history"]]
		before, _ := strconv.ParseUint(m["before"], 10, 64)
		// The first held change at or above before; the page walks down from below it.
		lo := sort.Search(len(hist), func(i int) bool { return hist[i].t.seq >= before })
		tokens := 0
		// Newest first, by seq; a page never splits a seq, so the next page,
		// asked below the last seq answered, misses none.
		for i := lo - 1; i >= 0; {
			seq := hist[i].t.seq
			j := i
			for j >= 0 && hist[j].t.seq == seq {
				j--
			}
			group := hist[j+1 : i+1]
			if tokens > 0 && tokens+len(group) > PageLimit {
				break
			}
			for g := len(group) - 1; g >= 0; g-- {
				h := group[g]
				tokens++
				out = append(out, tok(h.t))
				if e, ok := s.carriers.get(h.c); ok && e.kind == "entry" && e.b == b {
					out = append(out, car(e))
				}
			}
			i = j
		}
		return out, nil
	case "chain":
		v := chainOf(m["owner"], m["chain"])
		if v == nil {
			return out, nil
		}
		after, _ := strconv.ParseUint(m["after"], 10, 64)
		// The seqs that hold a rooted token only: a stranger's unrooted tokens are never walked.
		seqs := &v.rootedSeqs.sorted
		for i := seqs.upper(after); i < seqs.size(); i++ {
			var at []Formula
			for _, t := range v.bySeq[seqs.a[i]] {
				if t.rooted {
					at = append(at, tok(t))
				}
			}
			// A page never splits a seq.
			if len(out) > 0 && len(out)+len(at) > PageLimit {
				break
			}
			out = append(out, at...)
		}
		return out, nil
	}
	return nil, fmt.Errorf("ls_bstore: no answer for class %s", c.Q.Class)
}

// newest is up to n txids from a list of time keys, newest first below
// below; with since at or above zero, only those admitted since the window
// of OutcomeWindowDays before now began.
func (s *Service) newest(list *sortedStrings, below string, n int, now int64) []string {
	var out []string
	if list == nil {
		return out
	}
	var aged []string
	for i := list.lower(below) - 1; i >= 0 && len(out) < n; i-- {
		txid := txidOfKey(list.a[i])
		if now >= 0 {
			if e, ok := s.carriers.get(cOf(txid)); !ok || now-e.at >= OutcomeWindowDays*day {
				aged = append(aged, list.a[i])
				continue
			}
		}
		out = append(out, txid)
	}
	// An outcome that aged out of the free window since the last retention
	// pass is walked once, here, and leaves it.
	for _, k := range aged {
		list.remove(k)
	}
	return out
}

// Hydrate is an answer with each output's BEEF, as the engine hydrates one,
// read from the host's storage. A context rides along.
func (s *Service) Hydrate(ctx context.Context, formulas []Formula) ([]bmodule.HydratedOutput, error) {
	s.mu.Lock()
	storage := s.storage
	s.mu.Unlock()
	if storage == nil {
		return nil, errors.New("ls_bstore: storage is not restored yet")
	}
	out := make([]bmodule.HydratedOutput, 0, len(formulas))
	for _, f := range formulas {
		h, err := chainhash.NewHashFromHex(f.Txid)
		if err != nil {
			continue
		}
		o, err := storage.FindOutput(ctx, &transaction.Outpoint{Txid: *h, Index: f.Vout}, "")
		if err != nil {
			return nil, err
		}
		if o == nil || o.Beef == nil {
			continue
		}
		out = append(out, bmodule.HydratedOutput{Beef: o.Beef, OutputIndex: f.Vout, Context: f.Context})
	}
	return out, nil
}

// Lookup implements module.LookupService: asked through the engine, a
// class the host prices is refused here. The answer is hydrated here and
// carries each output's context (bcommon module.HydratedOutput).
func (s *Service) Lookup(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
	c, err := s.Classify(q.Service, q.Query)
	if err != nil {
		return nil, err
	}
	formulas, err := s.Answer(ctx, c, false)
	if err != nil {
		return nil, err
	}
	outs, err := s.Hydrate(ctx, formulas)
	if err != nil {
		return nil, err
	}
	ans := &lookup.LookupAnswer{Type: lookup.AnswerTypeOutputList, Outputs: make([]*lookup.OutputListItem, 0, len(outs)), Result: outs}
	for _, o := range outs {
		ans.Outputs = append(ans.Outputs, &lookup.OutputListItem{Beef: o.Beef, OutputIndex: o.OutputIndex})
	}
	return ans, nil
}

// GetDocumentation implements module.LookupService.
func (*Service) GetDocumentation() string {
	return strings.Join([]string{
		"# ls_bstore",
		"",
		"Answers every bstore bucket this host carries (spec section 11). Free: `bucket`,",
		"`bucket-seq`, `object`, `list`, `pending`, `unanchored`, `entry`, `ads`, `ad`, `fronts`,",
		"`front`, `hold`, `outcomes`, `outcomes-of`, `outcomes-by`, `outcomes-of-by` and `sweep`.",
		"Priceable: `history`, `chain`, `export`, `outcome-history` and `outcome-history-of`. The",
		"map classes answer for the chains this host serves, each proven against its latest",
		"rooted, consistent token. A question must carry exactly the members of one class;",
		"anything else is refused with an error. An empty answer means only that this host",
		"holds nothing that answers.",
	}, "\n")
}

// GetMetaData implements module.LookupService.
func (*Service) GetMetaData() *overlay.MetaData {
	return &overlay.MetaData{Name: storerec.LookupService, Description: "Answers bstore buckets: tokens, key maps, entries, advertisements, outcomes and fronts."}
}

// AdmissionMode implements module.LookupService.
func (*Service) AdmissionMode() bmodule.AdmissionMode { return bmodule.AdmitWholeTx }

// SpendNotificationMode implements module.LookupService.
func (*Service) SpendNotificationMode() bmodule.SpendNotificationMode { return bmodule.SpendNone }

// byteArray writes bytes as a JSON array of numbers, as the TypeScript
// route writes a number[].
type byteArray []byte

func (b byteArray) MarshalJSON() ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, c := range b {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.Itoa(int(c)))
	}
	sb.WriteByte(']')
	return []byte(sb.String()), nil
}

type outputJSON struct {
	Beef        byteArray `json:"beef"`
	OutputIndex uint32    `json:"outputIndex"`
	Context     byteArray `json:"context,omitempty"`
}

// OutputList is the engine's lookup answer as JSON.
type OutputList struct {
	Type    string       `json:"type"`
	Outputs []outputJSON `json:"outputs"`
}

// RouteAnswerer is ls_bstore as the terms route asks its questions: a
// priced answer is hydrated as the engine hydrates one.
type RouteAnswerer struct{ LS *Service }

var _ termsroute.Answerer = RouteAnswerer{}

// Restored implements termsroute.Answerer.
func (r RouteAnswerer) Restored() bool { return r.LS.Restored() }

// Classify implements termsroute.Answerer.
func (r RouteAnswerer) Classify(_ context.Context, q termsroute.Question) (string, error) {
	c, err := r.LS.Classify(q.Service, q.Query)
	if err != nil {
		return "", err
	}
	return c.Q.Class, nil
}

// Answer implements termsroute.Answerer.
func (r RouteAnswerer) Answer(ctx context.Context, q termsroute.Question) (any, error) {
	c, err := r.LS.Classify(q.Service, q.Query)
	if err != nil {
		return nil, err
	}
	formulas, err := r.LS.Answer(ctx, c, true)
	if err != nil {
		return nil, err
	}
	outs, err := r.LS.Hydrate(ctx, formulas)
	if err != nil {
		return nil, err
	}
	list := OutputList{Type: "output-list", Outputs: make([]outputJSON, 0, len(outs))}
	for _, o := range outs {
		list.Outputs = append(list.Outputs, outputJSON{Beef: o.Beef, OutputIndex: o.OutputIndex, Context: o.Context})
	}
	return list, nil
}
