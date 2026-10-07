package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/bstore/internal/anchor"
	"github.com/lightwebinc/bstore/internal/fundimport"
	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

const initHelp = `usage: bstore init -node URL -settle LEG -headers URL -hosts URLS [flags]

Create the home: the identity's wallet (its key is what a bucket's owner
names as a writer, and what owns the buckets it creates) and config.json.
An existing wallet is kept; an existing config.json is not overwritten.
Back up the home: the wallet is the identity.`

func cmdInit(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("init", initHelp)
	network := fs.String("network", "main", "main, test, or regtest (a chain you run)")
	node := fs.String("node", "", "the node's asset API URL")
	settle := fs.String("settle", "", "the settlement leg: rpc:URL, tcp:ADDR or arcade:URL")
	hdr := fs.String("headers", "", "the header source: woc:main, woc:test, chaintracks:URL, or a bridge URL")
	hosts := fs.String("hosts", g.hosts, "overlay hosts, comma separated")
	facade := fs.String("facade", "", "the plane's facade (none: unicast to hosts and providers)")
	providers := fs.String("providers", "", "providers' submit URLs, comma separated")
	uploads := fs.String("uploads", "", "providers' public URLs for upload sessions, comma separated")
	ca := fs.String("ca", "", "a PEM file of certificates trusted besides the system's")
	private := fs.Bool("fetch-private", false, "reach advertised URLs on loopback or private addresses")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("init takes flags only")
	}
	if err := os.MkdirAll(g.home, 0o700); err != nil {
		return err
	}
	id := ""
	if w, err := bwallet.Open(publish.WalletDir(g.home), publish.Profile); err == nil {
		id = w.Signer().IdentityHex()
		g.out("identity %s (exists)", id)
	} else {
		if id, err = publish.Init(g.home); err != nil {
			return err
		}
		g.out("identity %s", id)
	}
	if _, err := os.Stat(g.configPath()); err == nil {
		g.out("config %s (exists, kept)", g.configPath())
		return nil
	}
	c := &Config{Network: *network, Node: strings.TrimRight(*node, "/"), Settle: *settle, Headers: *hdr, Hosts: splitList(*hosts),
		Facade: strings.TrimRight(*facade, "/"), Providers: splitList(*providers), Uploads: splitList(*uploads), CA: *ca, FetchPrivate: *private}
	if err := c.check(); err != nil {
		return usage("%v", err)
	}
	if err := saveConfig(g.configPath(), c); err != nil {
		return err
	}
	g.out("config %s", g.configPath())
	return nil
}

const fundHelp = `usage: bstore fund [-txid TXID | -blocks N | -from H [-to H]]

With no flag, print the identity, its fund address and its pool.

-txid imports a payment you made: send a small amount from your own wallet
to the fund address, wait for it to be mined, then pass its txid. Every
output of it that pays the fund address and that the node shows unspent is
taken into the pool, its proof checked against the header source. This is
how a home is funded on mainnet and testnet.

-blocks and -from are coinbase: only on a regtest chain you run
(development and tests). -blocks mines N blocks to the fund address through
the node's RPC (settle rpc:URL) and pools the coinbase; -from takes the
coinbase outputs paying the fund address in blocks H to the tip (or -to)
into the pool, and finds no ordinary payment.`

func cmdFund(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("fund", fundHelp)
	txid := fs.String("txid", "", "import the outputs of a mined payment to the fund address (mainnet, testnet)")
	blocks := fs.Int("blocks", 0, "coinbase: only on a regtest chain you run (development and tests): blocks to mine to the fund address")
	from := fs.Uint("from", 0, "coinbase: only on a regtest chain you run (development and tests): first block height to rescan for coinbase")
	to := fs.Uint("to", 0, "last block height to rescan for coinbase (default the tip)")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("fund takes flags only")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	w := h.pub.Wallet()
	asset := h.pub.Asset()
	switch {
	case *txid != "":
		hc, err := headers.NewSource(h.cfg.Headers)
		if err != nil {
			return usage("headers: %v", err)
		}
		res, err := fundimport.Import(ctx, w.Signer(), w.Pool, asset, hc, *txid)
		if errors.Is(err, fundimport.ErrNotMined) {
			return incomplete("%v", err)
		}
		if err != nil {
			return err
		}
		g.out("%s mined at height %d: %d output(s) to the fund address, %d imported (%d satoshis), %d already held, %d spent",
			res.TxID, res.Height, res.Paying, res.Added, res.Satoshis, res.Held, res.Spent)
		g.out("pool %d output(s), %d satoshis", w.Pool.Count(), w.Pool.Balance())
	case *blocks > 0:
		kind, addr, _ := strings.Cut(h.cfg.Settle, ":")
		if kind != "rpc" || h.cfg.Network == "main" {
			return usage("fund -blocks is coinbase: only on a regtest chain you run (development and tests), with settle rpc:URL; on mainnet and testnet pay the fund address and use fund -txid")
		}
		rpc := &nodeapi.RPC{URL: addr, User: h.cfg.RPCUser, Pass: h.cfg.RPCPass, ID: "bstore"}
		n, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, rpc, asset, *blocks, 50)
		if err != nil {
			return err
		}
		g.out("%d coinbase output(s) pooled; pool %d satoshis", n, w.Pool.Balance())
	case *from > 0:
		top := uint32(*to)
		if top == 0 {
			tip, err := asset.BestHeader(ctx)
			if err != nil {
				return err
			}
			top = tip.Height
		}
		n, err := bwallet.Rescan(ctx, w.Signer(), w.Pool, asset, uint32(*from), top)
		if err != nil {
			return err
		}
		g.out("%d output(s) found; pool %d satoshis", n, w.Pool.Balance())
	default:
		addr, err := w.Signer().FundAddress(h.cfg.Network == "main")
		if err != nil {
			return err
		}
		g.out("identity     %s", w.Signer().IdentityHex())
		g.out("fund address %s", addr)
		g.out("pool         %d output(s), %d satoshis", w.Pool.Count(), w.Pool.Balance())
	}
	return nil
}

const termsHelp = `usage: bstore terms URL

A provider's terms document, from its front (a URL ending /bstore/v1), over
BRC-104; or a host's ls_bstore terms, from its base URL. The 402 a priced
route answers is authoritative; this is informative.`

func cmdTerms(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("terms", termsHelp)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 || !strings.HasPrefix(pos[0], "http") {
		return usage("terms URL")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	u := strings.TrimRight(pos[0], "/")
	var body []byte
	if strings.HasSuffix(u, storerec.FrontSuffix) {
		call, err := h.pub.Get(ctx, u+"/terms")
		if err != nil {
			return err
		}
		if call.Status != http.StatusOK {
			return incomplete("%s answered %d", u, call.Status)
		}
		body = call.Body
	} else {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u+"/ls_bstore/terms", nil)
		res, err := h.hostHC.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode == http.StatusNotFound {
			g.out("%s publishes no terms: it prices nothing", u)
			return nil
		}
		if res.StatusCode != http.StatusOK {
			return incomplete("%s answered %d", u, res.StatusCode)
		}
		if body, err = io.ReadAll(io.LimitReader(res.Body, 64<<10)); err != nil {
			return err
		}
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return incomplete("%s answered something that is not JSON", u)
	}
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Fprintln(g.stdout, termsafe.Text(string(out)))
	return nil
}

const doctorHelp = `usage: bstore doctor

Report the home (identity, pool, funding trees, holds, races, chains, what
is in the outbox and owed by upload) and whether the node, the header
source, the hosts and the providers answer. Reads only: it publishes
nothing and spends nothing. What is owed is 'bstore due'.`

func cmdDoctor(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("doctor", doctorHelp)
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("doctor takes no arguments")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	w := h.pub.Wallet()
	g.out("home        %s", g.home)
	g.out("network     %s", h.cfg.Network)
	g.out("identity    %s", h.pub.IdentityHex())
	g.out("pool        %d output(s), %d satoshis, change of %d transaction(s) held until mined", w.Pool.Count(), w.Pool.Balance(), len(w.Pool.UnprovenTxids()))
	if tip, err := h.pub.Asset().BestHeader(ctx); err != nil {
		g.out("node        %s: %v", h.cfg.Node, firstLine(err.Error()))
	} else {
		g.out("node        %s tip %d", h.cfg.Node, tip.Height)
	}
	if hc, err := h.rd.Headers.CurrentHeight(ctx); err != nil {
		g.out("headers     %s: %v", h.cfg.Headers, firstLine(err.Error()))
	} else {
		g.out("headers     %s height %d", h.cfg.Headers, hc)
	}
	for _, x := range h.cfg.Hosts {
		g.out("host        %s: %s", x, reach(ctx, h.hostHC, x+"/healthz", x+"/"))
	}
	for _, x := range h.cfg.Providers {
		g.out("provider    %s: %s", x, reach(ctx, h.hostHC, x+"/healthz"))
	}
	if h.cfg.Facade != "" {
		g.out("facade      %s", h.cfg.Facade)
	}
	holds := h.pub.Holds()
	ready, used := 0, 0
	for _, x := range holds {
		if x.Used {
			used++
		} else if x.BumpHex != "" {
			ready++
		}
	}
	g.out("holds       %d minted: %d ready, %d taken", len(holds), ready, used)
	races, _ := h.pub.Races()
	states := map[string]int{}
	for _, r := range races {
		states[r.State]++
	}
	g.out("races       %d %v", len(races), states)
	for _, r := range races {
		if r.State != publish.RaceOpen && r.State != publish.RaceClosed && r.State != publish.RaceLapsed {
			continue
		}
		// Why a candidate's draws went, from the draw log, which progress
		// notes never push out.
		for _, c := range r.Cands {
			if c.Status == publish.CandPassed || len(c.DrawLog) == 0 {
				continue
			}
			a := c.DrawLog[len(c.DrawLog)-1]
			g.out("draws       race %s advertiser %s: %s, %d spent, last %s: %s", short(r.Entry), short(c.Advertiser), c.Status, c.Draws, a.Outcome, a.Why)
		}
	}
	for _, r := range races {
		if age, ok := r.RemittedAge(time.Now()); ok && age >= publish.RemittedStale {
			g.out("ALARM       race %s: settlement %s remitted %s ago, no proof served: it may never mine", short(r.Entry), short(r.Settlement.Txid), age.Truncate(time.Second))
		}
	}
	total, by := h.pub.Outbox().Owed()
	if total > 0 {
		worst, most := "", int64(0)
		for u, n := range by {
			if n > most {
				worst, most = u, n
			}
		}
		g.out("owed        %d bytes in the outbox, most to %s (%d)", total, worst, most)
	}
	chains, _ := h.tool.Store.Load()
	for _, c := range chains {
		p := ""
		if c.Pending != nil {
			p = fmt.Sprintf(", token %d pending (%s)", c.Pending.Seq, short(c.Pending.Txid))
		}
		g.out("owns        %s id %s seq %d%s", c.Bucket, short(c.ID), c.Seq, p)
	}
	g.out("outbox      %d object(s) not yet delivered everywhere", h.pub.Outbox().Pending())
	g.out("uploads     %d closure(s) owed to a provider", h.pub.OwedUploads())
	if sw := h.pub.Sweeps(); len(sw) > 0 {
		g.out("killed      %d sweep(s) recorded: this identity publishes nothing more", len(sw))
	}
	return nil
}

func reach(ctx context.Context, hc *http.Client, urls ...string) string {
	var last string
	for _, u := range urls {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		req, _ := http.NewRequestWithContext(cctx, http.MethodGet, u, nil)
		res, err := hc.Do(req)
		cancel()
		if err != nil {
			last = firstLine(err.Error())
			continue
		}
		res.Body.Close()
		if res.StatusCode < 500 {
			return fmt.Sprintf("answers (%d)", res.StatusCode)
		}
		last = fmt.Sprintf("%d", res.StatusCode)
	}
	return "does not answer: " + last
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 240 {
		s = s[:240]
	}
	return s
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

const syncHelp = `usage: bstore sync

Send everything owed: the outbox (every object a target has not taken,
the same bytes), closures owed to a provider's upload session, hold
transactions not yet seen mined, and the change of payments made. Then
record which of this identity's entries a token of each configured chain
has named.`

func cmdSync(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("sync", syncHelp)
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("sync takes no arguments")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	h.pub.CollectChange(ctx)
	payments := h.pub.ReconcilePayments(ctx)
	h.pub.CheckHolds(ctx)
	h.pub.Outbox().RetryAll(ctx)
	left := h.pub.RetryUploads(ctx)
	named, problems := h.markNamed(ctx)
	g.out("outbox %d left; uploads %d owed; payments %d not yet mined; %d entr(ies) named by a token", h.pub.Outbox().Pending(), left, payments, named)
	for _, p := range problems {
		g.say("%s", p)
	}
	if h.pub.Outbox().Pending() > 0 || left > 0 || len(problems) > 0 {
		return incomplete("something is still owed or could not be checked; run sync again later")
	}
	return nil
}

// markNamed walks each configured chain and records which of this
// identity's entries its tokens named.
func (h *home) markNamed(ctx context.Context) (int, []string) {
	mine := map[string]map[string]bool{} // bucket -> txids
	for _, l := range h.pub.Entries() {
		if l.Named == nil {
			if mine[l.Bucket] == nil {
				mine[l.Bucket] = map[string]bool{}
			}
			mine[l.Bucket][l.Txid] = true
		}
	}
	n := 0
	var problems []string
	for _, b := range h.cfg.Buckets {
		if len(mine[b.Bucket]) == 0 {
			continue
		}
		ch, err := b.chain()
		if err != nil {
			continue
		}
		toks, err := h.rd.Tokens(ctx, ch)
		if err != nil {
			problems = append(problems, fmt.Sprintf("bucket %s: %v", b.Bucket, err))
			continue
		}
		for _, t := range toks {
			var cs [][32]byte
			for _, c := range t.Bucket.Changes {
				cs = append(cs, c.C)
			}
			cs = append(cs, t.Bucket.Consumed...)
			for _, c := range cs {
				id := displayHex(c)
				if mine[b.Bucket][id] {
					if err := h.pub.MarkNamed(id, t.Bucket.Seq); err == nil {
						n++
						delete(mine[b.Bucket], id)
					}
				}
			}
		}
	}
	return n, problems
}

const fillHelp = `usage: bstore fill

Replay to every host what this home made and the host lacks: each of its
entries no token of the chain has named yet (asked by entry), the hold
transactions of its open races, and, for each chain it owns, every token
from seq 0 with its parent (asked by bucket-seq). Objects are self
verifying: a host that lacked one admits it as if it had arrived the
first time (spec section 12.6).`

func cmdFill(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("fill", fillHelp)
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("fill takes no arguments")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	sent, problems := h.fillEntries(ctx)
	ts, tp := h.fillTokens(ctx)
	sent += ts
	problems = append(problems, tp...)
	g.out("%d object(s) replayed", sent)
	for _, p := range problems {
		g.say("%s", p)
	}
	if len(problems) > 0 {
		return incomplete("%d host(s) could not be filled", len(problems))
	}
	return nil
}

func (h *home) fillEntries(ctx context.Context) (int, []string) {
	sent := 0
	var problems []string
	if h.pub.Killed() {
		// The kill retracted every entry no token anchored.
		return 0, nil
	}
	for _, l := range h.pub.Entries() {
		if l.Named != nil {
			continue
		}
		beef, err := h.pub.EntryBEEF(l.Txid)
		if err != nil {
			continue
		}
		for _, host := range h.cfg.Hosts {
			a, err := h.rd.Ask(ctx, host, map[string]string{"bucket": l.Bucket, "entry": l.Txid})
			if err != nil {
				problems = append(problems, fmt.Sprintf("host %s: %v", host, firstLine(err.Error())))
				continue
			}
			has := false
			for _, o := range a.Outputs {
				if e, err := h.rd.VerifyEntry(ctx, l.Bucket, o.Beef); err == nil && e.Txid() == l.Txid {
					has = true
				}
			}
			if !has {
				if ok := h.pub.Resend(ctx, l.Txid+"-"+short(host), "entry", beef, []publish.Target{publish.HostTarget(host, l.Bucket)}); len(ok) > 0 {
					sent++
				}
			}
		}
	}
	return sent, problems
}

func (h *home) fillTokens(ctx context.Context) (int, []string) {
	chains, _ := h.tool.Store.Load()
	sent := 0
	var problems []string
	for _, c := range chains {
		ref := BucketRef{Bucket: c.Bucket, Owner: h.pub.IdentityHex(), ID: c.ID}
		ch, err := ref.chain()
		if err != nil {
			continue
		}
		toks, err := h.rd.Tokens(ctx, ch)
		if err != nil {
			problems = append(problems, fmt.Sprintf("bucket %s: %v", c.Bucket, err))
			continue
		}
		for _, host := range h.cfg.Hosts {
			for i, t := range toks {
				a, err := h.rd.Ask(ctx, host, map[string]string{"bucket": c.Bucket, "owner": ref.Owner, "id": ref.ID, "seq": fmt.Sprint(t.Bucket.Seq)})
				if err != nil {
					problems = append(problems, fmt.Sprintf("host %s: %v", host, firstLine(err.Error())))
					break
				}
				has := false
				for _, o := range a.Outputs {
					if tx, err := transactionOf(o.Beef); err == nil && tx == t.TxidHex() {
						has = true
					}
				}
				if has {
					continue
				}
				parent, err := h.tokenParent(ctx, c, toks, i)
				if err != nil {
					problems = append(problems, fmt.Sprintf("bucket %s: token %d: %v", c.Bucket, t.Bucket.Seq, err))
					break
				}
				beef, err := storerec.TokenBEEF(t.Tx(), parent)
				if err != nil {
					problems = append(problems, err.Error())
					break
				}
				if ok := h.pub.Resend(ctx, t.TxidHex()+"-"+short(host), "bucket token", beef, []publish.Target{publish.HostTarget(host, c.Bucket)}); len(ok) > 0 {
					sent++
				}
			}
		}
	}
	return sent, problems
}

const dueHelp = `usage: bstore due

What this home owes, and exit 3 when it owes anything: objects in the
outbox, closures owed to a provider, hold transactions not yet mined, open
races (with the blocks left to their deadline), settlements not yet mined
or whose remittances a payee has not taken (a payee whose output the node
shows spent is done; one a front forwards for, a bridged incumbent, is
listed and not owed), hold outputs whose deadline has
come and that nobody won (bstore reclaim), and the entries of this
identity no token has named yet.`

func cmdDue(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("due", dueHelp)
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("due takes no arguments")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	owed := 0
	note := func(format string, a ...any) {
		owed++
		g.out(format, a...)
	}
	if n := h.pub.Outbox().Pending(); n > 0 {
		note("outbox: %d object(s) not yet taken by every target (bstore sync)", n)
	}
	if n := h.pub.OwedUploads(); n > 0 {
		note("uploads: %d closure(s) owed to a provider (bstore sync)", n)
	}
	for _, x := range h.pub.Holds() {
		if x.BumpHex == "" {
			note("hold transaction %s: not yet seen mined (bstore sync)", short(x.Txid))
			break
		}
	}
	tip := uint64(0)
	if t, err := h.pub.Asset().BestHeader(ctx); err == nil {
		tip = uint64(t.Height)
	}
	races, _ := h.pub.Races()
	for _, r := range races {
		e := r.EntryOf()
		switch r.State {
		case publish.RaceOpen:
			left := int64(e.Hold.Deadline) - int64(tip)
			note("race %s (%s): open, %d of K = %d passed, %d block(s) to the deadline (bstore holds, then settle)", short(r.Entry), e.Key, len(r.Passed()), e.Hold.K, left)
		case publish.RaceSettling:
			note("race %s: settlement %s not yet seen mined (bstore settle)", short(r.Entry), short(r.Settlement.Txid))
		case publish.RaceRemitted:
			if age, _ := r.RemittedAge(time.Now()); age >= publish.RemittedStale {
				note("race %s: ALARM settlement %s remitted %s ago and the node serves no proof of it: it may never mine (bstore settle; check the node and arcade)", short(r.Entry), short(r.Settlement.Txid), age.Truncate(time.Second))
				break
			}
			note("race %s: settlement %s remitted on the network's acceptance, not yet seen mined (bstore settle collects its proof)", short(r.Entry), short(r.Settlement.Txid))
		case publish.RaceSettled:
			if err := h.pub.CheckPayouts(ctx, r); err != nil {
				g.say("race %s: %v", short(r.Entry), err)
			}
			for _, p := range r.Settlement.Payees {
				switch {
				case p.Done():
				case r.Forwarder(p.Advertiser) != "":
					// Not owed by this home: the front forwards it, and
					// the node shows when the payee spends it.
					g.out("race %s: rank %d's share is %s (not owed)", short(r.Entry), p.Rank, publish.ForwardedNote(r.Forwarder(p.Advertiser)))
				default:
					note("race %s: rank %d's remittance not yet taken (bstore settle)", short(r.Entry), p.Rank)
				}
			}
		case publish.RaceClosed, publish.RaceLapsed:
			if tip >= e.Hold.Deadline {
				note("race %s: nobody was paid and the deadline has come: reclaim the bounty (bstore reclaim)", short(r.Entry))
			}
		case publish.RaceReclaim:
			note("race %s: reclaim %s not yet seen mined (bstore reclaim)", short(r.Entry), short(r.Reclaim.Txid))
		}
	}
	pending := 0
	for _, l := range h.pub.Entries() {
		if l.Named == nil {
			pending++
		}
	}
	if pending > 0 {
		note("entries: %d no token has named yet (the owner anchors them; bstore sync records it)", pending)
	}
	chains, _ := h.tool.Store.Load()
	for _, c := range chains {
		if c.Pending != nil {
			note("bucket %s: token %d recorded and not yet published (bstore anchor)", c.Bucket, c.Pending.Seq)
		}
	}
	if owed > 0 {
		return &exitError{code: exitIncomplete}
	}
	g.out("nothing is due")
	return nil
}

const killHelp = `usage: bstore kill -confirm KILL [-yes]

The kill switch (spec sections 12.4 and 13.5). It sweeps every funding tree
this home holds (pieces, entries, outcomes), one mined transaction a tree,
every output used or not, and publishes each sweep to the records topic of
every bucket this identity published in. Once a sweep mines, every carrier
its outputs funded is a double spend, and every honest host retracts it:
every entry no token anchored stops being answered. It is IRREVERSIBLE,
and the identity publishes nothing after it.

Without -yes it prints what it would sweep and sends nothing. A run that
stops part way is finished by running kill again.

What it does not do: a key a token anchored stays anchored (an S3 delete is
a delete entry and the owner's next token, never a sweep); hold outputs
are not touched (bstore reclaim); a copy anyone kept stays a copy.`

func cmdKill(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("kill", killHelp)
	confirm := fs.String("confirm", "", "must be the literal KILL")
	yes := fs.Bool("yes", false, "sweep: mine and publish")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("kill -confirm KILL [-yes]")
	}
	if *confirm != "KILL" {
		return usage("kill sweeps every funding tree of this home and retracts every entry no token anchored; pass -confirm KILL")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	plan := h.pub.KillPlan()
	if !*yes {
		for _, t := range plan {
			s := ""
			if t.Unmined {
				s = " (not mined: left)"
			}
			g.out("would sweep %s tree %s: %d output(s)%s", t.Kind, short(t.Txid), len(t.Vouts), s)
		}
		g.out("%d tree(s); nothing sent (add -yes)", len(plan))
		return nil
	}
	done, err := h.pub.Kill(ctx)
	for _, s := range done {
		g.out("swept %s tree %s: %d output(s) in %s", s.Kind, short(s.Tree), len(s.Vouts), s.Txid)
	}
	for _, s := range h.pub.Sweeps() {
		if !s.Published && s.Failed == "" {
			g.say("sweep %s is not yet taken by every host; bstore kill again sends it", short(s.Txid))
			if err == nil {
				err = incomplete("a sweep is not yet published everywhere")
			}
		}
	}
	if err != nil {
		if errors.Is(err, publish.ErrNotYet) {
			return incomplete("%v", err)
		}
		return err
	}
	g.out("killed: %d sweep(s); this identity publishes nothing more", len(h.pub.Sweeps()))
	return nil
}

func displayHex(h [32]byte) string {
	var r [32]byte
	for i := range h {
		r[i] = h[31-i]
	}
	return hex.EncodeToString(r[:])
}

// transactionOf is the display txid of a BEEF's subject.
func transactionOf(beef []byte) (string, error) {
	tx, err := publish.BEEFSubject(beef, 1<<20)
	if err != nil {
		return "", err
	}
	return tx.TxID().String(), nil
}

// tokenParent is the transaction a token's BEEF carries beside it: the
// token before it, or for the first, the chain tree this home recorded.
func (h *home) tokenParent(_ context.Context, c *anchor.Chain, toks []*reader.Token, i int) (*transaction.Transaction, error) {
	if i > 0 {
		return toks[i-1].Tx(), nil
	}
	if c.Tree == "" || c.TreeBump == "" {
		return nil, errors.New("the chain tree is not recorded in this home")
	}
	return funding.Rebuild(c.Tree, c.TreeBump, "")
}
