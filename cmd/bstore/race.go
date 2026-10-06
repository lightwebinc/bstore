package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/storerec"
)

const holdsHelp = `usage: bstore holds [-entry TXID] [-watch DURATION] [-budget SATS]

The hold races of this identity's bounties, judged by this client (spec
section 9.4): for each open race it asks every host for the root's
advertisements, records when it first sees each eligible one (mined in its
own headers, the entry's root, expiring no earlier than the retention,
fileSize the root node's length, a CHIRP URL, not the anyone key), ranks
them by that time (ties in one look to the lower tie key, spec section
9.4), and challenges each candidate in
rank order at its front with a settlement challenge of min(checkC, freeC)
samples, within the provider's terms (freeC, minAge, minBounty), answered
free. A candidate that does not pass is skipped; a failure is published as
an outcome record. -watch keeps judging until the race closes (K passed) or
the time runs out. -budget pays a provider that prices its answer, up to
that much a challenge.`

func cmdHolds(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("holds", holdsHelp)
	entry := fs.String("entry", "", "one race, by its entry's txid")
	watch := fs.Duration("watch", 0, "keep judging this long")
	budget := fs.Uint64("budget", 0, "the most paid for one challenge a provider prices")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("holds takes flags only")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	races, err := h.races(*entry)
	if err != nil {
		return err
	}
	o := h.raceOptions(*budget)
	end := time.Now().Add(*watch)
	failed := map[string]bool{}
	for {
		open := 0
		for _, r := range races {
			if !r.Open() {
				continue
			}
			if err := h.pub.Step(ctx, r, o); err != nil {
				g.say("race %s: %v", short(r.Entry), err)
				failed[r.Entry] = true
			} else {
				delete(failed, r.Entry)
			}
			if r.Open() && !r.Closed(time.Now(), 0) {
				open++
			}
		}
		if open == 0 || time.Now().After(end) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	for _, r := range races {
		h.printRace(r)
	}
	if len(failed) > 0 {
		return incomplete("%d race(s) could not be judged now", len(failed))
	}
	return nil
}

// races are this home's races, or the one an entry names.
func (h *home) races(entry string) ([]*publish.Race, error) {
	if entry != "" {
		r, err := h.pub.Race(strings.ToLower(entry))
		if err != nil {
			return nil, usage("no race of entry %s in this home", entry)
		}
		return []*publish.Race{r}, nil
	}
	return h.pub.Races()
}

func (h *home) printRace(r *publish.Race) {
	g := h.g
	e := r.EntryOf()
	g.out("race %s  key %s  %s", r.Entry, e.Key, r.State)
	g.out("  hold %s  %d satoshis  t %d  K %d  checkC %d  deadline %d", r.Hold, e.Hold.Satoshis, e.Hold.T, e.Hold.K, e.Hold.CheckC, e.Hold.Deadline)
	if r.Note != "" {
		g.out("  %s", r.Note)
	}
	for i, c := range r.Ranked() {
		last := ""
		if n := len(c.Attempts); n > 0 {
			a := c.Attempts[n-1]
			last = fmt.Sprintf("  last: %s c=%d %s", a.Outcome, a.C, a.Why)
		}
		seen := time.Unix(0, c.FirstSeen).UTC().Format("15:04:05.000")
		g.out("  %2d. %s  first seen %s  %s%s", i+1, c.Advertiser, seen, c.Status, last)
	}
	if s := r.Settlement; s != nil {
		g.out("  settlement %s", s.Txid)
		for _, p := range s.Payees {
			st := "remitted"
			switch {
			case p.Refused != "":
				st = "refused: " + p.Refused
			case p.Spent != "" && !p.Remitted:
				st = "spent by the payee in " + p.Spent
			case !p.Remitted:
				st = "not yet remitted"
				if p.Last != "" {
					st += ": " + p.Last
				}
			}
			g.out("    rank %d  %s  %d satoshis  output %d  %s", p.Rank, p.Advertiser, p.Satoshis, p.Vout, st)
		}
		for _, d := range s.Dropped {
			g.out("    dropped (advertisement cancelled) %s", d)
		}
	}
	if rc := r.Reclaim; rc != nil {
		g.out("  reclaim %s", rc.Txid)
	}
	for _, o := range r.Outcomes {
		g.out("  outcome %s of %s published: %s", o.Outcome, short(o.Advertiser), o.Txid)
	}
}

const settleHelp = `usage: bstore settle [-entry TXID] [-close] [-wait DURATION] [-budget SATS]

Settle each closed race by rank (spec section 9.5): when K advertisers have
passed, or the race window (config.json's race_window_seconds) has ended
with at least t, or with -close now. Before it pays, it checks each payee's
advertisement unspent by positive evidence, a renewal counting as unspent;
a cancelled payee is dropped, the ranks below move up and t is counted
again. One mined transaction spends the hold output and pays each rank, by
square-root weights summing to exactly the hold output, a P2PKH output to
the key BRC-29 derives for its advertiser (prefix the entry's commitment,
suffix the rank); the fee from another coin. It is recorded before it is
sent: settle again resends the same bytes and never builds a second one.
Once it mines, each payee's remittance goes to its settle route; settle
again resends any not taken. A payee whose output the node shows spent
has its share and is owed nothing more; one that passed at a front other
than its own (a bridge in front of an incumbent) is told by that front,
which forwards the settlement, so its remittance is not counted as owed.`

func cmdSettle(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("settle", settleHelp)
	entry := fs.String("entry", "", "one race, by its entry's txid")
	closeNow := fs.Bool("close", false, "close the race now and settle with what has passed")
	wait := fs.Duration("wait", 0, "judge until the race closes, this long at most")
	budget := fs.Uint64("budget", 0, "the most paid for one challenge a provider prices")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("settle takes flags only")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	races, err := h.races(*entry)
	if err != nil {
		return err
	}
	o := h.raceOptions(*budget)
	var left, hard []string
	for _, r := range races {
		switch r.State {
		case publish.RaceOpen, publish.RaceSettling, publish.RaceRemitted, publish.RaceSettled:
		default:
			continue
		}
		if r.State == publish.RaceSettled && remitted(r) {
			continue
		}
		if r.Open() && r.Settlement == nil {
			end := time.Now().Add(*wait)
			for {
				if err := h.pub.Step(ctx, r, o); err != nil {
					g.say("race %s: %v", short(r.Entry), err)
				}
				if !r.Open() || r.Closed(time.Now(), o.Window) || *closeNow || time.Now().After(end) {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
			if r.State == publish.RaceClosed || r.State == publish.RaceLapsed {
				h.printRace(r)
				continue
			}
			if !r.Closed(time.Now(), o.Window) && !*closeNow {
				left = append(left, fmt.Sprintf("race %s: open, %d of K = %d passed (holds -watch, or settle -close)", short(r.Entry), len(r.Passed()), r.EntryOf().Hold.K))
				continue
			}
		}
		_, err := h.pub.Settle(ctx, r, *closeNow)
		h.printRace(r)
		switch {
		case err == nil:
		case errors.Is(err, publish.ErrNotYet), errors.Is(err, publish.ErrPastDeadline):
			left = append(left, fmt.Sprintf("race %s: %v", short(r.Entry), err))
		default:
			// The next race is still settled; this one is reported.
			hard = append(hard, fmt.Sprintf("race %s: %v", short(r.Entry), err))
		}
	}
	for _, l := range append(hard, left...) {
		g.say("%s", l)
	}
	switch {
	case len(hard) > 0:
		return &exitError{code: exitUsage}
	case len(left) > 0:
		return &exitError{code: exitIncomplete}
	}
	return nil
}

func remitted(r *publish.Race) bool { return r.Remitted() }

const reclaimHelp = `usage: bstore reclaim [-entry TXID] [-unused]

Spend each hold output nobody won back to the fund key once its deadline
has come (spec section 9.6): a race closed with fewer than t passed, or
whose deadline came with nothing settled. A hold output with a settlement
recorded is never reclaimed (a hold output settles or is reclaimed, never
both); one the chain shows spent by something else is noted and left.
-unused also reclaims the hold outputs minted ahead that no put took.
Each spend is recorded before it is sent.`

func cmdReclaim(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("reclaim", reclaimHelp)
	entry := fs.String("entry", "", "one race, by its entry's txid")
	unused := fs.Bool("unused", false, "also reclaim hold outputs no put took")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("reclaim takes flags only")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	races, err := h.races(*entry)
	if err != nil {
		return err
	}
	var left []string
	for _, r := range races {
		switch r.State {
		case publish.RaceOpen, publish.RaceClosed, publish.RaceLapsed, publish.RaceReclaim:
		default:
			continue
		}
		rc, err := h.pub.Reclaim(ctx, r)
		switch {
		case err == nil && rc != nil:
			g.out("race %s: reclaimed by %s", r.Entry, rc.Txid)
		case err == nil:
			g.out("race %s: %s", r.Entry, r.Note)
		case errors.Is(err, publish.ErrBeforeDeadline):
			if *entry != "" {
				return usage("race %s: %v", short(r.Entry), err)
			}
		case errors.Is(err, publish.ErrNotYet):
			left = append(left, fmt.Sprintf("race %s: %v", short(r.Entry), err))
		default:
			if *entry != "" {
				return err
			}
			left = append(left, fmt.Sprintf("race %s: %v", short(r.Entry), err))
		}
	}
	if *unused {
		done, err := h.pub.ReclaimUnused(ctx)
		for _, t := range done {
			g.out("unused hold output reclaimed by %s", t)
		}
		if err != nil {
			left = append(left, err.Error())
		}
	}
	for _, l := range left {
		g.say("%s", l)
	}
	if len(left) > 0 {
		return &exitError{code: exitIncomplete}
	}
	return nil
}

const challengeHelp = `usage: bstore challenge KEY -provider KEY [-c N] [-budget SATS] [flags]

A possession challenge (spec section 7) of the provider's advertisement of
KEY's object: c samples (default 30) of 4 KiB segments chosen from the tip
block's hash, a fresh nonce and the provider's key, posted to the
provider's front over BRC-104 and paid, through the provider's 402, up to
-budget satoshis (a price over the budget is refused, exit 2). The signed
response is checked sample-major; a by-reference sample is fetched from the
advertised URL and rechecked from the whole blob. The outcome is recorded
in the home (outcomes.jsonl) and published as an outcome record on the
bucket's records topic unless -no-publish.

Exit 0 a pass; 1 invalid (a signed response that fails, provable by anyone
who holds it); 3 no valid signed answer, or a sample the advertised URL did
not serve. c = 113 catches one dropped blob of a 100 MiB object 99 times in
100; -nonce fixes the nonce (hex, 16 bytes), for a reproducible sample.`

// outcomeLog is one challenge a home made.
type outcomeLog struct {
	At        int64  `json:"at"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	Root      string `json:"root"`
	Provider  string `json:"provider"`
	Answerer  string `json:"answerer"`
	Front     string `json:"front"`
	C         uint64 `json:"c"`
	Outcome   string `json:"outcome"`
	Why       string `json:"why,omitempty"`
	Paid      uint64 `json:"paid,omitempty"`
	Request   string `json:"request"`
	Published string `json:"published,omitempty"`
}

func cmdChallenge(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("challenge", challengeHelp)
	provider := fs.String("provider", "", "the advertiser's identity key")
	c := fs.Uint64("c", 30, "samples, 1 to 1024")
	budget := fs.Uint64("budget", 0, "the most paid for the answer")
	nonceHex := fs.String("nonce", "", "the nonce, 32 hex digits (default fresh)")
	front := fs.String("front", "", "the answerer's front URL (default derived from the advertisement)")
	answerer := fs.String("answerer", "", "the front's key when -front is not the provider's own")
	noPub := fs.Bool("no-publish", false, "record the outcome here only")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("challenge KEY -provider KEY")
	}
	if *c < 1 || *c > storerec.MaxC {
		return usage("-c is 1 to 1024")
	}
	prov, err := hex.DecodeString(strings.ToLower(*provider))
	if err == nil {
		_, err = storerec.CheckKey(prov)
	}
	if err != nil {
		return usage("-provider is the advertiser's identity key, 66 hex digits")
	}
	var nonce *[16]byte
	if *nonceHex != "" {
		b, err := hex.DecodeString(*nonceHex)
		if err != nil || len(b) != 16 {
			return usage("-nonce is 32 hex digits")
		}
		nonce = (*[16]byte)(b)
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	ch, err := h.chain()
	if err != nil {
		return err
	}
	f, err := h.rd.Object(ctx, ch, pos[0])
	if err != nil {
		return err
	}
	if f.Result == storerec.ObjectAbsent {
		return refused("NoSuchKey: %s is absent from %s", pos[0], ch.Bucket)
	}
	e := f.Entry.Entry
	if e.Size == 0 {
		return usage("an empty object has nothing to challenge")
	}
	adURL := ""
	for _, s := range h.rd.Sources(ctx, ch.Bucket, e.Root) {
		if s.Advertiser == hex.EncodeToString(prov) {
			adURL = s.URL
			break
		}
	}
	if adURL == "" {
		return incomplete("no host answers a live advertisement of %s by %s", hex.EncodeToString(e.Root[:]), short(*provider))
	}
	answer := prov
	fr := *front
	if fr == "" {
		var ok bool
		if fr, ok = storerec.DerivedFront(adURL); !ok {
			return incomplete("the advertisement's URL derives no front: name one with -front")
		}
	} else if *answerer != "" {
		if answer, err = hex.DecodeString(strings.ToLower(*answerer)); err != nil {
			return usage("-answerer is an identity key")
		}
	}
	var blobs []chirp.Child
	obj, err := h.rd.Open(ctx, e, h.store)
	if err != nil {
		return err
	}
	blobs = obj.Blobs()
	res, err := h.pub.Challenge(ctx, &publish.Challenge{Bucket: ch.Bucket, Root: e.Root, ProofRoot: e.ProofRoot, Size: e.Size,
		Entry: f.Entry.Txid(), Provider: prov, Answerer: answer, Front: fr, URL: adURL, Blobs: blobs, C: *c, Nonce: nonce, Budget: *budget}, h.fetch)
	if err != nil {
		return err
	}
	name := publish.OutcomeName(res.Outcome)
	l := outcomeLog{At: time.Now().Unix(), Bucket: ch.Bucket, Key: e.Key, Root: hex.EncodeToString(e.Root[:]), Provider: hex.EncodeToString(prov),
		Answerer: hex.EncodeToString(answer), Front: fr, C: *c, Outcome: name, Why: res.Why, Paid: res.Paid, Request: hex.EncodeToString(res.Request)}
	if !*noPub {
		if rec := res.OutcomeRecord(ch.Bucket); rec != nil {
			txid, err := h.pub.PublishOutcome(ctx, rec)
			if err != nil {
				g.say("the outcome record was not published: %v", err)
			}
			l.Published = txid
		}
	}
	if err := appendOutcome(g.home, l); err != nil {
		return err
	}
	g.out("challenge of %s at %s: %s (c = %d, paid %d)", short(l.Provider), fr, name, *c, res.Paid)
	if res.Why != "" {
		g.out("  %s", res.Why)
	}
	if l.Published != "" {
		g.out("  outcome record %s", l.Published)
	}
	switch res.Outcome {
	case storerec.OutcomePass:
		return nil
	case storerec.OutcomeInvalid:
		return &exitError{code: exitRefused, msg: "the provider's signed response is invalid"}
	}
	return &exitError{code: exitIncomplete, msg: "no valid signed answer: " + name}
}

func appendOutcome(home string, l outcomeLog) error {
	raw, _ := json.Marshal(l)
	f, err := os.OpenFile(filepath.Join(home, "outcomes.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
