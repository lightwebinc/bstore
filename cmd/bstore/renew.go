package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chaintoken"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/storerec"
)

const renewHelp = `usage: bstore renew KEY -provider KEY (-until T | -days N) [-budget SATS] [flags]

Buy more time for KEY's object from one of its advertisers (spec section
10.3): anyone may, a reader above all. The provider's live advertisement of
the object's root is found at the hosts; the renewal is posted to the
provider's front (derived from the advertisement's URL, or -front for a
front that answers for it, a bridge in front of an incumbent) over BRC-104
and paid, through the provider's 402, up to -budget satoshis (a price over
it is refused, exit 2). -until is the new expiry in Unix seconds; -days
moves the current expiry that many days later.

The answer is the successor advertisement: the same key and root, the new
expiry, in a transaction that spends the advertisement it renews. It is
checked here, and confirmed when its proof verifies in this home's headers;
until it mines it is a promise. Exit 0 a successor that checks (mined or a
promise, said which); 3 a renewal paid and owed (202), or no live
advertisement; 1 an answer that does not check.`

func cmdRenew(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("renew", renewHelp)
	provider := fs.String("provider", "", "the advertiser's identity key")
	until := fs.Uint64("until", 0, "the new expiry, Unix seconds")
	days := fs.Uint64("days", 0, "days added to the current expiry")
	budget := fs.Uint64("budget", 0, "the most paid for the renewal")
	front := fs.String("front", "", "the front to ask (default derived from the advertisement)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("renew KEY -provider KEY (-until T | -days N)")
	}
	if (*until == 0) == (*days == 0) {
		return usage("one of -until and -days")
	}
	if *days > 3660 {
		return usage("-days is at most 3660")
	}
	prov, err := hex.DecodeString(strings.ToLower(*provider))
	if err == nil {
		_, err = storerec.CheckKey(prov)
	}
	if err != nil {
		return usage("-provider is the advertiser's identity key, 66 hex digits")
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
	var cur struct {
		url, txid string
		expiry    uint64
	}
	for _, s := range h.rd.Sources(ctx, ch.Bucket, e.Root) {
		if s.Advertiser == hex.EncodeToString(prov) {
			cur.url, cur.txid, cur.expiry = s.URL, s.Txid, s.Expiry
			break
		}
	}
	if cur.url == "" {
		return incomplete("no host answers a live advertisement of %s by %s", hex.EncodeToString(e.Root[:]), short(*provider))
	}
	to := *until
	if *days > 0 {
		to = cur.expiry + *days*86400
	}
	if to <= cur.expiry {
		return usage("-until must be later than the advertisement's expiry (%d)", cur.expiry)
	}
	fr := *front
	if fr == "" {
		var ok bool
		if fr, ok = storerec.DerivedFront(cur.url); !ok {
			return incomplete("the advertisement's URL derives no front: name one with -front")
		}
	}
	body, _ := json.Marshal(map[string]any{"root": hex.EncodeToString(e.Root[:]), "until": to})
	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	call, err := h.pub.Post(cctx, strings.TrimRight(fr, "/")+"/renew", body, *budget)
	if errors.Is(err, publish.ErrOverBudget) || errors.Is(err, publish.ErrCannotPay) {
		return err
	}
	if err != nil {
		return incomplete("renewal at %s: no answer: %v", fr, err)
	}
	g.out("renewal of %s at %s: %d (paid %d)", short(hex.EncodeToString(prov)), fr, call.Status, call.Paid)
	g.out("  advertisement %s, expiry %d, until %d", cur.txid, cur.expiry, to)
	switch call.Status {
	case http.StatusOK:
	case http.StatusAccepted:
		return incomplete("paid; the provider owes the renewal and makes it as soon as it can")
	default:
		return incomplete("not renewed: %d %s", call.Status, firstLine(string(call.Body)))
	}
	var ans struct {
		Advertisement string `json:"advertisement"`
	}
	if err := json.Unmarshal(call.Body, &ans); err != nil {
		return refused("the renewal's answer does not read: %v", err)
	}
	beef, err := base64.StdEncoding.DecodeString(ans.Advertisement)
	if err != nil {
		return refused("the renewal's advertisement is not base64")
	}
	tx, vout, err := successor(beef, prov, e.Root, to, cur.txid)
	if err != nil {
		return refused("the renewal's advertisement does not check: %v", err)
	}
	state := "a promise until it mines"
	if chaintoken.Mined(ctx, tx, h.rd.Headers) {
		state = "mined, its proof verified in this home's headers"
	}
	g.out("  successor %s.%d spends %s, expiry %d: %s", tx.TxID().String(), vout, cur.txid, to, state)
	return nil
}

// successor checks a renewal's answer: an output advertising root by
// provider through until, in a transaction that spends prev.
func successor(beef, provider []byte, root [32]byte, until uint64, prev string) (*transaction.Transaction, uint32, error) {
	tx, err := transaction.NewTransactionFromBEEF(beef)
	if err != nil {
		return nil, 0, err
	}
	vout, err := checkSuccessor(tx, provider, root, until, prev)
	return tx, vout, err
}

func checkSuccessor(tx *transaction.Transaction, provider []byte, root [32]byte, until uint64, prev string) (uint32, error) {
	spends := false
	for _, in := range tx.Inputs {
		if in.SourceTXID != nil && in.SourceTXID.String() == prev {
			spends = true
		}
	}
	if !spends {
		return 0, fmt.Errorf("it does not spend the advertisement %s", prev)
	}
	for i, o := range tx.Outputs {
		if o == nil || o.LockingScript == nil {
			continue
		}
		ad, err := storerec.DecodeAdvertisement(*o.LockingScript)
		if err != nil || ad.Hash != root || string(ad.Host) != string(provider) {
			continue
		}
		if ad.Expiry != until {
			return 0, fmt.Errorf("its expiry is %d, not %d", ad.Expiry, until)
		}
		return uint32(i), nil //nolint:gosec // an output index
	}
	return 0, errors.New("no output advertises the root by the provider")
}
