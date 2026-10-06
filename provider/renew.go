package provider

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"slices"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bstore/auth104"
)

// renew is <front>/renew (spec sections 10.1 and 10.3): anyone may buy
// more time for a root this provider holds. The renewal is priced
// (BRC-105) by size and days, never below the minimum; once paid, the
// lease is extended through until before anything is broadcast, the live
// advertisement is spent into its successor (the same key and root, the
// later expiry), and the answer is the successor's BEEF: with its proof
// when it mined at once, else unmined, a promise until it does.
//
// Griefing is bounded: a renewal must move the expiry later and no
// further than max_renew_seconds ahead, each one pays at least the
// minimum (which covers its fee), one root is renewed at most
// renewals_per_root_per_hour, and a root with an advertisement still
// pending is not renewed until it mines.
func (p *Provider) renew(ctx context.Context, r *http.Request, body []byte, who *auth104.Caller) auth104.Reply {
	var req struct {
		Root  string `json:"root"`
		Until uint64 `json:"until"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Root) != 64 {
		return auth104.Error(http.StatusBadRequest, "ERR_BAD_REQUEST", "the body is {root: 64 hex digits, until: Unix seconds}")
	}
	root, err := hex.DecodeString(req.Root)
	if err != nil || hex.EncodeToString(root) != req.Root {
		return auth104.Error(http.StatusBadRequest, "ERR_BAD_REQUEST", "root is 64 lowercase hex digits")
	}
	now := uint64(p.now().Unix())
	p.mu.Lock()
	h := p.st.Holdings[req.Root]
	var size, expiry uint64
	ok := h != nil && h.Verified && !h.Damaged && h.Ad != nil
	pending := false
	if ok {
		size, expiry = h.Size, h.Ad.Expiry
		for _, pa := range p.st.PendingAds {
			pending = pending || pa.Root == req.Root
		}
	}
	p.mu.Unlock()
	switch {
	case !ok:
		return auth104.Error(http.StatusNotFound, "ERR_NOT_HELD", "this provider holds no advertised root of that hash")
	case pending:
		return auth104.Error(http.StatusConflict, "ERR_PENDING", "an advertisement for this root is not mined yet; try again later", "Retry-After", "60")
	case req.Until <= expiry:
		return auth104.Error(http.StatusBadRequest, "ERR_UNTIL", "until must be later than the advertisement's expiry")
	case req.Until > now+p.cfg.Terms.MaxRenewSeconds:
		return auth104.Error(http.StatusBadRequest, "ERR_UNTIL", "until is further ahead than this provider renews")
	}
	if who == nil {
		return auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", "a renewal is priced; ask over BRC-104 authentication")
	}
	price := renewPrice(size, max(now, expiry), req.Until, p.cfg.Terms.RenewPerGiBDay, p.cfg.Terms.RenewMinimum)
	raw := r.Header.Get("x-bsv-payment")
	if raw == "" {
		_, rep := p.requirePayment(ctx, r, who, price, "renew")
		return *rep
	}
	// A payment taken before is refused before the root's budget is
	// spent, so an old payment cannot lock a root's renewals.
	pl, rep := p.checkPayment(ctx, raw, who, price)
	if rep != nil {
		return *rep
	}
	if ok, retry := p.limits.roots.Take(req.Root); !ok {
		return auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "this root was renewed often lately; try again later", "Retry-After", itoa(uint64(retry)))
	}
	// The payment reaches the network before the renewal is made: a payer
	// cannot take its coin back once the renewal costs the provider a fee.
	tx, err := paymentTx(pl)
	if err != nil {
		return auth104.Error(http.StatusBadRequest, "ERR_INVALID_PAYMENT", "the payment does not read")
	}
	if err := p.send(ctx, tx); err != nil {
		p.logf("renewal payment %s: %v", short(pl.Txid), err)
		return auth104.Error(http.StatusBadRequest, "ERR_PAYMENT_REFUSED", "the network did not take the payment; no renewal is made")
	}
	if _, rep := p.claimPayment(*pl, "renew"); rep != nil {
		return *rep
	}
	// The renewal is owed from here: recorded before it is made, and made
	// by the background when it cannot be now.
	p.mu.Lock()
	p.st.Owed = append(p.st.Owed, Owed{Root: req.Root, Until: req.Until, Payment: pl.Txid})
	err = p.save()
	p.mu.Unlock()
	if err != nil {
		return auth104.Error(http.StatusInternalServerError, "ERR_INTERNAL", "the renewal could not be recorded")
	}
	ad, err := p.advertiseVia(ctx, [32]byte(root), req.Until)
	if err != nil {
		p.logf("root %s: renewal through %d paid, made later: %v", short(req.Root), req.Until, err)
		return auth104.JSON(http.StatusAccepted, map[string]any{"pending": true, "until": req.Until,
			"description": "paid; the renewal is owed and made as soon as it can be, and the root's advertisement then answers it"})
	}
	p.payOwed(req.Root, req.Until)
	p.metrics.inc("bstore_provider_renewals_total")
	return auth104.JSON(http.StatusOK, map[string]any{"advertisement": base64.StdEncoding.EncodeToString(ad.BEEF), "txid": ad.Txid, "expiryTime": ad.Expiry})
}

// renewPrice is perGiBDay for each GiB a day from from to until, rounded
// up, and never below minimum.
func renewPrice(size, from, until, perGiBDay, minimum uint64) uint64 {
	if until <= from {
		return minimum
	}
	n := new(big.Int).SetUint64(perGiBDay)
	n.Mul(n, new(big.Int).SetUint64(size))
	n.Mul(n, new(big.Int).SetUint64(until-from))
	d := new(big.Int).SetUint64((1 << 30) * 86400)
	n.Add(n, new(big.Int).Sub(d, big.NewInt(1)))
	n.Div(n, d)
	if !n.IsUint64() || n.Uint64() > 1<<53-1 {
		return 1<<53 - 1
	}
	return max(minimum, n.Uint64())
}

// paymentTx is a ledger payment's transaction, with its ancestry.
func paymentTx(pl *paymentLine) (*transaction.Transaction, error) {
	raw, err := base64.StdEncoding.DecodeString(pl.Beef)
	if err != nil {
		return nil, err
	}
	return transaction.NewTransactionFromBEEF(raw)
}

// payOwed drops the owed renewals an advertisement through until settles.
func (p *Provider) payOwed(root string, until uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.Owed = slices.DeleteFunc(p.st.Owed, func(o Owed) bool { return o.Root == root && o.Until <= until })
	_ = p.save()
}

// makeOwed makes the renewals paid for and not made yet.
func (p *Provider) makeOwed(ctx context.Context) {
	p.mu.Lock()
	owed := slices.Clone(p.st.Owed)
	p.mu.Unlock()
	for _, o := range owed {
		p.mu.Lock()
		h := p.st.Holdings[o.Root]
		gone := h == nil || h.Damaged || !h.Verified
		p.mu.Unlock()
		if gone {
			p.logf("root %s: a renewal through %d was paid (%s) and cannot be made: the root is no longer held", short(o.Root), o.Until, short(o.Payment))
			p.payOwed(o.Root, o.Until)
			continue
		}
		if _, err := p.advertiseVia(ctx, mustHash(o.Root), o.Until); err != nil {
			continue
		}
		p.payOwed(o.Root, o.Until)
	}
}
