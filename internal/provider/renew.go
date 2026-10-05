package provider

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"

	"github.com/lightwebinc/bstore/internal/auth104"
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
	price := renewPrice(size, max(now, expiry), req.Until, p.cfg.Terms.RenewPerGiBDay, p.cfg.Terms.RenewMinimum)
	var pay *paymentLine
	if price > 0 {
		if who == nil {
			return auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", "a renewal is priced; ask over BRC-104 authentication")
		}
		raw := r.Header.Get("x-bsv-payment")
		if raw == "" {
			_, rep := p.requirePayment(ctx, r, who, price, "renew")
			return *rep
		}
		pl, rep := p.checkPayment(ctx, raw, who, price)
		if rep != nil {
			return *rep
		}
		pay = pl
	}
	if ok, retry := p.limits.roots.Take(req.Root); !ok {
		return auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "this root was renewed often lately; try again later", "Retry-After", itoa(uint64(retry)))
	}
	if pay != nil {
		if _, rep := p.claimPayment(*pay, "renew"); rep != nil {
			return *rep
		}
	}
	ad, err := p.be.Advertise(ctx, [32]byte(root), req.Until)
	if err != nil {
		p.logf("root %s: renewal through %d paid and not made: %v", short(req.Root), req.Until, err)
		if errors.Is(err, errAdPending) {
			return auth104.Error(http.StatusConflict, "ERR_PENDING", "an advertisement for this root is not mined yet; the payment is kept and the renewal is made once it mines", "Retry-After", "60")
		}
		return auth104.Error(http.StatusServiceUnavailable, "ERR_RENEWAL", "the renewal could not be made now; the payment is kept")
	}
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
