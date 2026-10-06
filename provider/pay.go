package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/bsv-blockchain/go-sdk/auth/utils"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/acceptance"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/payee"

	"github.com/lightwebinc/bstore/auth104"
)

// BRC-105 on a priced route: without a payment the route answers 402 with
// the version, the price and a derivation prefix this provider issued (an
// HMAC nonce, so a prefix it did not issue is known without a record); with
// one, the payment must be that prefix's, its output 0 must pay at least
// the price to the key BRC-29 derives for the prefix, the payer's suffix
// and the payer, it must verify against the provider's headers, and its
// txid and every coin it spends must be new to the ledger. One payment
// buys one answer, and it is in the ledger, synced, before the answer is
// signed. The payee settle run takes it into the pool later.

const paymentVersion = "1.0"

// maxPaymentBEEF bounds a payment's BEEF.
const maxPaymentBEEF = 192 << 10

// paid is a payment taken for one answer.
type paid struct {
	txid string
	sats uint64
}

// requirePayment answers the 402, or checks and takes the payment the
// request carries. reply is set when the route must answer with it.
func (p *Provider) requirePayment(ctx context.Context, r *http.Request, who *auth104.Caller, price uint64, class string) (*paid, *auth104.Reply) {
	if who == nil {
		rep := auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", class+" is priced; ask over BRC-104 authentication")
		return nil, &rep
	}
	raw := r.Header.Get("x-bsv-payment")
	if raw == "" {
		prefix, err := utils.CreateNonce(ctx, p.wallet, wallet.Counterparty{Type: wallet.CounterpartyTypeSelf})
		if err != nil {
			rep := auth104.Error(http.StatusInternalServerError, "ERR_INTERNAL", "no derivation prefix")
			return nil, &rep
		}
		p.metrics.inc("bstore_provider_payments_total", "decision=requested", "reason=no-payment")
		rep := auth104.JSON(http.StatusPaymentRequired, map[string]any{"status": "error", "code": "ERR_PAYMENT_REQUIRED",
			"satoshisRequired": price, "description": fmt.Sprintf("%s costs %d satoshis", class, price)},
			"x-bsv-payment-version", paymentVersion, "x-bsv-payment-satoshis-required", strconv.FormatUint(price, 10),
			"x-bsv-payment-derivation-prefix", prefix)
		return nil, &rep
	}
	return p.takeAccepted(ctx, raw, who, price, class)
}

// checkPayment checks a payment header without taking it: what a route
// that must do work before it takes the payment runs first.
func (p *Provider) checkPayment(ctx context.Context, raw string, who *auth104.Caller, price uint64) (*payee.Payment, *auth104.Reply) {
	bad := func(status int, code, d string) (*payee.Payment, *auth104.Reply) {
		rep := auth104.Error(status, code, d)
		return nil, &rep
	}
	var pm struct {
		DerivationPrefix string `json:"derivationPrefix"`
		DerivationSuffix string `json:"derivationSuffix"`
		Transaction      string `json:"transaction"`
	}
	if err := json.Unmarshal([]byte(raw), &pm); err != nil || pm.DerivationPrefix == "" || pm.DerivationSuffix == "" || pm.Transaction == "" {
		return bad(http.StatusBadRequest, "ERR_MALFORMED_PAYMENT", "the payment header is not {derivationPrefix, derivationSuffix, transaction}")
	}
	if ok, err := utils.VerifyNonce(ctx, pm.DerivationPrefix, p.wallet, wallet.Counterparty{Type: wallet.CounterpartyTypeSelf}); err != nil || !ok {
		return bad(http.StatusBadRequest, "ERR_INVALID_DERIVATION_PREFIX", "this provider did not issue that derivation prefix")
	}
	beef, err := base64.StdEncoding.Strict().DecodeString(pm.Transaction)
	if err != nil || len(beef) > maxPaymentBEEF {
		return bad(http.StatusBadRequest, "ERR_MALFORMED_PAYMENT", "the payment transaction is not standard base64 of at most 192 KiB")
	}
	// A payment taken before is refused before any proof is checked.
	if _, _, txid, err := guard.ParseBEEF(beef, maxPaymentBEEF); err != nil || txid == nil {
		return bad(http.StatusBadRequest, "ERR_INVALID_PAYMENT", "the payment is not a BEEF with its subject")
	} else if p.ledger.has(txid.String()) && !p.isHeld(txid.String()) {
		return bad(http.StatusConflict, "ERR_PAYMENT_REPLAYED", "this payment was already used")
	}
	in, err := p.incoming(ctx, beef, 0, pm.DerivationPrefix, pm.DerivationSuffix, who.Hex)
	if err != nil {
		return bad(http.StatusBadRequest, "ERR_INVALID_PAYMENT", "output 0 does not pay the key derived for this prefix, suffix and payer, or the payment does not verify against this provider's headers")
	}
	sats := in.Tx.Outputs[0].Satoshis
	if sats < price {
		return bad(http.StatusBadRequest, "ERR_INVALID_PAYMENT", fmt.Sprintf("output 0 holds %d satoshis, not %d", sats, price))
	}
	pl := payment(in, beef, 0, sats, pm.DerivationPrefix, pm.DerivationSuffix, who.Hex, "")
	// A payment spending a coin a taken payment spent is refused here,
	// before any budget is spent on it.
	if c := p.ledger.refusal(pl.Txid, pl.Inputs); c != payee.Accepted && !p.isHeld(pl.Txid) {
		return bad(http.StatusConflict, "ERR_PAYMENT_CONFLICT", "this payment, or a coin it spends, was already used")
	}
	return &pl, nil
}

// takeAccepted checks and takes a payment, held to acceptance first: a
// route that takes a payment at once (no work before it) answers only on
// Fast.
func (p *Provider) takeAccepted(ctx context.Context, raw string, who *auth104.Caller, price uint64, class string) (*paid, *auth104.Reply) {
	pl, rep := p.checkPayment(ctx, raw, who, price)
	if rep != nil {
		p.metrics.inc("bstore_provider_payments_total", "decision=refuse", "reason=malformed")
		return nil, rep
	}
	v, tx, err := p.acceptPayment(ctx, pl, who.Hex)
	if err != nil {
		rep := auth104.Error(http.StatusServiceUnavailable, "ERR_BUSY", "the payment could not be checked now; try again later")
		return nil, &rep
	}
	switch v.Decision {
	case acceptance.Refuse:
		rep := auth104.Error(http.StatusBadRequest, "ERR_PAYMENT_REFUSED", "the payment is refused: "+v.String())
		return nil, &rep
	case acceptance.Hold:
		rep := heldReply(pl.Txid, v)
		return nil, &rep
	}
	pd, rep := p.claimPayment(*pl, class)
	if rep != nil {
		p.forgetFast(pl.Txid)
		return nil, rep
	}
	p.watchFast(v, tx, pl, who.Hex)
	return pd, nil
}

// claimPayment writes a checked payment to the ledger. A payment this
// provider signed a not-taken note for is never taken.
func (p *Provider) claimPayment(pl payee.Payment, class string) (*paid, *auth104.Reply) {
	pl.Class = class
	p.noteMu.Lock()
	if _, noted := p.notTaken[pl.Txid]; noted {
		p.noteMu.Unlock()
		rep := auth104.Error(http.StatusConflict, "ERR_PAYMENT_REFUSED", "this payment was refused and is not taken")
		return nil, &rep
	}
	c, err := p.ledger.take(pl)
	p.noteMu.Unlock()
	if err != nil {
		rep := auth104.Error(http.StatusInternalServerError, "ERR_INTERNAL", "the payment could not be recorded")
		return nil, &rep
	}
	switch c {
	case payee.Replayed:
		rep := auth104.Error(http.StatusConflict, "ERR_PAYMENT_REPLAYED", "this payment was already used")
		return nil, &rep
	case payee.Conflict:
		rep := auth104.Error(http.StatusConflict, "ERR_PAYMENT_CONFLICT", "this payment spends a coin an earlier payment spent")
		return nil, &rep
	}
	p.logf("payment %s accepted: %d satoshis for %s", short(pl.Txid), pl.Satoshis, class)
	return &paid{txid: pl.Txid, sats: pl.Satoshis}, nil
}

// paymentLine is a ledger line.
type paymentLine = payee.Payment
