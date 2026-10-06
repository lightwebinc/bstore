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
		p.metrics.inc("bstore_provider_payments_total", "result=requested")
		rep := auth104.JSON(http.StatusPaymentRequired, map[string]any{"status": "error", "code": "ERR_PAYMENT_REQUIRED",
			"satoshisRequired": price, "description": fmt.Sprintf("%s costs %d satoshis", class, price)},
			"x-bsv-payment-version", paymentVersion, "x-bsv-payment-satoshis-required", strconv.FormatUint(price, 10),
			"x-bsv-payment-derivation-prefix", prefix)
		return nil, &rep
	}
	pd, rep := p.takePayment(ctx, raw, who, price, class)
	if rep != nil {
		p.metrics.inc("bstore_provider_payments_total", "result=refused")
		return nil, rep
	}
	p.metrics.inc("bstore_provider_payments_total", "result=accepted")
	return pd, nil
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
	} else if p.ledger.has(txid.String()) {
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
	if c := p.ledger.refusal(pl.Txid, pl.Inputs); c != payee.Accepted {
		return bad(http.StatusConflict, "ERR_PAYMENT_CONFLICT", "this payment, or a coin it spends, was already used")
	}
	return &pl, nil
}

// takePayment checks and takes a payment.
func (p *Provider) takePayment(ctx context.Context, raw string, who *auth104.Caller, price uint64, class string) (*paid, *auth104.Reply) {
	pl, rep := p.checkPayment(ctx, raw, who, price)
	if rep != nil {
		return nil, rep
	}
	return p.claimPayment(*pl, class)
}

// claimPayment writes a checked payment to the ledger.
func (p *Provider) claimPayment(pl payee.Payment, class string) (*paid, *auth104.Reply) {
	pl.Class = class
	c, err := p.ledger.take(pl)
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
