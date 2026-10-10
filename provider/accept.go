package provider

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/acceptance"
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/auth104"
)

// Payment acceptance (bcommon acceptance, docs/provider.md): every payment
// the provider is offered, for a challenge, a renewal or a remittance, is
// checked (its outputs, finality, no more out than in, SPV), broadcast by
// the provider, and decided on value. At or below the threshold, within
// the payer's and the total limits, and with arcade's word that the
// network took it and the node's spend view clean, it is fast: taken and
// answered at once, and watched until it mines. Above the threshold, or
// without that evidence, it is held: taken once its proof verifies against
// the provider's headers. A refusal is a refusal. Without an arcade
// settlement leg nothing is fast.

// acceptMaxAge is how long a fast payment may stay unmined before the
// watch reports it.
const acceptMaxAge = 24 * time.Hour

// setupAcceptance builds the verifier and its watch over the legs.
func (p *Provider) setupAcceptance() {
	pol := p.cfg.AcceptPolicy()
	v := &acceptance.Verifier{Policy: pol, Exposure: acceptance.NewExposure(), Headers: p.hc, Settler: p.settler, Spends: p.chain, Proofs: p.chain}
	if p.arcade != nil {
		v.Status = []acceptance.StatusSource{p.arcade}
	} else {
		// No verdict can come: decide at once rather than wait for one.
		v.Policy.Wait = time.Millisecond
	}
	if p.opts.Accept != nil {
		p.opts.Accept(v)
	}
	p.acc = v
	p.mon = &acceptance.Monitor{Verifier: v, MaxAge: acceptMaxAge, Hook: p.acceptEvent}
	for _, dr := range [][2]string{{"requested", "no-payment"}, {"fast", "at-or-below-threshold"}, {"fast", "mined"}, {"hold", "above-threshold"},
		{"hold", "payer-limit"}, {"hold", "no-network-verdict"}, {"refuse", "network-refused"}, {"refuse", "double-spent"}} {
		p.metrics.preset("bstore_provider_payments_total", "decision="+dr[0], "reason="+dr[1])
	}
	for _, k := range []string{"confirmed", "double-spent", "refused", "unmined"} {
		p.metrics.preset("bstore_provider_payment_events_total", "kind="+k)
	}
}

// acceptPayment decides a checked payment line for payer. A fast payment
// not mined is watched until it mines.
func (p *Provider) acceptPayment(ctx context.Context, pl *paymentLine, payer string) (acceptance.Verdict, *transaction.Transaction, error) {
	tx, err := paymentTx(pl)
	if err != nil || int(pl.OutputIndex) >= len(tx.Outputs) || tx.Outputs[pl.OutputIndex].LockingScript == nil {
		v := acceptance.Verdict{Decision: acceptance.Refuse, Reason: acceptance.ReasonMalformed}
		p.countPayment(v)
		return v, nil, nil
	}
	out := tx.Outputs[pl.OutputIndex]
	v, err := p.acc.Accept(ctx, acceptance.Payment{Tx: tx, Payer: payer,
		Pays: []acceptance.Output{{Vout: pl.OutputIndex, Script: *out.LockingScript, Sats: out.Satoshis}}})
	if err != nil {
		return v, tx, err
	}
	p.countPayment(v)
	p.logf("payment %s: %s", short(pl.Txid), v)
	return v, tx, nil
}

// watchFast watches a fast payment, once it is taken, until it mines.
func (p *Provider) watchFast(v acceptance.Verdict, tx *transaction.Transaction, pl *paymentLine, payer string) {
	if v.Decision == acceptance.Fast && v.Reason != acceptance.ReasonMined && tx != nil {
		p.mon.Watch(tx, payer, pl.Satoshis)
	}
}

// noteable is a refusal the provider's payment can never come back from:
// refused before it was broadcast, or refused by the network itself. A
// spend view that shows an input spent elsewhere after the provider's own
// broadcast is not one: the provider's transaction may still win, so no
// payment-not-taken note is signed for it.
func noteable(v acceptance.Verdict) bool {
	switch v.Reason {
	case acceptance.ReasonDoubleSpent, acceptance.ReasonSpendUnknown, acceptance.ReasonConflict:
		return false
	}
	return true
}

// countPayment counts a decision as bstore_provider_payments_total.
func (p *Provider) countPayment(v acceptance.Verdict) {
	p.metrics.inc("bstore_provider_payments_total", "decision="+v.Decision.String(), "reason="+string(v.Reason))
}

// forgetFast releases a fast payment the provider did not take after all.
func (p *Provider) forgetFast(txid string) {
	p.acc.Exposure.Release(txid)
}

// minedNow reports whether the node serves a proof of txid that verifies
// against the provider's headers.
func (p *Provider) minedNow(ctx context.Context, txid string) bool {
	mp, _, err := p.chain.Proof(ctx, txid)
	if err != nil || mp == nil {
		return false
	}
	h, err := chainHash(txid)
	if err != nil {
		return false
	}
	ok, err := mp.Verify(ctx, h, p.hc)
	return err == nil && ok
}

// spentAway names the transaction that spent one of tx's inputs instead
// of tx, or "" when the node shows none (an input it cannot answer for is
// passed over).
func (p *Provider) spentAway(ctx context.Context, tx *transaction.Transaction) string {
	err := p.chain.SpentElsewhere(ctx, tx)
	var se *nodeapi.SpentError
	if errors.As(err, &se) {
		return se.By
	}
	return ""
}

// acceptEvent is the watch's report on a fast payment: counted, logged,
// and, when it will never settle, recorded unsettleable.
func (p *Provider) acceptEvent(e acceptance.Event) {
	if e.Kind == acceptance.DoubleSpent && p.reencoded(e.Txid) {
		// A miner's re-encoding of the same payment: the same output to
		// this provider, under another txid. Not a loss, and not the payer's.
		p.acc.Exposure.Unflag(e.Payer)
		p.logf("fast payment %s was mined re-encoded; it still pays this provider", short(e.Txid))
		e.Kind = acceptance.Confirmed
	}
	p.metrics.inc("bstore_provider_payment_events_total", "kind="+e.Kind.String())
	if e.Kind == acceptance.Confirmed {
		return
	}
	p.logf("fast payment %s (%d sat from %s) %s: %s; its payer is held for confirmation from now on", short(e.Txid), e.Sats, short(e.Payer), e.Kind, e.Why)
	if e.Kind == acceptance.DoubleSpent || e.Kind == acceptance.Refused {
		_ = lockedBook{p}.RecordUnsettleable(e.Txid, e.Kind.String()+": "+e.Why)
	}
}

// reencoded reports whether the transaction that spent txid's inputs pays
// this provider's output of txid, same script and value.
func (p *Provider) reencoded(txid string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pl := p.ledger.find(txid)
	if pl == nil {
		return false
	}
	tx, err := paymentTx(pl)
	if err != nil || int(pl.OutputIndex) >= len(tx.Outputs) {
		return false
	}
	by := p.spentAway(ctx, tx)
	if by == "" {
		return false
	}
	raw, err := p.chain.TxRaw(ctx, by)
	if err != nil {
		return false
	}
	other, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return false
	}
	want := tx.Outputs[pl.OutputIndex]
	for _, o := range other.Outputs {
		if o.Satoshis == want.Satoshis && o.LockingScript != nil && want.LockingScript != nil && o.LockingScript.Equals(want.LockingScript) {
			return true
		}
	}
	return false
}

// heldReply is the answer to a payment held for confirmation: 202 with the
// code ERR_PAYMENT_HELD, as a held renewal is answered. Not a 402: a BRC-105
// client (go-sdk's AuthFetch, the TS AuthFetch) treats every 402 as a price
// to pay, so a 402 without the BRC-105 headers fails inside the client and
// never reaches the caller, and one with them would be paid again.
func heldReply(txid string, v acceptance.Verdict) auth104.Reply {
	return auth104.JSON(http.StatusAccepted, map[string]any{"status": "error", "code": "ERR_PAYMENT_HELD", "txid": txid,
		"reason": string(v.Reason), "description": fmt.Sprintf("the payment is broadcast and held for confirmation (%s); send it again once it is mined, with a request on a fresh seed for a challenge", v.Reason)})
}

// heldFor is how long a held challenge payment waits to be sent again
// once taken.
const heldFor = 30 * 24 * time.Hour

// isHeld reports a challenge payment taken and held for confirmation.
func (p *Provider) isHeld(txid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.st.Held[txid]
	return ok
}

// hold records a payment taken and held for confirmation.
func (p *Provider) hold(txid, class string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.st.Held == nil {
		p.st.Held = map[string]HeldRec{}
	}
	p.st.Held[txid] = HeldRec{Class: class, At: p.now().Unix()}
	return p.save()
}

// takeHeld ends a hold, once: the payment mined and buys its answer. Two
// requests sending it again at once get one answer between them.
func (p *Provider) takeHeld(txid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.st.Held[txid]; !ok {
		return false
	}
	delete(p.st.Held, txid)
	_ = p.save()
	return true
}

// confirmHeld runs on the background tick: the watch over fast payments,
// held renewals made owed once their payment mines (dropped when it never
// can), and holds forgotten once settled or unsettleable.
func (p *Provider) confirmHeld(ctx context.Context) {
	p.reflag.Do(func() {
		// A payer whose payment was lost stays held across a restart.
		p.mu.Lock()
		lost := slices.Clone(p.st.Unsettleable)
		good := maps.Clone(p.st.MadeGood)
		p.mu.Unlock()
		for _, u := range lost {
			if _, ok := good[u.Txid]; ok {
				continue
			}
			if pl := p.ledger.find(u.Txid); pl != nil && pl.SenderIdentityKey != "" {
				p.acc.Exposure.Flag(pl.SenderIdentityKey, "unsettleable: "+u.Txid)
			}
		}
	})
	p.mon.Sweep(ctx)
	p.mu.Lock()
	owed := slices.Clone(p.st.Owed)
	// A hold stands until its payment buys its answer, never mines, or is
	// not sent again within heldFor; settling it into the pool does not end
	// it.
	for txid, h := range p.st.Held {
		if p.st.IsUnsettleable(txid) || p.now().Sub(time.Unix(h.At, 0)) > heldFor {
			delete(p.st.Held, txid)
			p.dirty = true
		}
	}
	p.mu.Unlock()
	for _, o := range owed {
		if !o.Held || ctx.Err() != nil {
			continue
		}
		if p.minedNow(ctx, o.Payment) {
			p.setOwedHeld(o, false)
			p.logf("root %s: the renewal payment %s mined; the renewal through %d is made now", short(o.Root), short(o.Payment), o.Until)
			continue
		}
		pl := p.ledger.find(o.Payment)
		if pl == nil {
			p.logf("root %s: the renewal payment %s is not in the ledger; the held renewal through %d is dropped", short(o.Root), short(o.Payment), o.Until)
			p.payOwed(o.Root, o.Until)
			continue
		}
		tx, err := paymentTx(pl)
		if err != nil {
			continue
		}
		if by := p.spentAway(ctx, tx); by != "" {
			p.logf("root %s: the renewal payment %s was double-spent by %s; the renewal through %d is not made", short(o.Root), short(o.Payment), short(by), o.Until)
			_ = lockedBook{p}.RecordUnsettleable(o.Payment, "double-spent by "+by+" before the renewal was made")
			p.payOwed(o.Root, o.Until)
		}
	}
}

// setOwedHeld sets the hold on one owed renewal.
func (p *Provider) setOwedHeld(o Owed, held bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Owed {
		if p.st.Owed[i] == o {
			p.st.Owed[i].Held = held
		}
	}
	_ = p.save()
}

// chainHash is a display-order txid as a hash.
func chainHash(txid string) (*chainhash.Hash, error) { return chainhash.NewHashFromHex(txid) }
