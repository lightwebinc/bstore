package provider

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/payee"

	"github.com/lightwebinc/bcommon/acceptance"

	"github.com/lightwebinc/bstore/auth104"
	"github.com/lightwebinc/bstore/storerec"
)

// settle is <front>/settle (spec sections 9.5 and 10.1): a writer's BRC-29
// remittance for one rank of one entry's settlement, unicast, never on the
// plane. It is taken only when everything it says follows from the entry
// this provider holds for: the prefix is base64 of the entry's commitment,
// the suffix base64 of the two-byte rank (1 to the entry's K), the sender
// the entry's writer, the transaction spends the entry's hold output, and
// the named output pays the key BRC-29 derives for this provider with them,
// verifying against the provider's headers. It answers 200 once the
// payment is in the ledger (again 200 for the same payment sent twice) and
// 400 for an output not derivable for this key. The payee settle run takes
// it into the pool once it mines.
func (p *Provider) settle(ctx context.Context, r *http.Request, body []byte, _ *auth104.Caller) auth104.Reply {
	if ok, retry := p.limits.settles.Take(auth104.AddressKey(p.clientAddr(r))); !ok {
		return auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many remittances; try again later", "Retry-After", itoa(uint64(retry)))
	}
	var rm struct {
		Entry             string `json:"entry"`
		Rank              int    `json:"rank"`
		Transaction       string `json:"transaction"`
		OutputIndex       uint32 `json:"outputIndex"`
		DerivationPrefix  string `json:"derivationPrefix"`
		DerivationSuffix  string `json:"derivationSuffix"`
		SenderIdentityKey string `json:"senderIdentityKey"`
	}
	bad := func(d string) auth104.Reply { return auth104.Error(http.StatusBadRequest, "ERR_REMITTANCE", d) }
	if err := json.Unmarshal(body, &rm); err != nil {
		return bad("the body is not a settlement remittance")
	}
	p.mu.Lock()
	e := p.st.Entries[strings.ToLower(rm.Entry)]
	var ec EntryRec
	if e != nil {
		ec = *e
	}
	p.mu.Unlock()
	if e == nil || ec.Hold == nil {
		return bad("this provider holds no entry of that commitment with hold terms")
	}
	c, _ := displayHash(ec.C)
	switch {
	case rm.Rank < 1 || uint64(rm.Rank) > ec.Hold.K:
		return bad("the rank is not 1 to the entry's K")
	case rm.DerivationPrefix != storerec.DerivationPrefix(c):
		return bad("the derivation prefix is not base64 of the entry's commitment")
	case rm.DerivationSuffix != storerec.DerivationSuffix(rm.Rank):
		return bad("the derivation suffix is not base64 of the rank")
	case !strings.EqualFold(rm.SenderIdentityKey, ec.Writer):
		return bad("the sender is not the entry's writer")
	}
	beef, err := base64.StdEncoding.Strict().DecodeString(rm.Transaction)
	if err != nil {
		return bad("the transaction is not standard base64")
	}
	in, err := p.incoming(ctx, beef, rm.OutputIndex, rm.DerivationPrefix, rm.DerivationSuffix, ec.Writer)
	if err != nil {
		return bad("the output is not derivable for this key, or the transaction does not verify: " + err.Error())
	}
	if !spendsHold(in.Tx, ec.Hold) {
		return bad("the transaction does not spend the entry's hold output")
	}
	sats := in.Tx.Outputs[rm.OutputIndex].Satoshis
	if p.ledger.has(in.Txid) {
		return auth104.JSON(http.StatusOK, map[string]any{"accepted": true, "txid": in.Txid, "satoshis": sats})
	}
	pl := payment(in, beef, rm.OutputIndex, sats, rm.DerivationPrefix, rm.DerivationSuffix, ec.Writer, "settle")
	// Held to acceptance on this provider's own output: a remittance of a
	// settlement not yet mined is taken now when its output is at or below
	// the threshold and the network has it; otherwise it is held and taken
	// once mined, when the payout scan finds the hold output's spend.
	v, ptx, err := p.acceptPayment(ctx, &pl, ec.Writer)
	if err != nil {
		return auth104.Error(http.StatusServiceUnavailable, "ERR_BUSY", "the remittance could not be checked now; try again later", "Retry-After", "10")
	}
	switch v.Decision {
	case acceptance.Refuse:
		return bad("the network refused the settlement: " + v.String())
	case acceptance.Hold:
		return auth104.JSON(http.StatusAccepted, map[string]any{"accepted": true, "held": true, "txid": in.Txid, "satoshis": sats, "reason": string(v.Reason),
			"description": "held for confirmation; taken once the settlement mines"})
	}
	if _, rep := p.claimPayment(pl, "settle"); rep != nil {
		p.forgetFast(pl.Txid)
		return *rep
	}
	p.watchFast(v, ptx, &pl, ec.Writer)
	p.notePayout(ec.C, in.Txid, rm.Rank, sats, "remittance")
	return auth104.JSON(http.StatusOK, map[string]any{"accepted": true, "txid": in.Txid, "satoshis": sats})
}

// scanEvery is how often one entry's hold output is looked at for a
// settlement that paid this provider without a remittance.
var scanEvery = time.Minute

func spendsHold(tx *transaction.Transaction, h *HoldRec) bool {
	for _, in := range tx.Inputs {
		if in.SourceTXID != nil && in.SourceTXID.String() == h.Txid && in.SourceTxOutIndex == h.Vout {
			return true
		}
	}
	return false
}

// payoutOpenLocked: no payout is known for the entry, or the one known was
// refused by the network for good (a remittance of a settlement never
// broadcast, which another transaction's spend of the hold output beat).
func (p *Provider) payoutOpenLocked(c string) bool {
	po := p.st.Payouts[c]
	return po == nil || (po.Via != "spent, nothing to this provider" && p.st.IsUnsettleable(po.Settlement))
}

func (p *Provider) notePayout(c, txid string, rank int, sats uint64, via string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.payoutOpenLocked(c) {
		return
	}
	p.st.Payouts[c] = &Payout{Settlement: txid, Rank: rank, Satoshis: sats, Via: via, At: p.now().Unix()}
	_ = p.save()
	p.metrics.inc("bstore_provider_payouts_total", "via="+via)
	p.logf("entry %s: settlement %s pays rank %d, %d satoshis (%s)", short(c), short(txid), rank, sats, via)
}

// scanPayouts finds, for every entry held with a bounty and no payout
// known, whether its hold output was spent, and if the spend pays this
// provider at any rank's BRC-29 key, takes it: a payee that missed the
// remittance finds its output from public data (spec section 9.5). A spend
// that pays nothing here (another payee set, or a reclaim) is noted once.
func (p *Provider) scanPayouts(ctx context.Context) {
	p.mu.Lock()
	var todo []EntryRec
	now := p.now()
	if p.scanned == nil {
		p.scanned = map[string]time.Time{}
	}
	for c, e := range p.st.Entries {
		if e.Decision == decisionHeld && e.Hold != nil && p.payoutOpenLocked(c) && now.Sub(p.scanned[c]) >= scanEvery {
			todo = append(todo, *e)
			p.scanned[c] = now
		}
	}
	p.mu.Unlock()
	for _, e := range todo {
		if ctx.Err() != nil {
			return
		}
		by, err := p.asset.Spender(ctx, e.Hold.Txid, e.Hold.Vout)
		if err != nil || by == "" {
			continue
		}
		mp, height, err := p.asset.Proof(ctx, by)
		if err != nil {
			continue // claimed once it mines
		}
		raw, err := p.asset.TxRaw(ctx, by)
		if err != nil {
			continue
		}
		tx, err := guard.ParseTransaction(raw, len(raw))
		if err != nil {
			continue
		}
		tx.MerklePath = mp
		c, _ := displayHash(e.C)
		prefix := storerec.DerivationPrefix(c)
		if p.front != nil {
			p.forward(ctx, e, by, tx, height, prefix)
			continue
		}
		found := false
		for rank := 1; rank <= int(e.Hold.K) && !found; rank++ {
			suffix := storerec.DerivationSuffix(rank)
			want, err := p.signer.DerivedScript(ctx, &bwallet.Derivation{SecurityLevel: int(bwallet.PaymentProtocol.SecurityLevel),
				Protocol: bwallet.PaymentProtocol.Protocol, KeyID: bwallet.PaymentKeyID(prefix, suffix), CounterpartyHex: e.Writer,
				OwnerHex: p.signer.IdentityHex()})
			if err != nil {
				break
			}
			for i, o := range tx.Outputs {
				if o.LockingScript == nil || !equalScript(*o.LockingScript, *want) {
					continue
				}
				beef, err := tx.AtomicBEEF(false)
				if err != nil {
					break
				}
				in := payee.Payment{Txid: by, Beef: base64.StdEncoding.EncodeToString(beef), OutputIndex: uint32(i), Satoshis: o.Satoshis,
					DerivationPrefix: prefix, DerivationSuffix: suffix, SenderIdentityKey: e.Writer, Class: "settle", Inputs: spends(tx)}
				if !p.ledger.has(by) {
					if _, rep := p.claimPayment(in, "settle"); rep != nil {
						p.logf("entry %s: settlement %s found and not recorded: %s", short(e.C), short(by), string(rep.Body))
						break
					}
				}
				p.madeGood(e, by, o.Satoshis)
				p.notePayout(e.C, by, rank, o.Satoshis, "scan")
				found = true
				break
			}
		}
		if !found {
			p.mu.Lock()
			if !p.payoutOpenLocked(e.C) {
				p.mu.Unlock()
				continue
			}
			p.st.Payouts[e.C] = &Payout{Settlement: by, Via: "spent, nothing to this provider", At: p.now().Unix()}
			_ = p.save()
			p.mu.Unlock()
			p.logf("entry %s: its hold output was spent by %s (height %d), paying this provider nothing", short(e.C), short(by), height)
		}
	}
}

func equalScript(a, b []byte) bool { return hex.EncodeToString(a) == hex.EncodeToString(b) }

// forward passes a spend of a held entry's hold output to the front's
// Forward, once: the bounty pays the advertiser, whose key the front does
// not hold.
func (p *Provider) forward(ctx context.Context, e EntryRec, by string, tx *transaction.Transaction, height uint32, prefix string) {
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return
	}
	n := Settlement{Entry: e.C, Bucket: e.Bucket, Root: e.Root, Hold: e.Hold.Outpoint(), Writer: e.Writer, K: e.Hold.K,
		Txid: by, Height: height, DerivationPrefix: prefix, BEEF: beef}
	if p.front.Forward != nil {
		if err := p.front.Forward(ctx, n); err != nil {
			p.logf("entry %s: the spend %s of its hold output was not forwarded: %v; tried again", short(e.C), short(by), err)
			return
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.payoutOpenLocked(e.C) {
		return
	}
	p.st.Payouts[e.C] = &Payout{Settlement: by, Via: "forwarded", At: p.now().Unix()}
	_ = p.save()
	p.metrics.inc("bstore_provider_payouts_total", "via=forwarded")
	p.logf("entry %s: its hold output was spent by %s (height %d); forwarded for the advertiser", short(e.C), short(by), height)
}

// madeGood: the entry's last known payout was a fast remittance that never
// mined, and the mined spend by now pays this provider at least as much.
// The writer built its settlement again and the provider has its share, so
// the loss is made good: the payer is no longer held for confirmation on
// its account, at once and across a restart. A later loss flags it again.
func (p *Provider) madeGood(e EntryRec, by string, sats uint64) {
	p.mu.Lock()
	po := p.st.Payouts[e.C]
	if po == nil || po.Settlement == by || po.Via != "remittance" || !p.st.IsUnsettleable(po.Settlement) || sats < po.Satoshis {
		p.mu.Unlock()
		return
	}
	if p.st.MadeGood == nil {
		p.st.MadeGood = map[string]string{}
	}
	p.st.MadeGood[po.Settlement] = by
	lost := po.Settlement
	_ = p.save()
	p.mu.Unlock()
	// Only the flag this loss set is lifted: a flag naming another payment
	// (a loss the watch found and the book has not recorded yet) stands.
	if why, flagged := p.acc.Exposure.Flagged(e.Writer); flagged && strings.HasSuffix(why, lost) && !p.stillLost(e.Writer) {
		p.acc.Exposure.Unflag(e.Writer)
	}
	p.logf("entry %s: the lost remittance %s was made good by settlement %s; its writer is no longer held on its account", short(e.C), short(lost), short(by))
}

// stillLost reports whether payer has a lost payment not made good.
func (p *Provider) stillLost(payer string) bool {
	p.mu.Lock()
	lost := slices.Clone(p.st.Unsettleable)
	good := maps.Clone(p.st.MadeGood)
	p.mu.Unlock()
	for _, u := range lost {
		if _, ok := good[u.Txid]; ok {
			continue
		}
		if pl := p.ledger.find(u.Txid); pl != nil && strings.EqualFold(pl.SenderIdentityKey, payer) {
			return true
		}
	}
	return false
}
