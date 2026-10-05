package provider

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/payee"

	"github.com/lightwebinc/bstore/internal/auth104"
	"github.com/lightwebinc/bstore/internal/storerec"
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
func (p *Provider) settle(ctx context.Context, _ *http.Request, body []byte, _ *auth104.Caller) auth104.Reply {
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
	if _, rep := p.claimPayment(pl, "settle"); rep != nil {
		return *rep
	}
	p.notePayout(ec.C, in.Txid, rm.Rank, sats, "remittance")
	return auth104.JSON(http.StatusOK, map[string]any{"accepted": true, "txid": in.Txid, "satoshis": sats})
}

func spendsHold(tx *transaction.Transaction, h *HoldRec) bool {
	for _, in := range tx.Inputs {
		if in.SourceTXID != nil && in.SourceTXID.String() == h.Txid && in.SourceTxOutIndex == h.Vout {
			return true
		}
	}
	return false
}

func (p *Provider) notePayout(c, txid string, rank int, sats uint64, via string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.st.Payouts[c] != nil {
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
	for c, e := range p.st.Entries {
		if e.Decision == decisionHeld && e.Hold != nil && p.st.Payouts[c] == nil {
			todo = append(todo, *e)
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
				p.notePayout(e.C, by, rank, o.Satoshis, "scan")
				found = true
				break
			}
		}
		if !found {
			p.mu.Lock()
			p.st.Payouts[e.C] = &Payout{Settlement: by, Via: "spent, nothing to this provider", At: p.now().Unix()}
			_ = p.save()
			p.mu.Unlock()
			p.logf("entry %s: its hold output was spent by %s (height %d), paying this provider nothing", short(e.C), short(by), height)
		}
	}
}

func equalScript(a, b []byte) bool { return hex.EncodeToString(a) == hex.EncodeToString(b) }
