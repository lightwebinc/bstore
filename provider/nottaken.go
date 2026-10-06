package provider

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/auth104"
	"github.com/lightwebinc/bstore/storerec"
)

// The payment-not-taken note (frozen.md section 14, row C30): a priced
// challenge refused after its payment passed the payment check (the
// provider's own derivation prefix, output 0 paying this provider at least
// the price, new to the ledger), a payment this provider did not take, is
// answered with a note signed under the challenge key naming the payment,
// the request, why and when. The buyer releases the payment's coin at once
// on it. Before the note is signed the payment's txid is appended, synced,
// to nottaken.jsonl, read back at every start: a payment noted is refused
// for good, so a note never names a payment this provider takes.

// notesPerCaller bounds the notes one caller is signed a day; maxNotTaken
// bounds the txids kept. Past either a refusal carries no note, and the
// buyer holds its coin until it sees the payment was never broadcast.
const (
	notesPerCaller = 200
	maxNotTaken    = 1_000_000
)

func notTakenPath(data string) string { return filepath.Join(data, "nottaken.jsonl") }

// loadNotTaken reads the payments noted not taken.
func loadNotTaken(data string) (map[string]bool, error) {
	out := map[string]bool{}
	f, err := os.Open(notTakenPath(data))
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var l struct {
			Txid string `json:"txid"`
		}
		if json.Unmarshal(sc.Bytes(), &l) == nil && len(l.Txid) == 64 {
			out[l.Txid] = true
		}
	}
	return out, sc.Err()
}

// noteTxid records txid as noted, synced; the caller holds noteMu.
func (p *Provider) noteTxid(txid string) error {
	f, err := os.OpenFile(notTakenPath(p.cfg.Data), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(map[string]any{"txid": txid, "at": p.now().Unix()})
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

// withNotTaken adds a signed note to a refusal of a challenge whose payment
// txid passed the payment check and was not taken. No note on a 402, a
// 409, a 401 or a 429 (a rate limit is answered before any work).
func (p *Provider) withNotTaken(ctx context.Context, rep auth104.Reply, txid, caller string, body []byte) auth104.Reply {
	switch rep.Status {
	case http.StatusOK, http.StatusPaymentRequired, http.StatusConflict, http.StatusUnauthorized, http.StatusTooManyRequests:
		return rep
	}
	var req struct {
		Request string `json:"request"`
	}
	if json.Unmarshal(body, &req) != nil {
		return rep
	}
	request, err := base64.StdEncoding.Strict().DecodeString(req.Request)
	if err != nil || len(request) > storerec.MaxRequest {
		return rep
	}
	h, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return rep
	}
	var m map[string]any
	if json.Unmarshal(rep.Body, &m) != nil {
		return rep
	}
	now := p.now()
	day := now.UTC().Format("2006-01-02")
	p.noteMu.Lock()
	if p.notTaken == nil {
		p.notTaken = map[string]bool{}
	}
	if p.notesBy == nil || p.notesDay != day {
		p.notesBy, p.notesDay = map[string]int{}, day
	}
	if !p.notTaken[txid] {
		if p.ledger.has(txid) || len(p.notTaken) >= maxNotTaken || p.notesBy[caller] >= notesPerCaller {
			p.noteMu.Unlock()
			return rep
		}
		if err := p.noteTxid(txid); err != nil {
			p.noteMu.Unlock()
			p.logf("payment %s: the note could not be recorded (%v); none is signed", short(txid), err)
			return rep
		}
		p.notTaken[txid] = true
		p.notesBy[caller]++
	}
	p.noteMu.Unlock()
	n := &storerec.NotTaken{Payment: [32]byte(h[:]), Request: sha256.Sum256(request), Reason: notTakenReason(rep, m),
		Time: uint64(max(now.Unix(), 0)), Answerer: p.signer.Identity.Compressed()}
	nb, err := n.Encode()
	if err != nil {
		return rep
	}
	d := sha256.Sum256(nb)
	res, err := p.wallet.CreateSignature(ctx, wallet.CreateSignatureArgs{
		EncryptionArgs:     wallet.EncryptionArgs{ProtocolID: storerec.Protocol, KeyID: storerec.KeyChallenge, Counterparty: pushdrop.Anyone()},
		HashToDirectlySign: d[:],
	}, p.wallet.Originator)
	if err != nil {
		return rep
	}
	signed, err := storerec.SignedNotTaken(nb, res.Signature.Serialize())
	if err != nil {
		return rep
	}
	if _, err := storerec.VerifyNotTaken(signed); err != nil {
		return rep
	}
	m["notTaken"] = base64.StdEncoding.EncodeToString(signed)
	out := auth104.JSON(rep.Status, m)
	for k, vs := range rep.Headers {
		if k != "Content-Type" {
			out.Headers[k] = vs
		}
	}
	p.metrics.inc("bstore_provider_payments_total", "result=not-taken")
	p.logf("payment %s not taken (%s): a signed note goes with the refusal", short(txid), n.Reason)
	return out
}

// notTakenReason is a refusal's reason in the note's words: a 422's label,
// else its error code without "ERR_", lowercase, "-" for "_".
func notTakenReason(rep auth104.Reply, m map[string]any) string {
	if rep.Status == http.StatusUnprocessableEntity {
		if s, ok := m["error"].(string); ok && noteWord(s) {
			return s
		}
	}
	if s, ok := m["code"].(string); ok {
		s = strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, "ERR_")), "_", "-")
		if noteWord(s) {
			return s
		}
	}
	return "refused"
}

func noteWord(s string) bool {
	if len(s) < 1 || len(s) > storerec.MaxNoteReason {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
