package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/auth104"
	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// A priced challenge refused after its payment came, here because the
// object is lost when the answer is read, carries a note signed under the
// challenge key that the payment was not taken (row C30); the payment is
// not in the ledger and is refused from then on.
func TestPaymentNotTakenNote(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, true)
	front := cfg.FrontURL()
	data := content(64<<10, 7)
	put := r.put(ctx, []*Provider{p}, data, "lost/one", testwriter.Terms{Sats: 5000, T: 1, K: 1, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	root := put.Object.Built.RootHash
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(root) != nil })
	me := p.signer.Identity.Compressed()

	// Answered and paid: no note.
	_, raw, err := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	call, err := r.writer.Challenge(ctx, front, raw, "", 1000)
	if err != nil || call.Status != 200 || call.Payment == nil {
		t.Fatalf("paid: %v %+v", err, call)
	}

	// Lost after the 402: the paid request is refused with a note.
	_, raw2, _ := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{10})
	for _, b := range put.Object.Built.Blobs {
		if err := p.store.Delete(sha256.Sum256(b)); err != nil {
			t.Fatal(err)
		}
	}
	call, err = r.writer.Challenge(ctx, front, raw2, "", 1000)
	if err != nil || call.Status != 422 || call.Payment == nil {
		t.Fatalf("refused after payment: %v %+v", err, call)
	}
	var body struct {
		Error    string `json:"error"`
		NotTaken string `json:"notTaken"`
	}
	if err := json.Unmarshal(call.Body, &body); err != nil || body.Error != "unknown-root" || body.NotTaken == "" {
		t.Fatalf("refusal body %s", call.Body)
	}
	signed, _ := base64.StdEncoding.DecodeString(body.NotTaken)
	txid := call.Payment.TxID()
	n, err := storerec.CheckNotTaken(signed, [32]byte(txid[:]), raw2, me)
	if err != nil || n.Reason != "unknown-root" || n.Time == 0 {
		t.Fatalf("note: %v %+v", err, n)
	}
	if p.ledger.has(txid.String()) {
		t.Fatal("a payment noted not taken is in the ledger")
	}
	// The noted payment is refused from then on, and never signed for
	// twice in the ledger's place.
	if _, rep := p.claimPayment(paymentLine{Txid: txid.String()}, "challenge"); rep == nil || rep.Status != 409 {
		t.Fatalf("a noted payment was taken: %+v", rep)
	}
	// After a restart the noted payment is still refused: it was synced
	// to nottaken.jsonl before the note was signed.
	p.noteMu.Lock()
	p.notTaken = nil
	p.noteMu.Unlock()
	reloaded, err := loadNotTaken(cfg.Data)
	if err != nil || !reloaded[txid.String()] {
		t.Fatalf("not reloaded: %v", err)
	}
	p.noteMu.Lock()
	p.notTaken = reloaded
	p.noteMu.Unlock()
	if _, rep := p.claimPayment(paymentLine{Txid: txid.String()}, "challenge"); rep == nil || rep.Status != 409 {
		t.Fatal("a noted payment was taken after a restart")
	}
	// No note on a rate limit, nor past a caller's daily cap.
	reqBody := []byte(`{"request":"` + base64.StdEncoding.EncodeToString(raw2) + `"}`)
	busy := auth104.Error(429, "ERR_RATE_LIMITED", "later")
	if rep := p.withNotTaken(ctx, busy, strings.Repeat("ab", 32), "caller", reqBody); bytes.Contains(rep.Body, []byte("notTaken")) {
		t.Fatal("a 429 carried a note")
	}
	refused := auth104.Error(503, "ERR_BUSY", "later")
	for i := 0; i <= notesPerCaller; i++ {
		id := fmt.Sprintf("%064x", i+1)
		rep := p.withNotTaken(ctx, refused, id, "flooder", reqBody)
		if got := bytes.Contains(rep.Body, []byte("notTaken")); got != (i < notesPerCaller) {
			t.Fatalf("note %d: %v", i, got)
		}
	}
}
