package provider

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/acceptance"
	"github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/bstore/internal/testwriter"
)

// switchStatus answers arcade's status as the test says: the real one, a
// double spend attempted, or a refusal.
type switchStatus struct {
	mu   sync.Mutex
	mode string
	next acceptance.StatusSource
}

func (s *switchStatus) set(mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = mode
}

func (s *switchStatus) Status(ctx context.Context, txid string) (*publish.ArcadeStatus, error) {
	s.mu.Lock()
	mode := s.mode
	s.mu.Unlock()
	switch mode {
	case "dsa":
		return &publish.ArcadeStatus{TxID: txid, TxStatus: "DOUBLE_SPEND_ATTEMPTED", CompetingTxs: []string{strings.Repeat("ab", 32)}}, nil
	case "rejected":
		return &publish.ArcadeStatus{TxID: txid, TxStatus: "REJECTED", ExtraInfo: "test"}, nil
	}
	return s.next.Status(ctx, txid)
}

func (p *Provider) counter(name string, labels ...string) float64 {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()
	return p.metrics.counters[series(name, labels)]
}

// A paid challenge at or below the threshold is broadcast by the provider
// and answered; past the payer's limit it is held; arcade's
// DOUBLE_SPEND_ATTEMPTED holds; arcade's refusal refuses with a not-taken
// note; a payer whose fast payment was double-spent is held from then on.
func TestAcceptanceChallenge(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.PayerLimit = 80 // two challenges of 40
	st := &switchStatus{}
	p := r.startWith(cfg, true, Options{Accept: func(v *acceptance.Verifier) {
		st.next = v.Status[0]
		v.Status = []acceptance.StatusSource{st}
	}})
	front := cfg.FrontURL()
	put := r.put(ctx, []*Provider{p}, content(64<<10, 3), "accept/one", testwriter.Terms{Sats: 5000, T: 1, K: 1, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	root := put.Object.Built.RootHash
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(root) != nil })
	me := p.signer.Identity.Compressed()
	writer := r.writer.Signer.IdentityHex()
	nonce := byte(20)
	ask := func() (int, []byte) {
		t.Helper()
		nonce++
		_, raw, err := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{nonce})
		if err != nil {
			t.Fatal(err)
		}
		call, err := r.writer.Challenge(ctx, front, raw, "", 1000)
		if err != nil {
			t.Fatal(err)
		}
		return call.Status, call.Body
	}
	fast := func() float64 {
		return p.counter("bstore_provider_payments_total", "decision=fast", "reason=at-or-below-threshold")
	}
	held := func(reason string) float64 {
		return p.counter("bstore_provider_payments_total", "decision=hold", "reason="+reason)
	}

	if s, _ := ask(); s != 200 || fast() != 1 {
		t.Fatalf("small: %d, fast %v\n%s", s, fast(), r.said())
	}
	for _, pl := range p.ledger.payments() {
		if r.chain.Tx(pl.Txid) == nil {
			t.Fatalf("payment %s answered and not broadcast by the provider", pl.Txid)
		}
	}
	if s, _ := ask(); s != 200 || fast() != 2 {
		t.Fatalf("second: %d", s)
	}
	// The limit: two of 40 unmined is all one payer may have fast. The
	// chain mines at once here, so the watch releases them on its next
	// sweep; asked before that, the third is held.
	p.acc.Exposure.Charge(writer, strings.Repeat("cd", 32), 80, time.Now())
	if s, _ := ask(); s != 202 || held("payer-limit") < 1 {
		t.Fatalf("over the payer's limit: %d %v", s, held("payer-limit"))
	}
	p.acc.Exposure.Release(strings.Repeat("cd", 32))
	r.eventually("the watch confirms the fast payments", 10*time.Second, func() bool {
		return p.counter("bstore_provider_payment_events_total", "kind=confirmed") >= 2
	})

	st.set("dsa")
	if s, _ := ask(); s != 202 || held("double-spend-attempted") < 1 {
		t.Fatalf("DOUBLE_SPEND_ATTEMPTED: %d %v", s, held("double-spend-attempted"))
	}
	st.set("rejected")
	s, body := ask()
	var rb struct{ NotTaken string }
	_ = json.Unmarshal(body, &rb)
	if s != 400 || rb.NotTaken == "" || !bytes.Contains(body, []byte("ERR_PAYMENT_REFUSED")) {
		t.Fatalf("refused on the fast path, with a note: %d %s", s, body)
	}
	st.set("")

	// A fast payment its payer double-spends: the watch flags the payer.
	r.chain.SetHold(true)
	before := len(p.ledger.payments())
	if s, _ := ask(); s != 200 {
		t.Fatalf("before the double spend: %d\n%s", s, r.said())
	}
	ps := p.ledger.payments()
	if len(ps) != before+1 {
		t.Fatal("no new payment")
	}
	lost := ps[len(ps)-1]
	in, _, _ := strings.Cut(lost.Inputs[0], ".")
	r.chain.SpendElsewhere(in, 0, strings.Repeat("ef", 32))
	r.eventually("the payer flagged", 10*time.Second, func() bool { _, f := p.acc.Exposure.Flagged(writer); return f })
	r.chain.SetHold(false)
	r.eventually("the lost payment recorded unsettleable", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.st.IsUnsettleable(lost.Txid)
	})
	if s, _ := ask(); s != 202 || held("payer-flagged") < 1 {
		t.Fatalf("a flagged payer: %d", s)
	}
}

// With a threshold below every price, a challenge's payment is broadcast,
// taken and held 202, and the same payment buys its answer once mined; a
// renewal answers 202 and is made once its payment mines.
func TestAcceptanceHeld(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	one := uint64(1)
	cfg.ThresholdSats = &one
	p := r.start(cfg, true)
	front := cfg.FrontURL()
	put := r.put(ctx, []*Provider{p}, content(64<<10, 4), "accept/two", testwriter.Terms{Sats: 5000, T: 1, K: 1, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	root := put.Object.Built.RootHash
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(root) != nil })
	me := p.signer.Identity.Compressed()

	r.chain.SetHold(true)
	_, raw, _ := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{1})
	if call, err := r.writer.Challenge(ctx, front, raw, "", 1000); err != nil || call.Status != 202 || !bytes.Contains(call.Body, []byte("ERR_PAYMENT_HELD")) {
		t.Fatalf("a held challenge, through a BRC-105 client: %v %+v", err, call)
	}
	ps := p.ledger.payments()
	if len(ps) != 1 || !p.isHeld(ps[0].Txid) || r.chain.Tx(ps[0].Txid) == nil {
		t.Fatalf("held: %d payments\n%s", len(ps), r.said())
	}
	pl := ps[0]
	var chk *paymentLine
	if rep, done := p.challengePayment(ctx, &pl, r.writer.Signer.IdentityHex(), &chk); !done || rep.Status != 202 {
		t.Fatalf("sent again before it mines: %v %d", done, rep.Status)
	}
	r.chain.SetHold(false)
	r.chain.Mine()
	// Settled into the pool meanwhile: the hold still stands.
	if err := (lockedBook{p}).RecordSettled(pl.Txid); err != nil {
		t.Fatal(err)
	}
	p.confirmHeld(ctx)
	if !p.isHeld(pl.Txid) {
		t.Fatal("settling a held payment ended its hold")
	}
	// Two requests sending it again at once: one answer between them.
	answers := make(chan bool, 2)
	for range 2 {
		go func() {
			var c *paymentLine
			_, done := p.challengePayment(ctx, &pl, r.writer.Signer.IdentityHex(), &c)
			answers <- !done
		}()
	}
	if a, b := <-answers, <-answers; a == b || p.isHeld(pl.Txid) {
		t.Fatalf("sent again once mined, it buys exactly one answer: %v %v", a, b)
	}

	ad := p.liveAd(root)
	r.chain.SetHold(true)
	body, _ := json.Marshal(map[string]any{"root": hex.EncodeToString(root[:]), "until": ad.Expiry + 86400})
	call, err := r.writer.Post(ctx, front+"/renew", body, 100000)
	if err != nil || call.Status != 202 || !bytes.Contains(call.Body, []byte(`"held":true`)) {
		t.Fatalf("renewal above the threshold: %v %+v", err, call)
	}
	time.Sleep(500 * time.Millisecond)
	if a := p.liveAd(root); a.Expiry != ad.Expiry {
		t.Fatal("renewed before the payment mined")
	}
	r.chain.SetHold(false)
	r.chain.Mine()
	r.eventually("renewed once mined", 20*time.Second, func() bool { a := p.liveAd(root); return a != nil && a.Expiry == ad.Expiry+86400 })
}
