package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/auth104"
	"github.com/lightwebinc/bstore/storerec"
)

// fakeFront is a provider's front over BRC-104 that answers challenges of
// one content: honestly, or only the draws whose samples miss the
// segments it lacks (a 500 for the others), or refusing every request.
type fakeFront struct {
	t       *testing.T
	key     *ec.PrivateKey
	content []byte
	tree    *storerec.ProofTree
	// lacks marks segments it does not hold; refuse answers every request
	// 422 unknown-root.
	lacks  func(seg uint64) bool
	refuse bool
	// noRoute answers every request 404, as a host with no bstore routes;
	// delay holds each answer.
	noRoute bool
	// notFound answers every request 404 with a code other than
	// ERR_ROUTE_NOT_FOUND: a route that is there and refused.
	notFound bool
	delay    time.Duration
	mu       sync.Mutex
	seen     map[string]int // nonce -> requests
	srv      *httptest.Server
}

func newFakeFront(t *testing.T, content []byte) *fakeFront {
	key, _ := ec.NewPrivateKey()
	f := &fakeFront{t: t, key: key, content: content, tree: storerec.NewProofTree(content), seen: map[string]int{}}
	pw, err := wallet.NewProtoWallet(wallet.ProtoWalletArgs{Type: wallet.ProtoWalletArgsTypePrivateKey, PrivateKey: key})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := auth104.New(context.Background(), auth104.Config{Wallet: pw, PerAddress: auth104.Budget{Rate: 1000, Burst: 1000},
		Handshakes: auth104.Budget{Rate: 1000, Burst: 1000}, Responses: auth104.Budget{Rate: 1000, Burst: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(auth104.HandshakePath, srv.Handshake)
	mux.HandleFunc("/store/bstore/v1/challenge", func(w http.ResponseWriter, r *http.Request) {
		srv.Serve(w, r, 1<<16, func(_ context.Context, _ *http.Request, body []byte, _ *auth104.Caller) auth104.Reply {
			var req struct {
				Request string `json:"request"`
			}
			_ = json.Unmarshal(body, &req)
			raw, _ := base64.StdEncoding.DecodeString(req.Request)
			q, err := storerec.DecodeRequest(raw)
			if err != nil {
				return auth104.JSON(422, map[string]string{"error": "request"})
			}
			f.mu.Lock()
			f.seen[hex.EncodeToString(q.Nonce[:])]++
			f.mu.Unlock()
			time.Sleep(f.delay)
			if f.noRoute {
				return auth104.Error(http.StatusNotFound, "ERR_ROUTE_NOT_FOUND", "no such route")
			}
			if f.notFound {
				return auth104.Error(http.StatusNotFound, "ERR_NOT_FOUND", "no such object")
			}
			if f.refuse {
				return auth104.JSON(422, map[string]string{"error": "unknown-root"})
			}
			for k := uint32(0); k < uint32(q.C); k++ {
				if f.lacks != nil && f.lacks(q.Index(k)) {
					return auth104.Error(500, "ERR_INTERNAL", "not now")
				}
			}
			b, err := storerec.Answer(q, raw, f.content, f.tree, true).Encode()
			if err != nil {
				return auth104.Error(500, "ERR_INTERNAL", err.Error())
			}
			ck, _ := wallet.NewKeyDeriver(f.key).DerivePrivateKey(storerec.Protocol, storerec.KeyChallenge, pushdrop.Anyone())
			signed, err := storerec.SignResponse(ck, raw, b)
			if err != nil {
				return auth104.Error(500, "ERR_INTERNAL", err.Error())
			}
			return auth104.Reply{Status: 200, Headers: http.Header{"Content-Type": {"application/cbor"}}, Body: signed}
		})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeFront) identity() []byte { return f.key.PubKey().Compressed() }

func (f *fakeFront) draws() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seen)
}

// stageContent replaces the rig's staged object.
func (r *raceRig) stageContent(n int) []byte {
	r.t.Helper()
	content := make([]byte, n)
	_, _ = rand.Read(content)
	st, err := r.store.Stage(r.ctx, bytes.NewReader(content), 1<<30)
	if err != nil {
		r.t.Fatal(err)
	}
	r.staged = st
	return content
}

func candAt(r *Race, f *fakeFront) *Cand {
	url := f.srv.URL + "/store" + storerec.CHIRPURLSuffix(r.entry.Root)
	c := &Cand{Advertiser: hex.EncodeToString(f.identity()), First: strings.Repeat("ab", 32), FirstSeen: 1, URL: url, Expiry: 2_000_000_000, Status: CandSeen}
	r.Cands = append(r.Cands, c)
	return c
}

// A provider that answers only the draws it can serve gets no more draws
// than maxDraws, whatever it refuses, and does not pass.
func TestDrawsAreBounded(t *testing.T) {
	r := newRaceRig(t)
	content := r.stageContent(256 << 10) // 64 segments
	race := r.put("k", Bounty{Satoshis: 4000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	f := newFakeFront(t, content)
	f.lacks = func(seg uint64) bool { return seg%2 == 1 } // half of it
	c := candAt(race, f)
	blobs, err := r.p.closureBlobs(r.ctx, race.entry)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20 && c.Status != CandFailed && c.Status != CandPassed; i++ {
		tip, _ := r.p.bestHeader(r.ctx)
		challengeAlone(r.ctx, r.p, race, c, blobs, tip.Height)
	}
	// A draw left unanswered three times is spent and the front is not
	// asked again, its own derived front included: with no other front the
	// candidate waits, never passing.
	if c.Status == CandPassed || f.draws() > maxDraws || c.Draws < 1 || !slices.Contains(c.BadFronts, f.srv.URL+"/store/bstore/v1") {
		t.Fatalf("status %s after %d distinct draws, %d spent, bad fronts %v", c.Status, f.draws(), c.Draws, c.BadFronts)
	}
	// A request not answered is sent again unchanged, never redrawn.
	for nonce, n := range f.seen {
		if n < pendingTries {
			t.Fatalf("draw %s sent %d times", nonce[:8], n)
		}
	}
}

// A front that is not the advertiser cannot fail it: its refusal is its
// own, the front is not asked again, and the advertiser's own front then
// passes.
func TestThirdPartyFrontCannotFailAnAdvertiser(t *testing.T) {
	r := newRaceRig(t)
	content := r.stageContent(64 << 10)
	race := r.put("k", Bounty{Satoshis: 4000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	honest := newFakeFront(t, content)
	hostile := newFakeFront(t, content)
	hostile.refuse = true
	c := candAt(race, honest)
	blobs, _ := r.p.closureBlobs(r.ctx, race.entry)
	tip, _ := r.p.bestHeader(r.ctx)
	// A draw bound to the hostile front, as a statement would name it.
	ch := &Challenge{Root: race.entry.Root, ProofRoot: race.entry.ProofRoot, Size: race.entry.Size, Provider: honest.identity(),
		Answerer: hostile.identity(), C: 30}
	_, raw, err := r.p.NewRequest(r.ctx, ch)
	if err != nil {
		t.Fatal(err)
	}
	c.Pending = &PendingDraw{Request: hex.EncodeToString(raw), Front: hostile.srv.URL + "/store/bstore/v1", Answerer: hex.EncodeToString(hostile.identity())}
	challengeAlone(r.ctx, r.p, race, c, blobs, tip.Height)
	if c.Status == CandFailed || len(c.BadFronts) != 1 || c.Draws != 1 {
		t.Fatalf("a third party's refusal: %s %v %d", c.Status, c.BadFronts, c.Draws)
	}
	challengeAlone(r.ctx, r.p, race, c, blobs, tip.Height)
	if c.Status != CandPassed {
		t.Fatalf("the advertiser's own front: %s %+v", c.Status, c.Attempts)
	}
	// The advertiser's own refusal fails it at once.
	c2 := candAt(race, hostile)
	c2.Advertiser = hex.EncodeToString(hostile.identity())
	challengeAlone(r.ctx, r.p, race, c2, blobs, tip.Height)
	if c2.Status != CandFailed {
		t.Fatalf("the advertiser's own unknown-root: %s", c2.Status)
	}
}

// A request is seeded below the tip, so an answerer a block or two behind
// still holds the seed.
func TestSeedBelowTheTip(t *testing.T) {
	r := newRaceRig(t)
	tip, _ := r.p.bestHeader(r.ctx)
	q, _, err := r.p.NewRequest(r.ctx, &Challenge{Root: [32]byte{1}, ProofRoot: [32]byte{2}, Size: 1, Provider: r.p.Identity(), Answerer: r.p.Identity(), C: 1})
	if err != nil || q.SeedHeight != uint64(tip.Height)-SeedBack {
		t.Fatalf("seed %d at tip %d: %v", q.SeedHeight, tip.Height, err)
	}
}

// A renewal chain longer than maxAdHops is not followed to its end: the
// candidate counts as not live, and no spend source is asked without
// bound.
func TestAdChainIsBounded(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 4000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a := r.provider("a")
	first := r.advertise(a, 1_800_000_000, nil)
	ad := first
	for i := 1; i <= maxAdHops+1; i++ {
		ad = r.advertise(a, 1_800_000_000+uint64(i), ad)
	}
	got, err := r.p.adChain(r.ctx, first.TxID().String(), 0, a.key(), race.entry.Root, 1_800_000_000, 0)
	if err != nil || got.Live() {
		t.Fatalf("a chain past the bound: %v live %v", err, got != nil && got.Live())
	}
	short := r.advertise(a, 1_900_000_000, nil)
	short = r.advertise(a, 1_900_000_001, short)
	got, err = r.p.adChain(r.ctx, short.TxID().String(), 0, a.key(), race.entry.Root, 1_900_000_001, 0)
	_ = got
	if err != nil {
		t.Fatal(err)
	}
}

// The window closing with at least t passed settles without -close.
func TestWindowCloseSettles(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 5000, T: 1, K: 3, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a := r.provider("a")
	adA := r.advertise(a, 2_000_000_000, nil)
	passed(race, a, adA, 1)
	race.Started = time.Now().Add(-2 * time.Hour).Unix()
	if err := r.p.Step(r.ctx, race, RaceOptions{Window: time.Hour}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.p.Settle(r.ctx, race, false)
	if rec == nil || race.State != RaceSettled {
		t.Fatalf("a race the window closed: %v %s", err, race.State)
	}
}

// A recorded settlement whose fee coin was spent elsewhere can never mine:
// it is set aside, the next settle builds another that pays, and a
// reclaim is not refused by the one set aside.
func TestStrandedSettlementIsRebuilt(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 5000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a := r.provider("a")
	adA := r.advertise(a, 2_000_000_000, nil)
	passed(race, a, adA, 1)
	r.leg.drop.Store(true)
	short, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	rec, _ := r.p.Settle(short, race, false)
	cancel()
	r.leg.drop.Store(false)
	if rec == nil {
		t.Fatal("no settlement recorded")
	}
	feeTx, err := funding.Rebuild(rec.Fee.Raw, rec.Fee.Bump, "")
	if err != nil {
		t.Fatal(err)
	}
	change, _ := r.p.signer.FundScript()
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(feeTx, rec.Fee.Vout, r.p.signer.FundUnlocker())
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: feeTx.Outputs[rec.Fee.Vout].Satoshis - 500, LockingScript: change})
	if err := tx.Sign(); err != nil {
		t.Fatal(err)
	}
	if err := r.leg.Submit(r.ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.p.Settle(r.ctx, race, false); !errors.Is(err, ErrNotYet) || race.Settlement != nil || len(race.Abandoned) != 1 {
		t.Fatalf("a stranded settlement: %v %+v", err, race.Settlement)
	}
	rec2, err := r.p.Settle(r.ctx, race, false)
	if rec2 == nil || rec2.Txid == rec.Txid || race.State != RaceSettled {
		t.Fatalf("rebuilt: %v %s", err, race.State)
	}
}

// A put that fails after taking its hold output gives it back, and one
// left taken with no entry is reclaimed; each reclaim is recorded whole.
func TestFailedPutReleasesItsHold(t *testing.T) {
	r := newRaceRig(t)
	r.p.cfg.Delivery.Uploads = []string{"https://127.0.0.1:1"}
	b := Bounty{Satoshis: 3000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 30, CheckC: 30}
	if _, err := r.p.Put(r.ctx, Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: "x", ContentType: "text/plain"},
		Object{Root: r.staged.Root, Size: r.staged.Size, ContentHash: r.staged.ContentHash, ProofRoot: r.staged.ProofRoot,
			Closure: StoreClosure{Store: r.store, Manifest: r.staged.Manifest}}, &b); err == nil {
		t.Fatal("a put no provider took succeeded")
	}
	r.p.cfg.Delivery.Uploads = nil
	for _, h := range r.p.Holds() {
		if h.Used {
			t.Fatalf("hold %s.%d stranded", h.Txid[:8], h.Vout)
		}
	}
	// The fill a take started has finished: no hold output appears behind
	// the reclaim.
	r.p.holdMu.Lock()
	r.p.holdMu.Unlock()
	// One taken by a run that stopped before its entry.
	hs := r.p.Holds()
	_ = r.p.updateHold(hs[0], func(x *HoldRecord) { x.Used = true })
	done, err := r.p.ReclaimUnused(r.ctx)
	if err != nil || len(done) < 1 {
		t.Fatalf("reclaim: %v %v", err, done)
	}
	before := map[string]bool{}
	for _, h := range hs {
		if h.BumpHex != "" {
			before[h.Txid+string(rune(h.Vout))] = true
		}
	}
	for _, h := range r.p.Holds() {
		if !before[h.Txid+string(rune(h.Vout))] {
			continue // minted by a fill after the snapshot
		}
		if h.Reclaim == nil || h.Reclaim.Raw == "" || h.Reclaim.Height == 0 {
			t.Fatalf("hold %s.%d: %+v", h.Txid[:8], h.Vout, h.Reclaim)
		}
	}
}

// A payment the payee may still hold is never refunded at once: its coin
// comes back only when the node shows it unspent long after.
func TestUncertainPaymentReconciled(t *testing.T) {
	r := newRaceRig(t)
	pay := r.p.NewPayer(r.ctx)
	fee, err := pay.TakeAtLeast(r.ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	change, _ := r.p.signer.FundScript()
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(fee.Tx, fee.Vout, fee.Unlocker)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: fee.Tx.Outputs[fee.Vout].Satoshis - 300, LockingScript: change})
	if err := tx.Sign(); err != nil {
		t.Fatal(err)
	}
	r.p.notePayment(tx, false, false)
	coin := fee.Tx.TxID().String()
	has := func() bool {
		for _, o := range r.p.wallet.Pool.Outputs() {
			if o.TxID == coin && o.Vout == fee.Vout {
				return true
			}
		}
		return false
	}
	if r.p.ReconcilePayments(r.ctx) != 1 || has() {
		t.Fatal("a fresh payment's coin came back")
	}
	r.p.cfg.Now = func() time.Time { return time.Now().Add(2 * paymentWait) }
	if r.p.ReconcilePayments(r.ctx) != 0 || !has() {
		t.Fatal("a payment never broadcast did not give its coin back")
	}
}

// No authenticated answer is read past the bound.
func TestAnswerBound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		buf := make([]byte, 64<<10)
		for i := 0; i < 64; i++ {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	hc := &http.Client{Transport: limitTransport{base: http.DefaultTransport, max: 1 << 20}}
	res, err := hc.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if n, err := io.Copy(io.Discard, res.Body); err == nil || n > 1<<20 {
		t.Fatalf("read %d bytes past the bound: %v", n, err)
	}
}

// After a kill nothing it retracted is sent again.
func TestOutboxDrop(t *testing.T) {
	dir := t.TempDir()
	o, err := openOutbox(filepath.Join(dir, "outbox"), &Delivery{}, nil, t.Logf, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"piece", "entry", "outcome", "bucket token", "hold output"} {
		if _, err := o.add(strings.ReplaceAll(w, " ", "-"), w, []byte{1}, []Target{{Base: "http://x", Topic: "t"}}); err != nil {
			t.Fatal(err)
		}
	}
	if n := o.Drop("piece", "entry", "outcome"); n != 3 || o.Pending() != 2 {
		t.Fatalf("dropped %d, %d left", n, o.Pending())
	}
}

// A re-encoding (other unlocking scripts, the same outpoints and outputs)
// is the same spend; anything else is not.
func TestSameEffect(t *testing.T) {
	prev := transaction.NewTransaction()
	s1, _ := hex.DecodeString("76a914" + strings.Repeat("11", 20) + "88ac")
	mk := func(unlock string, sats uint64) *transaction.Transaction {
		tx := transaction.NewTransaction()
		h := prev.TxID()
		u, _ := hex.DecodeString(unlock)
		us := scriptOf(u)
		tx.AddInput(&transaction.TransactionInput{SourceTXID: h, SourceTxOutIndex: 0, UnlockingScript: us})
		ls := scriptOf(s1)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: ls})
		return tx
	}
	a, b, c := mk("0101", 10), mk("4c0101", 10), mk("0101", 11)
	if !SameEffect(a, b) || SameEffect(a, c) {
		t.Fatal("SameEffect")
	}
}

func scriptOf(b []byte) *script.Script {
	s := script.Script(b)
	return &s
}

// challengeAlone is one candidate's step as judge runs it, alone.
func challengeAlone(ctx context.Context, p *Publisher, race *Race, c *Cand, blobs []chirp.Child, tip uint32) {
	p.challengeCand(ctx, race, c, blobs, tip, RaceOptions{}, func() error { return p.saveRace(race) },
		func(n PublishedNote) { race.Outcomes = append(race.Outcomes, n) })
}
