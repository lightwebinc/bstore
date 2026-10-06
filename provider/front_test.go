package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/producer"

	"github.com/lightwebinc/bstore/backend"
	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// fakeBridge is a bridge backend for tests: the "incumbent" is an object
// map served over TLS with Range, and its advertisements are minted under
// an incumbent wallet on the rig's chain. Get can be made to lose objects or
// to answer ranges with other bytes.
type fakeBridge struct {
	r    *rig
	inc  *bwallet.Embedded
	base string

	mu         sync.Mutex
	objs       map[[32]byte][]byte
	lost       map[[32]byte]bool
	rangeWrong bool
	wrong      int
	puts       int
	otherKey   bool
	down       bool
}

func newFakeBridge(t *testing.T, r *rig) *fakeBridge {
	inc, err := bwallet.Create(filepath.Join(t.TempDir(), "incumbent"), testwriter.Profile)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeBridge{r: r, inc: inc, objs: map[[32]byte][]byte{}, lost: map[[32]byte]bool{}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	cert, err := tls.LoadX509KeyPair(r.cert, r.key)
	if err != nil {
		t.Fatal(err)
	}
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	f.base = srv.URL + "/inc"
	return f
}

func (f *fakeBridge) key() []byte { return f.inc.Signer().Identity.Compressed() }

func (f *fakeBridge) serve(w http.ResponseWriter, r *http.Request) {
	i := strings.LastIndex(r.URL.Path, "/")
	h, err := chirp.ParseIdentifier(r.URL.Path[i+1:])
	f.mu.Lock()
	data, ok := f.objs[h]
	if f.lost[h] {
		ok = false
	}
	f.mu.Unlock()
	if err != nil || !ok {
		http.NotFound(w, r)
		return
	}
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
}

func (f *fakeBridge) Put(h [32]byte, data []byte) error {
	if sha256.Sum256(data) != h {
		return backend.ErrHash
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[h] = append([]byte(nil), data...)
	f.puts++
	return nil
}

func (f *fakeBridge) Has(h [32]byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objs[h]
	return ok && !f.lost[h]
}

func (f *fakeBridge) Get(h [32]byte, off, n uint64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objs[h]
	if f.down {
		return nil, &net.OpError{Op: "read", Err: io.EOF}
	}
	if !ok || f.lost[h] {
		return nil, backend.ErrMissing
	}
	if off+n > uint64(len(data)) {
		return nil, backend.ErrRange
	}
	out := append([]byte(nil), data[off:off+n]...)
	if f.rangeWrong && !(off == 0 && n == uint64(len(data))) {
		for i := range out {
			out[i] ^= 0x5a
		}
	}
	return out, nil
}

func (f *fakeBridge) ServeURL(root [32]byte) string { return f.base + storerec.CHIRPURLSuffix(root) }

func (f *fakeBridge) RangeWrong([32]byte) {
	f.mu.Lock()
	f.wrong++
	f.mu.Unlock()
}

// Advertise mints the incumbent's advertisement of root through until and
// mines it: the incumbent's commit.
func (f *fakeBridge) Advertise(ctx context.Context, root [32]byte, until uint64) (*backend.Advert, error) {
	f.mu.Lock()
	rb, ok := f.objs[root]
	other := f.otherKey
	f.mu.Unlock()
	if !ok {
		return nil, errors.New("the incumbent does not hold the root")
	}
	host := f.key()
	signer := f.inc
	if other {
		w, err := bwallet.Create(filepath.Join(f.r.t.TempDir(), "other"), testwriter.Profile)
		if err != nil {
			return nil, err
		}
		host, signer = w.Signer().Identity.Compressed(), w
	}
	lock, err := storerec.AdDerivation.Lock(ctx, signer, "test", adFields(host, root, f.ServeURL(root), until, uint64(len(rb))), true)
	if err != nil {
		return nil, err
	}
	w := f.r.writer
	tip, err := w.Asset.BestHeader(ctx)
	if err != nil {
		return nil, err
	}
	pay := &producer.Payer{Pool: w.W.Pool, Tip: tip.Height, Keys: map[string]*bwallet.Signer{w.Signer.IdentityHex(): w.Signer},
		Settler: w.Settler, Asset: w.Asset, Fees: w.Fees}
	fee, err := pay.TakeAtLeast(ctx, w.Fees.Floor*4)
	if err != nil {
		return nil, err
	}
	change, _ := w.Signer.FundScript()
	tx, err := mintAd(lock, fee, change, w)
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	if err := w.Settler.Submit(ctx, tx); err != nil {
		return nil, err
	}
	mp, h, err := nodeapi.WaitMined(ctx, w.Asset, tx.TxID().String(), 20*time.Millisecond)
	if err != nil {
		return nil, err
	}
	pay.Change(tx, h, mp)
	tx.MerklePath = mp
	beef, err := tx.BEEF()
	if err != nil {
		return nil, err
	}
	return &backend.Advert{Txid: tx.TxID().String(), Vout: 0, Expiry: until, BEEF: beef}, nil
}

func TestFrontHandsAnswersByReferenceAndForwards(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	saved := scanEvery
	t.Cleanup(func() { scanEvery = saved })
	scanEvery = 200 * time.Millisecond
	fb := newFakeBridge(t, r)
	cfg := r.config(filepath.Join(t.TempDir(), "front"))
	cfg.Backend = "bridge"
	var fmu sync.Mutex
	var forwarded []Settlement
	p := r.startWith(cfg, false, Options{Front: &Front{Backend: fb, Advertiser: fb.key(), Forward: func(_ context.Context, s Settlement) error {
		fmu.Lock()
		defer fmu.Unlock()
		forwarded = append(forwarded, s)
		return nil
	}}})
	front := cfg.FrontURL()
	me := p.signer.Identity.Compressed()

	data := content(2*storerec.BlobSize+5000, 7)
	put := r.put(ctx, []*Provider{p}, data, "photos/front.bin", testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	root := put.Object.Built.RootHash
	r.eventually("the incumbent's advertisement adopted", 30*time.Second, func() bool { return p.liveAd(root) != nil })
	ad := p.liveAd(root)
	adv, err := advOf(ad)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(adv.Host, fb.key()) || adv.URL != fb.ServeURL(root) {
		t.Fatalf("adopted %x at %s", adv.Host, adv.URL)
	}
	p.mu.Lock()
	h := p.st.Holdings[hex.EncodeToString(root[:])]
	handed, held := h.Handed, h.Bytes
	q0 := len(p.st.Outbox)
	p.mu.Unlock()
	if !handed || held != 0 {
		t.Fatalf("handed %v, bytes %d", handed, held)
	}
	for _, o := range put.Object.Built.Objects() {
		if p.store.Has(sha256.Sum256(o)) {
			t.Fatal("the front kept a copy of a handed object")
		}
	}
	if q0 == 0 && !strings.Contains(r.said(), "published to the records topic") {
		t.Fatal("the advertisement was not queued for the records topic")
	}

	// A free settlement challenge, by reference, answered by the front for
	// the incumbent.
	request := func(provider []byte, nonce byte) (*storerec.Request, []byte) {
		q, _, err := r.writer.Request(ctx, put.Object.Built, provider, 30, [16]byte{nonce})
		if err != nil {
			t.Fatal(err)
		}
		q.Answerer = me
		raw, err := q.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return q, raw
	}
	fetch := &testwriter.Fetcher{HTTP: r.client, URL: adv.URL}
	q, raw := request(fb.key(), 1)
	call, err := r.writer.Challenge(ctx, front, raw, put.C, 0)
	if err != nil || call.Status != 200 {
		t.Fatalf("free challenge at the front: %v %+v\n%s", err, call, r.said())
	}
	if v := testwriter.Verify(q, raw, call.Body, put.Object, fetch); v.Outcome != storerec.OutcomePass {
		t.Fatalf("verdict %+v", v)
	}
	if fetch.Ranged == 0 {
		t.Fatal("no sample was by reference")
	}

	// It signs for no other advertiser, and never as the advertiser.
	_, bad := request(me, 2)
	if call, err = r.writer.Challenge(ctx, front, bad, put.C, 1000); err != nil || call.Status != 422 {
		t.Fatalf("a request naming the front as provider: %v %+v", err, call)
	}

	// An incumbent whose ranges are wrong: the front rereads whole blobs.
	fb.mu.Lock()
	fb.rangeWrong = true
	fb.mu.Unlock()
	q, raw = request(fb.key(), 3)
	if call, err = r.writer.Challenge(ctx, front, raw, put.C, 1000); err != nil || call.Status != 200 {
		t.Fatalf("challenge with wrong ranges: %v %+v\n%s", err, call, r.said())
	}
	if v := testwriter.Verify(q, raw, call.Body, put.Object, fetch); v.Outcome != storerec.OutcomePass {
		t.Fatalf("verdict with wrong ranges %+v", v)
	}
	fb.mu.Lock()
	wrong := fb.wrong
	fb.rangeWrong = false
	fb.mu.Unlock()
	if wrong == 0 {
		t.Fatal("the backend was not told of its wrong ranges")
	}

	// No object routes and no remittances at a front.
	if res, err := r.client.Get("https://" + p.Addr() + "/store" + storerec.CHIRPURLSuffix(root)); err != nil || res.StatusCode != 404 {
		t.Fatalf("object route at a front: %v %v", err, res)
	}
	s, err := r.writer.Settle(ctx, put, []testwriter.Payee{{Identity: fb.key(), Rank: 1, Sats: 5000}})
	if err != nil {
		t.Fatal(err)
	}
	if call, err = r.writer.Post(ctx, front+"/settle", s.Remittances[1], 0); err != nil || call.Status != 400 {
		t.Fatalf("remittance at a front: %v %+v", err, call)
	}
	// The settlement is found and forwarded for the incumbent.
	r.eventually("the settlement forwarded", 20*time.Second, func() bool {
		fmu.Lock()
		defer fmu.Unlock()
		return len(forwarded) == 1
	})
	fw := forwarded[0]
	cHash, _ := displayHash(put.C)
	if fw.Txid != s.Tx.TxID().String() || fw.DerivationPrefix != storerec.DerivationPrefix(cHash) || fw.K != 2 ||
		fw.Writer != r.writer.Signer.IdentityHex() {
		t.Fatalf("forwarded %+v", fw)
	}
	if _, err := transaction.NewTransactionFromBEEF(fw.BEEF); err != nil {
		t.Fatalf("forwarded BEEF: %v", err)
	}

	// The incumbent loses its blobs: caught, nothing signed, the root
	// damaged here.
	fb.mu.Lock()
	for _, b := range put.Object.Built.Objects() {
		if !isNode(b) {
			fb.lost[sha256.Sum256(b)] = true
		}
	}
	fb.mu.Unlock()
	_, raw = request(fb.key(), 4)
	if call, err = r.writer.Challenge(ctx, front, raw, put.C, 1000); err != nil || call.Status != 422 || !bytes.Contains(call.Body, []byte("unknown-root")) {
		t.Fatalf("challenge after the incumbent lost the blobs: %v %+v", err, call)
	}
	p.mu.Lock()
	damaged := p.st.Holdings[hex.EncodeToString(root[:])].Damaged
	p.mu.Unlock()
	if !damaged {
		t.Fatal("the root is not marked damaged")
	}

	// The incumbent serves them again (a restart, a proxy's passing 404):
	// the front looks again, reads them, and answers again.
	savedRecheck := recheckEvery
	t.Cleanup(func() { recheckEvery = savedRecheck })
	recheckEvery = 100 * time.Millisecond
	fb.mu.Lock()
	fb.lost = map[[32]byte]bool{}
	fb.mu.Unlock()
	r.eventually("the damaged root looked at again", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return !p.st.Holdings[hex.EncodeToString(root[:])].Damaged
	})
	q, raw = request(fb.key(), 5)
	if call, err = r.writer.Challenge(ctx, front, raw, put.C, 1000); err != nil || call.Status != 200 {
		t.Fatalf("challenge after the incumbent serves again: %v %+v", err, call)
	}

	// A passing failure to reach the incumbent marks nothing.
	fb.mu.Lock()
	fb.down = true
	fb.mu.Unlock()
	_, raw = request(fb.key(), 6)
	if call, err = r.writer.Challenge(ctx, front, raw, put.C, 1000); err != nil || call.Status != 503 {
		t.Fatalf("challenge with the incumbent unreachable: %v %+v", err, call)
	}
	p.mu.Lock()
	damaged = p.st.Holdings[hex.EncodeToString(root[:])].Damaged
	p.mu.Unlock()
	if damaged {
		t.Fatal("a passing failure marked the root damaged")
	}
}

func TestFrontRefusesAnotherAdvertisersAdvertisement(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	fb := newFakeBridge(t, r)
	fb.otherKey = true
	cfg := r.config(filepath.Join(t.TempDir(), "front"))
	cfg.Backend = "bridge"
	p := r.startWith(cfg, false, Options{Front: &Front{Backend: fb, Advertiser: fb.key()}})
	put := r.put(ctx, []*Provider{p}, content(9000, 9), "photos/x.bin", testwriter.Terms{Sats: 5000, T: 1, K: 1, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	r.eventually("the foreign advertisement refused", 30*time.Second, func() bool {
		return strings.Contains(r.said(), "not by the advertiser this front answers for")
	})
	if p.liveAd(put.Object.Built.RootHash) != nil {
		t.Fatal("an advertisement by another key was adopted")
	}
}

func TestFrontConfiguration(t *testing.T) {
	r := newRig(t)
	cfg := r.config(filepath.Join(t.TempDir(), "front"))
	cfg.Backend = "bridge"
	inline := true
	cfg.Policy.AnswerInline = &inline
	if err := cfg.Check(); err == nil {
		t.Fatal("a front answering inline was accepted")
	}
	cfg.Policy.AnswerInline = nil
	if err := cfg.Check(); err != nil {
		t.Fatal(err)
	}
	if _, err := Init(cfg.Data); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), cfg, Options{}); err == nil {
		t.Fatal("backend bridge without a front was accepted")
	}
	native := r.config(filepath.Join(t.TempDir(), "native"))
	_ = native.Check()
	if _, err := Init(native.Data); err != nil {
		t.Fatal(err)
	}
	fb := newFakeBridge(t, r)
	if _, err := New(context.Background(), native, Options{Front: &Front{Backend: fb, Advertiser: fb.key()}}); err == nil {
		t.Fatal("a front on the native backend was accepted")
	}
}

func mintAd(lock *script.Script, fee mint.Input, change *script.Script, w *testwriter.Writer) (*transaction.Transaction, error) {
	return mint.Transition(lock, 1, nil, fee, change, w.Fees)
}
