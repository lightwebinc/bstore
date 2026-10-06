package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

func fundProvider(ctx context.Context, p *Provider, rpc *nodeapi.RPC, asset *nodeapi.Asset) (int, []string, error) {
	return bwallet.FundFromCoinbase(ctx, p.signer, p.pool, rpc, asset, 104, 0)
}

// content is n bytes no two runs differ in.
func content(n int, seed byte) []byte {
	out := make([]byte, 0, n+32)
	h := sha256.Sum256([]byte{seed})
	for len(out) < n {
		out = append(out, h[:]...)
		h = sha256.Sum256(h[:])
	}
	return out[:n]
}

func (r *rig) put(ctx context.Context, ps []*Provider, data []byte, key string, terms testwriter.Terms) *testwriter.Put {
	r.t.Helper()
	w := r.writer
	o, err := w.Build(ctx, data)
	if err != nil {
		r.t.Fatal(err)
	}
	put, err := w.Entry(ctx, o, key, terms)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, p := range ps {
		base := "https://" + p.Addr()
		ans, err := w.Submit(ctx, base, storerec.RecordsTopic(testBucket), put.BEEF)
		if err != nil {
			r.t.Fatal(err)
		}
		if ok, why := testwriter.Admitted(ans, storerec.RecordsTopic(testBucket)); !ok {
			r.t.Fatalf("entry not taken: %s", why)
		}
		for _, pc := range o.Pieces {
			ans, err := w.Submit(ctx, base, storerec.BlobTopic(testBucket), pc)
			if err != nil {
				r.t.Fatal(err)
			}
			if ok, why := testwriter.Admitted(ans, storerec.BlobTopic(testBucket)); !ok {
				r.t.Fatalf("piece not taken: %s", why)
			}
		}
	}
	return put
}

func (p *Provider) liveAd(root [32]byte) *AdRec {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.st.Holdings[hex.EncodeToString(root[:])]
	if h == nil || h.Ad == nil {
		return nil
	}
	c := *h.Ad
	return &c
}

func TestProviderFlow(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, true)
	front := cfg.FrontURL()

	data := content(2*storerec.BlobSize+12345, 1)
	put := r.put(ctx, []*Provider{p}, data, "photos/one.jpg", testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	root := put.Object.Built.RootHash
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(root) != nil })
	ad := p.liveAd(root)
	if ad.Expiry < put.Entry.Created/1000+86400 {
		t.Fatalf("expiry %d too short", ad.Expiry)
	}

	// The object route serves the closure, with Range.
	adv, err := advOf(ad)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.client.Get(adv.URL)
	if err != nil {
		t.Fatal(err)
	}
	rootBytes := readAll(t, res)
	if sha256.Sum256(rootBytes) != root {
		t.Fatal("the root route served other bytes")
	}

	// The free settlement challenge passes.
	me := p.signer.Identity.Compressed()
	q, raw, err := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	call, err := r.writer.Challenge(ctx, front, raw, put.C, 0)
	if err != nil {
		t.Fatal(err)
	}
	if call.Status != 200 {
		t.Fatalf("free challenge: %d %s\n%s", call.Status, call.Body, r.said())
	}
	v := testwriter.Verify(q, raw, call.Body, put.Object, &testwriter.Fetcher{HTTP: r.client, URL: adv.URL})
	if v.Outcome != storerec.OutcomePass {
		t.Fatalf("verdict %+v", v)
	}
	// A second free one, then the third is priced.
	_, raw2, _ := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{2})
	if call, err = r.writer.Challenge(ctx, front, raw2, put.C, 0); err != nil || call.Status != 200 {
		t.Fatalf("second free: %v %+v", err, call)
	}
	_, raw3, _ := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{3})
	if call, err = r.writer.Challenge(ctx, front, raw3, put.C, 0); err == nil || !strings.Contains(err.Error(), "40 satoshis asked") {
		t.Fatalf("third free: %v %+v", err, call)
	}
	// Paid, it is answered.
	if call, err = r.writer.Challenge(ctx, front, raw3, put.C, 1000); err != nil || call.Status != 200 {
		t.Fatalf("paid: %v %+v %s", err, call, r.said())
	}

	// Seed outside the window, and a root not held.
	stale := *q
	stale.SeedHeight -= 7
	h, _ := r.writer.Asset.HashAtHeight(ctx, uint32(stale.SeedHeight))
	sh, _ := displayHash(h)
	stale.SeedHash = sh
	sraw, _ := stale.Encode()
	if call, err = r.writer.Challenge(ctx, front, sraw, put.C, 1000); err != nil || call.Status != 422 || !bytes.Contains(call.Body, []byte("seed")) {
		t.Fatalf("stale seed: %v %+v", err, call)
	}
	other := *q
	other.ProofRoot[0] ^= 1
	oraw, _ := other.Encode()
	if call, err = r.writer.Challenge(ctx, front, oraw, put.C, 1000); err != nil || call.Status != 422 || !bytes.Contains(call.Body, []byte("unknown-root")) {
		t.Fatalf("unknown root: %v %+v", err, call)
	}

	// Renewal: paid, the successor spends the advertisement.
	body, _ := json.Marshal(map[string]any{"root": hex.EncodeToString(root[:]), "until": ad.Expiry + 86400})
	call, err = r.writer.Post(ctx, front+"/renew", body, 100000)
	if err != nil || call.Status != 200 {
		t.Fatalf("renew: %v %+v\n%s", err, call, r.said())
	}
	r.eventually("renewed", 10*time.Second, func() bool { a := p.liveAd(root); return a != nil && a.Expiry == ad.Expiry+86400 })
	ad2 := p.liveAd(root)
	if ad2.First != ad.Txid {
		t.Fatalf("the renewal keeps the original: %s %s", ad2.First, ad.Txid)
	}

	// Settlement: rank 1 pays this provider.
	s, err := r.writer.Settle(ctx, put, []testwriter.Payee{{Identity: me, Rank: 1, Sats: 5000}})
	if err != nil {
		t.Fatal(err)
	}
	before := p.pool.Balance()
	call, err = r.writer.Post(ctx, front+"/settle", s.Remittances[1], 0)
	if err != nil || call.Status != 200 {
		t.Fatalf("settle: %v %+v", err, call)
	}
	// A remittance for a rank the output is not derived for is refused.
	var rm map[string]any
	_ = json.Unmarshal(s.Remittances[1], &rm)
	rm["rank"], rm["derivationSuffix"] = 2, storerec.DerivationSuffix(2)
	bad, _ := json.Marshal(rm)
	if call, err = r.writer.Post(ctx, front+"/settle", bad, 0); err != nil || call.Status != 400 {
		t.Fatalf("wrong rank: %v %+v", err, call)
	}
	r.eventually("payout pooled", 20*time.Second, func() bool { return p.pool.Balance() >= before+5000 })
	_ = base64.StdEncoding
}

func readAll(t *testing.T, res *http.Response) []byte {
	t.Helper()
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func advOf(a *AdRec) (*storerec.Advertisement, error) {
	raw, err := hex.DecodeString(a.RawHex)
	if err != nil {
		return nil, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, err
	}
	return storerec.DecodeAdvertisement(*tx.Outputs[a.Vout].LockingScript)
}
