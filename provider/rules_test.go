package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	authhttp "github.com/bsv-blockchain/go-sdk/auth/clients/authhttp"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"
	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/payee"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

var bounty = testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30}

// held is a started provider holding and advertising one object.
func held(t *testing.T, edit func(*Config), size int, terms testwriter.Terms) (*rig, *Provider, *Config, *testwriter.Put) {
	t.Helper()
	r := newRig(t)
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	if edit != nil {
		edit(cfg)
	}
	p := r.start(cfg, true)
	put := r.put(context.Background(), []*Provider{p}, content(size, 7), "k", terms)
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(put.Object.Built.RootHash) != nil })
	return r, p, cfg, put
}

func priced(err error) bool { return err != nil && strings.Contains(err.Error(), "satoshis asked") }

func TestUpperPathIsRFC6962(t *testing.T) {
	for n := 1; n <= 70; n++ {
		roots := make([][32]byte, n)
		for i := range roots {
			roots[i] = sha256.Sum256([]byte{byte(i), byte(n)})
		}
		ls := buildLevels(roots)
		want, _ := commit.RootOfSubtrees(roots)
		if ls[len(ls)-1][0] != want {
			t.Fatalf("n=%d: root", n)
		}
		for b := range n {
			p, _ := commit.ProveSubtrees(roots, b)
			got := ls.path(uint64(b))
			if fmt.Sprint(got) != fmt.Sprint(p.Siblings()) {
				t.Fatalf("n=%d b=%d: path differs", n, b)
			}
		}
	}
}

func TestRenewPrice(t *testing.T) {
	gib := uint64(1 << 30)
	for _, c := range []struct{ size, from, until, per, min, want uint64 }{
		{gib, 0, 86400, 100, 1, 100},
		{gib / 2, 0, 86400, 100, 1, 50},
		{1, 0, 1, 100, 7, 7},
		{gib, 0, 86401, 100, 1, 101},
		{gib, 100, 50, 100, 9, 9},
	} {
		if got := renewPrice(c.size, c.from, c.until, c.per, c.min); got != c.want {
			t.Errorf("%+v: %d", c, got)
		}
	}
}

func TestConfigRefusals(t *testing.T) {
	good := func() *Config {
		return &Config{Data: "/var/lib/x", PublicURL: "https://provider.example/store", Node: "http://n", Settle: "rpc:http://n/rpc",
			Headers: "http://h", Buckets: []BucketConfig{{Bucket: testBucket}}}
	}
	if err := good().Check(); err != nil {
		t.Fatal(err)
	}
	// No node: WhatsOnChain, its headers and GorillaPool's arcade.
	bare := good()
	bare.Node, bare.Settle, bare.Headers = "", "", ""
	if err := bare.Check(); err != nil {
		t.Fatalf("the zero-infrastructure profile: %v", err)
	}
	if s := bare.Spec(); s.Network != "test" {
		t.Fatalf("network %q", s.Network)
	}
	// Named in the configuration itself, for a module that embeds it.
	if bare.Chain != "woc:test" || bare.Settle != "arcade:test" || bare.Headers != "woc:test" {
		t.Fatalf("defaults: chain %q settle %q headers %q", bare.Chain, bare.Settle, bare.Headers)
	}
	main := good()
	main.Node, main.Settle, main.Headers, main.Mainnet = "", "", "", true
	if err := main.Check(); err != nil || main.Chain != "woc:main" || main.Settle != "arcade:main" || main.Headers != "woc:main" {
		t.Fatalf("mainnet defaults: %v: chain %q settle %q headers %q", err, main.Chain, main.Settle, main.Headers)
	}
	// Four submissions or uploads in flight from one address unless set.
	for set, want := range map[int]int{0: 4, 32: 32} {
		c := good()
		c.Limits.InFlightPerAddr = set
		if err := c.Check(); err != nil {
			t.Fatal(err)
		}
		l, err := newRouteLimits(c, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if c.Limits.InFlightPerAddr != want || l.inflight.max != want {
			t.Fatalf("in_flight_per_addr %d: %d, routes %d", set, c.Limits.InFlightPerAddr, l.inflight.max)
		}
	}
	// A node key stays the chain view: chain is not filled beside it.
	if n := good(); n.Check() != nil || n.Chain != "" {
		t.Fatalf("node: chain %q", n.Chain)
	}
	for name, edit := range map[string]func(*Config){
		"relative data":  func(c *Config) { c.Data = "x" },
		"http public":    func(c *Config) { c.PublicURL = "http://provider.example" },
		"trailing slash": func(c *Config) { c.PublicURL = "https://provider.example/" },
		"query":          func(c *Config) { c.PublicURL = "https://provider.example/?a" },
		"user":           func(c *Config) { c.PublicURL = "https://u@provider.example" },
		"chirp path":     func(c *Config) { c.PublicURL = "https://provider.example/chirp/v1/x" },
		"freeC":          func(c *Config) { c.Terms.FreeC = 29 },
		"minAge":         func(c *Config) { c.Terms.MinAge = 1009 },
		"bucket":         func(c *Config) { c.Buckets[0].Bucket = "Bad" },
		"writer":         func(c *Config) { c.Buckets[0].Writers = []string{"02" + strings.Repeat("ff", 32)} },
		"settle":         func(c *Config) { c.Settle = "ftp:x" },
		"chain and node": func(c *Config) { c.Chain = "woc:test" },
		"chain":          func(c *Config) { c.Node, c.Chain = "", "junglebus:x" },
		"fee twice":      func(c *Config) { r := mint.Rate{Sats: 1, Bytes: 1}; c.FeeRate, c.Fee = 1, &feepolicy.Config{Rate: &r} },
		"fee source":     func(c *Config) { c.Fee = &feepolicy.Config{Source: "arc"} },
		"floor":          func(c *Config) { c.FeeFloor = 600 },
		"backend":        func(c *Config) { c.Backend = "s3" },
		"unclaimed":      func(c *Config) { c.QuotaBytes, c.UnclaimedBytes = 10, 11 },
		"writer share":   func(c *Config) { c.UnclaimedBytes, c.Policy.MaxObject = 4<<20, 2<<20 },
		"in flight":      func(c *Config) { c.Limits.InFlightPerAddr = -1 },
		"verify workers": func(c *Config) { c.Limits.VerifyWorkers = maxVerifyWorkers + 1 },
		"no verifiers":   func(c *Config) { c.Limits.VerifyWorkers = -1 },
	} {
		c := good()
		edit(c)
		if c.Check() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	p := filepath.Join(t.TempDir(), "c.json")
	_ = os.WriteFile(p, []byte(`{"data":"/x","public_url":"https://a.example","node":"http://n","settle":"rpc:x","headers":"http://h","buckets":[{"bucket":"`+testBucket+`"}],"quota":1}`), 0o600)
	if _, err := LoadConfig(p); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("a misspelt key: %v", err)
	}
}

// The free settlement challenge, exactly as frozen (spec section 9.4).
func TestFreeChallengeRules(t *testing.T) {
	r, p, cfg, put := held(t, nil, storerec.BlobSize+100, bounty)
	ctx := context.Background()
	front := cfg.FrontURL()
	me := p.signer.Identity.Compressed()
	b := put.Object.Built
	n := byte(0)
	ask := func(w *testwriter.Writer, c uint64, entry string) error {
		n++
		_, raw, err := w.Request(ctx, b, me, c, [16]byte{n})
		if err != nil {
			t.Fatal(err)
		}
		call, err := w.Challenge(ctx, front, raw, entry, 0)
		if err == nil && call.Status != 200 {
			return fmt.Errorf("status %d %s", call.Status, call.Body)
		}
		return err
	}
	// Not the entry's writer: priced.
	hc, _ := headers.NewSource(r.srv.URL)
	other, err := testwriter.New(ctx, t.TempDir(), testBucket, &nodeapi.RPC{URL: r.srv.URL + "/rpc", ID: "o"}, &nodeapi.Asset{Base: r.srv.URL}, hc, r.client)
	if err != nil {
		t.Fatal(err)
	}
	if err := ask(other, 30, put.C); !priced(err) {
		t.Fatalf("another identity: %v", err)
	}
	// No entry named, or c above min(checkC, freeC): priced.
	if err := ask(r.writer, 30, ""); !priced(err) {
		t.Fatalf("no entry: %v", err)
	}
	if err := ask(r.writer, 31, put.C); !priced(err) {
		t.Fatalf("c 31: %v", err)
	}
	// Not deep enough for minAge: priced; then deep enough.
	p.cfg.Terms.MinAge = 1008
	if err := ask(r.writer, 30, put.C); !priced(err) {
		t.Fatalf("minAge: %v", err)
	}
	p.cfg.Terms.MinAge = 1
	// Below the minimum bounty: priced.
	p.cfg.Terms.MinBounty = 5001
	if err := ask(r.writer, 30, put.C); !priced(err) {
		t.Fatalf("min bounty: %v", err)
	}
	p.cfg.Terms.MinBounty = 1000
	// A second entry naming the same hold output shares its two.
	put2, err := r.writer.Entry(ctx, put.Object, "k2", testwriter.Terms{T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30, ReuseHold: put.Hold})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.writer.Submit(ctx, "https://"+p.Addr(), storerec.RecordsTopic(testBucket), put2.BEEF); err != nil {
		t.Fatal(err)
	}
	r.eventually("the second entry declined", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		e := p.st.Entries[put2.C]
		return e != nil && strings.Contains(e.Decision, "already backs")
	})
	if err := ask(r.writer, 30, put.C); err != nil {
		t.Fatalf("first free: %v", err)
	}
	// An entry this provider declined is owed nothing, whatever hold it names.
	if err := ask(r.writer, 30, put2.C); !priced(err) {
		t.Fatalf("by the declined entry: %v", err)
	}
	if err := ask(r.writer, 30, put.C); err != nil {
		t.Fatalf("second free: %v", err)
	}
	if err := ask(r.writer, 30, put.C); !priced(err) {
		t.Fatalf("third free: %v", err)
	}
	p.mu.Lock()
	if p.st.Free[put.Hold.Tx.TxID().String()+".0"] != 2 {
		t.Fatalf("free count %v", p.st.Free)
	}
	p.mu.Unlock()

	// A spent hold output owes nothing: a fresh entry and hold, spent.
	put3 := r.put(ctx, []*Provider{p}, content(storerec.BlobSize+100, 7), "k3", bounty)
	r.eventually("third entry held", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.st.Entries[put3.C] != nil && p.st.Entries[put3.C].Decision == decisionHeld
	})
	if _, err := r.writer.Settle(ctx, put3, []testwriter.Payee{{Identity: me, Rank: 1, Sats: 5000}}); err != nil {
		t.Fatal(err)
	}
	if err := ask(r.writer, 30, put3.C); !priced(err) {
		t.Fatalf("spent hold: %v", err)
	}
	// Past the deadline owes nothing.
	put4 := r.put(ctx, []*Provider{p}, content(storerec.BlobSize+100, 7), "k4", testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 30, CheckC: 30})
	r.eventually("fourth entry held", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.st.Entries[put4.C] != nil && p.st.Entries[put4.C].Decision == decisionHeld
	})
	addr, _ := r.writer.Signer.FundAddress(false)
	if _, err := r.writer.RPC.GenerateToAddress(ctx, 40, addr); err != nil {
		t.Fatal(err)
	}
	if err := ask(r.writer, 30, put4.C); !priced(err) {
		t.Fatalf("past the deadline: %v", err)
	}
}

// A buyer is answered at most its budget of challenges.
func TestChallengeRatePerBuyer(t *testing.T) {
	r, p, cfg, put := held(t, func(c *Config) { c.Limits.ChallengeBurst = 2; c.Limits.ChallengesPerBuyerPerMin = 1 }, 5000, bounty)
	ctx := context.Background()
	me := p.signer.Identity.Compressed()
	var last *testwriter.Call
	for i := range 3 {
		_, raw, _ := r.writer.Request(ctx, put.Object.Built, me, 10, [16]byte{byte(i)})
		call, err := r.writer.Challenge(ctx, cfg.FrontURL(), raw, put.C, 1000)
		if err != nil {
			t.Fatal(err)
		}
		last = call
	}
	if last.Status != 429 {
		t.Fatalf("third: %d %s", last.Status, last.Body)
	}
}

// A dropped blob, caught: the provider refuses to sign for a root it no
// longer holds whole, and stops serving it.
func TestDroppedBlob(t *testing.T) {
	r, p, cfg, put := held(t, nil, 3*storerec.BlobSize, bounty)
	ctx := context.Background()
	me := p.signer.Identity.Compressed()
	c, _ := storerec.VerifyCHIRP(put.Object.Built.RootHash, objectsOf(put.Object))
	lost := c.Blobs[1].Hash
	if err := p.store.Delete(lost); err != nil {
		t.Fatal(err)
	}
	q, _, _ := r.writer.Request(ctx, put.Object.Built, me, 20, [16]byte{})
	for nonce := 0; ; nonce++ {
		q.Nonce = [16]byte{byte(nonce), byte(nonce >> 8)}
		hit := false
		for k := range uint32(20) {
			hit = hit || q.Index(k)/storerec.SegmentsPerBlob == 1
		}
		if hit {
			break
		}
	}
	raw, _ := q.Encode()
	call, err := r.writer.Challenge(ctx, cfg.FrontURL(), raw, put.C, 100000)
	if err != nil || call.Status != 422 || !bytes.Contains(call.Body, []byte("unknown-root")) {
		t.Fatalf("%v %+v", err, call)
	}
	ad, _ := advOf(p.liveAd(put.Object.Built.RootHash))
	res, err := r.client.Get(ad.URL)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("a damaged root served: %d", res.StatusCode)
	}
	// No payment was taken for the refused answer.
	if n := len(p.ledger.payments()); n != 0 {
		t.Fatalf("%d payments taken", n)
	}
}

func objectsOf(o *testwriter.Object) chirp.Fetch {
	return func(h [32]byte) ([]byte, error) {
		for _, x := range o.Built.Objects() {
			if sha256.Sum256(x) == h {
				return x, nil
			}
		}
		return nil, fmt.Errorf("none")
	}
}

// Answered by reference, the buyer fetches each sample from the advertised
// URL with Range, and the provider's own Range answers are right.
func TestByReferenceAndRange(t *testing.T) {
	inline := false
	export := filepath.Join(t.TempDir(), "export")
	t.Cleanup(func() {
		_ = filepath.Walk(export, func(p string, fi os.FileInfo, err error) error {
			if err == nil && fi.IsDir() {
				_ = os.Chmod(p, 0o755)
			}
			return nil
		})
	})
	r, p, cfg, put := held(t, func(c *Config) { c.Policy.AnswerInline = &inline; c.Export = export }, 2*storerec.BlobSize+5000, bounty)
	// The read-only export holds the closure, the root first.
	rootHex := hex.EncodeToString(put.Object.Built.RootHash[:])
	if b, err := os.ReadFile(filepath.Join(export, rootHex, "0")); err != nil || sha256.Sum256(b) != put.Object.Built.RootHash {
		t.Fatalf("export: %v", err)
	}
	if err := os.WriteFile(filepath.Join(export, rootHex, "new"), nil, 0o644); err == nil && os.Getuid() != 0 {
		t.Fatal("the export is writable")
	}
	ctx := context.Background()
	me := p.signer.Identity.Compressed()
	ad, _ := advOf(p.liveAd(put.Object.Built.RootHash))
	q, raw, _ := r.writer.Request(ctx, put.Object.Built, me, 30, [16]byte{5})
	call, err := r.writer.Challenge(ctx, cfg.FrontURL(), raw, put.C, 0)
	if err != nil || call.Status != 200 {
		t.Fatalf("%v %+v", err, call)
	}
	f := &testwriter.Fetcher{HTTP: r.client, URL: ad.URL}
	v := testwriter.Verify(q, raw, call.Body, put.Object, f)
	if v.Outcome != storerec.OutcomePass || len(v.Rechecked) != 0 || f.Ranged != 30 || f.Whole != 0 {
		t.Fatalf("verdict %+v ranged %d whole %d", v, f.Ranged, f.Whole)
	}
	// A Range of the last blob's tail.
	c, _ := storerec.VerifyCHIRP(put.Object.Built.RootHash, objectsOf(put.Object))
	last := c.Blobs[2].Hash
	u := ad.URL[:strings.LastIndex(ad.URL, "/")+1] + chirp.Identifier(last)
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.Header.Set("Range", "bytes=4990-")
	res, err := r.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 206 || len(b) != 10 || !bytes.Equal(b, put.Object.Content[2*storerec.BlobSize+4990:]) {
		t.Fatalf("range: %d %d", res.StatusCode, len(b))
	}
	// An object outside the closure, a bad identifier, a traversal: 404.
	for _, path := range []string{
		strings.Replace(u, chirp.Identifier(last), chirp.Identifier(sha256.Sum256([]byte("x"))), 1),
		strings.Replace(u, chirp.Identifier(last), "..%2F..%2Fstate.json", 1),
		cfg.PublicURL + "/chirp/v1/../../state.json",
		cfg.PublicURL + "/chirp/v1/" + hex.EncodeToString(last[:]) + "/objects/" + hex.EncodeToString(last[:]),
	} {
		res, err := r.client.Get(path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode == 200 || bytes.Contains(body, []byte("identity")) {
			t.Fatalf("%s: %d", path, res.StatusCode)
		}
	}
	// The terms.
	res, err = r.client.Get(cfg.FrontURL() + "/terms")
	if err != nil {
		t.Fatal(err)
	}
	var terms map[string]any
	_ = json.NewDecoder(res.Body).Decode(&terms)
	res.Body.Close()
	if terms["service"] != "bstore" || terms["challenge"].(map[string]any)["freeC"].(float64) != 30 {
		t.Fatalf("terms %v", terms)
	}
}

// A closure the writer got wrong is never held: a root with an extension
// (rows C2 and C4), and a proof root the content does not have.
func TestWritersFaultsDeclined(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, true)
	data := content(9000, 3)
	built, err := chirp.Build(data, []chirp.Extension{{Type: 1, Value: []byte("text/plain")}})
	if err != nil {
		t.Fatal(err)
	}
	sb, _ := storerec.Build(data)
	ext := &storerec.Built{Built: built, ContentHash: sb.ContentHash, ProofRoot: sb.ProofRoot, Size: sb.Size}
	objs := ext.Objects()
	tree, err := r.writer.Tree(ctx, storerec.PieceDerivation, len(objs))
	if err != nil {
		t.Fatal(err)
	}
	base := "https://" + p.Addr()
	for i, ob := range objs {
		pc := &storerec.Piece{Bucket: testBucket, Writer: r.writer.Identity(), Root: built.RootHash, Piece: ob}
		rec, _ := pc.Encode()
		beef, _, err := r.writer.Carrier(ctx, storerec.PieceDerivation, rec, tree, uint32(i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.writer.Submit(ctx, base, storerec.BlobTopic(testBucket), beef); err != nil {
			t.Fatal(err)
		}
	}
	o := &testwriter.Object{Content: data, Built: ext}
	put, err := r.writer.Entry(ctx, o, "ext", bounty)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.writer.Submit(ctx, base, storerec.RecordsTopic(testBucket), put.BEEF); err != nil {
		t.Fatal(err)
	}
	// The proof root wrong: the same content as a bstore root.
	o2, _ := r.writer.Build(ctx, data)
	wrong := o2.Built.ProofRoot
	wrong[0] ^= 1
	put2, err := r.writer.Entry(ctx, o2, "pr", testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30, ProofRootOverride: &wrong})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.writer.Submit(ctx, base, storerec.RecordsTopic(testBucket), put2.BEEF); err != nil {
		t.Fatal(err)
	}
	for _, pc := range o2.Pieces {
		if _, err := r.writer.Submit(ctx, base, storerec.BlobTopic(testBucket), pc); err != nil {
			t.Fatal(err)
		}
	}
	decision := func(c string) string {
		p.mu.Lock()
		defer p.mu.Unlock()
		if e := p.st.Entries[c]; e != nil {
			return e.Decision
		}
		return ""
	}
	r.eventually("both declined", 10*time.Second, func() bool { return decision(put.C) != "" && decision(put2.C) != "" })
	if !strings.Contains(decision(put.C), "extension") || !strings.Contains(decision(put2.C), "proof-root") {
		t.Fatalf("decisions %q %q", decision(put.C), decision(put2.C))
	}
	if p.liveAd(built.RootHash) != nil || p.liveAd(o2.Built.RootHash) != nil {
		t.Fatal("advertised a closure it refused")
	}
}

// Unicast through BRC-167's upload sessions.
func TestUploadSessions(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, true)
	o, err := r.writer.Build(ctx, content(storerec.BlobSize+10, 4))
	if err != nil {
		t.Fatal(err)
	}
	af := authhttp.New(r.writer.W, authhttp.WithoutLogging(), authhttp.WithHttpClient(r.client))
	call := func(method, url string, body []byte, ct string) (int, []byte) {
		res, err := af.Fetch(ctx, url, &authhttp.SimplifiedFetchRequestOptions{Method: method, Headers: map[string]string{"content-type": ct}, Body: body})
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, b
	}
	up := cfg.PublicURL + "/chirp/v1/uploads"
	// Unauthenticated: refused.
	res, err := r.client.Post(up, "application/json", strings.NewReader(`{"retentionSeconds":"60","logicalLength":null}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatalf("unauthenticated session: %d", res.StatusCode)
	}
	st, b := call("POST", up, []byte(`{"retentionSeconds":"60","logicalLength":null}`), "application/json")
	if st != 201 {
		t.Fatalf("open: %d %s", st, b)
	}
	var s struct {
		UploadID string `json:"uploadId"`
	}
	_ = json.Unmarshal(b, &s)
	objs := o.Built.Objects()
	if st, b := call("PUT", up+"/"+s.UploadID+"/objects/"+chirp.Identifier(sha256.Sum256(objs[0])), []byte("not it"), "application/octet-stream"); st != 422 {
		t.Fatalf("wrong digest: %d %s", st, b)
	}
	for _, ob := range objs {
		if st, b := call("PUT", up+"/"+s.UploadID+"/objects/"+chirp.Identifier(sha256.Sum256(ob)), ob, "application/octet-stream"); st != 201 {
			t.Fatalf("put: %d %s", st, b)
		}
	}
	commitBody := []byte(`{"rootIdentifier":"` + chirp.Identifier(o.Built.RootHash) + `"}`)
	if st, b := call("POST", up+"/"+s.UploadID+"/commit", commitBody, "application/json"); st != 202 {
		t.Fatalf("commit before the entry: %d %s", st, b)
	}
	put, err := r.writer.Entry(ctx, o, "u", bounty)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.writer.Submit(ctx, "https://"+p.Addr(), storerec.RecordsTopic(testBucket), put.BEEF); err != nil {
		t.Fatal(err)
	}
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(o.Built.RootHash) != nil })
	st, b = call("POST", up+"/"+s.UploadID+"/commit", commitBody, "application/json")
	if st != 201 || !bytes.Contains(b, []byte("hostedFileLocation")) {
		t.Fatalf("commit after: %d %s", st, b)
	}
}

// The default price is a number people compare by the GB: docs/limits.md and
// the published comparisons are arithmetic on it.
func TestDefaultPrice(t *testing.T) {
	c := &Config{}
	c.defaults()
	if got := c.Terms.RenewPerGiBDay; got != 250 {
		t.Fatalf("default price %d satoshis per GiB-day", got)
	}
	if got := 3 * renewPrice(1<<30, 0, 30*86400, c.Terms.RenewPerGiBDay, 0); got != 22500 {
		t.Fatalf("three copies of 1 GiB for 30 days at the default price: %d satoshis", got)
	}
}

// A bounty pays the provider's price for the object's size and retention,
// from an even share among the paid ranks.
func TestBountyShort(t *testing.T) {
	const gib, week = 1 << 30, 7 * 86400
	for _, c := range []struct {
		size, sats, k, retention, perGiBDay, price uint64
		short                                      bool
	}{
		{gib, 2100, 3, week, 100, 700, false},
		{gib, 2099, 3, week, 100, 700, true},
		{gib, 1000, 3, week, 100, 700, true},
		{gib, 700, 1, week, 100, 700, false},
		{gib, 9000, 3, 30 * 86400, 100, 3000, false},
		{100 << 20, 1000, 3, week, 100, 69, false},
		{1000, 1000, 3, week, 100, 1, false},
	} {
		price, short := bountyShort(c.size, &HoldRec{Sats: c.sats, K: c.k, Retention: c.retention}, c.perGiBDay)
		if price != c.price || short != c.short {
			t.Errorf("%+v: price %d, short %v", c, price, short)
		}
	}
}

// A provider declines a hold below its price, and takes one at it.
func TestProviderDeclinesAnUnderpricedHold(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	// 1 MiB for a day at this price is 2,000 satoshis.
	cfg.Terms.RenewPerGiBDay = 2048000
	p := r.start(cfg, true)
	terms := func(sats uint64) testwriter.Terms {
		return testwriter.Terms{Sats: sats, T: 1, K: 1, Retention: 86400, DeadlineAhead: 200, CheckC: 30}
	}
	low := r.put(ctx, []*Provider{p}, content(1<<20, 3), "low", terms(1999))
	r.eventually("declined", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		e := p.st.Entries[low.C]
		return e != nil && strings.Contains(e.Decision, "price of 2000")
	})
	at := r.put(ctx, []*Provider{p}, content(1<<20, 4), "at", terms(2000))
	r.eventually("held", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		e := p.st.Entries[at.C]
		return e != nil && e.Decision == decisionHeld
	})
}

// The pieces of an object the provider declined give way at once: on the plane
// they arrive whether it wants them or not, and they would otherwise keep the
// writer's next object out for their whole time.
func TestDeclinedPiecesGiveWay(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.QuotaBytes, cfg.UnclaimedBytes, cfg.Policy.MaxObject = 256<<20, 32<<20, 8<<20
	// 8 MiB for a day at this price is 10,000 satoshis.
	cfg.Terms.RenewPerGiBDay = 1280000
	p := r.start(cfg, true)
	terms := func(sats uint64) testwriter.Terms {
		return testwriter.Terms{Sats: sats, T: 1, K: 1, Retention: 86400, DeadlineAhead: 200, CheckC: 30}
	}
	base := "https://" + p.Addr()
	// As the plane delivers it: the pieces, then the entry.
	o, err := r.writer.Build(ctx, content(8<<20, 3))
	if err != nil {
		t.Fatal(err)
	}
	for i, pc := range o.Pieces {
		ans, _ := r.writer.Submit(ctx, base, storerec.BlobTopic(testBucket), pc)
		if ok, why := testwriter.Admitted(ans, storerec.BlobTopic(testBucket)); !ok {
			t.Fatalf("piece %d: %s", i+1, why)
		}
	}
	low, err := r.writer.Entry(ctx, o, "low", terms(9999))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.writer.Submit(ctx, base, storerec.RecordsTopic(testBucket), low.BEEF); err != nil {
		t.Fatal(err)
	}
	r.eventually("declined", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		e := p.st.Entries[low.C]
		return e != nil && strings.Contains(e.Decision, "price of 10000")
	})
	at := r.put(ctx, []*Provider{p}, content(8<<20, 4), "at", terms(10000))
	r.eventually("held", 10*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		e := p.st.Entries[at.C]
		return e != nil && e.Decision == decisionHeld
	})
}

// One writer's share of the space for unclaimed pieces holds a whole object
// of policy.max_object, which is its content and its nodes: a piece refused on
// the plane is never sent again, so the object would never be held.
func TestWriterShareHoldsAWholeObject(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.QuotaBytes, cfg.UnclaimedBytes, cfg.Policy.MaxObject = 256<<20, 32<<20, 8<<20
	p := r.start(cfg, true)
	base := "https://" + p.Addr()
	o, _ := r.writer.Build(ctx, content(int(cfg.Policy.MaxObject), 7))
	for i, piece := range o.Pieces {
		ans, _ := r.writer.Submit(ctx, base, storerec.BlobTopic(testBucket), piece)
		if ok, why := testwriter.Admitted(ans, storerec.BlobTopic(testBucket)); !ok {
			t.Fatalf("piece %d of %d: %s", i+1, len(o.Pieces), why)
		}
	}
}

// Storage is bounded: pieces no entry claimed have their own share and
// their own time, and a holding goes only once its lease and advertisement
// run out.
func TestQuotaAndEviction(t *testing.T) {
	var offset atomic.Int64
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.QuotaBytes, cfg.UnclaimedBytes, cfg.UnclaimedSeconds, cfg.Policy.MaxObject = 64<<20, 6<<20, 600, 1<<20
	if err := cfg.Check(); err != nil {
		t.Fatal(err)
	}
	if _, err := Init(cfg.Data); err != nil {
		t.Fatal(err)
	}
	p, err := New(ctx, cfg, Options{Tick: 100 * time.Millisecond, Logf: r.logf, Now: func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fundProvider(ctx, p, &nodeapi.RPC{URL: r.srv.URL + "/rpc", ID: "p"}, &nodeapi.Asset{Base: r.srv.URL}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	base := "https://" + p.Addr()
	// Two 4 MiB pieces of a closure with no entry: the second is over the
	// unclaimed share.
	o, _ := r.writer.Build(ctx, content(2*storerec.BlobSize+1, 9))
	ans, _ := r.writer.Submit(ctx, base, storerec.BlobTopic(testBucket), o.Pieces[1])
	if ok, why := testwriter.Admitted(ans, storerec.BlobTopic(testBucket)); !ok {
		t.Fatalf("first piece: %s", why)
	}
	ans, _ = r.writer.Submit(ctx, base, storerec.BlobTopic(testBucket), o.Pieces[2])
	if ok, why := testwriter.Admitted(ans, storerec.BlobTopic(testBucket)); ok || !strings.Contains(why, "room") {
		t.Fatalf("second piece: %v %s", ok, why)
	}
	// Past their time, unclaimed pieces go.
	offset.Store(int64(11 * time.Minute))
	r.eventually("unclaimed evicted", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.st.Unclaimed) == 0
	})
	offset.Store(0)
	// A holding is kept while advertised, and goes once the
	// advertisement and lease run out.
	put := r.put(ctx, []*Provider{p}, content(1000, 5), "small", bounty)
	root := put.Object.Built.RootHash
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(root) != nil })
	offset.Store(int64(12 * time.Hour))
	time.Sleep(300 * time.Millisecond)
	if p.liveAd(root) == nil || !p.store.Has(root) {
		t.Fatal("evicted while advertised")
	}
	ad := p.liveAd(root)
	offset.Store(int64(time.Until(time.Unix(int64(ad.Expiry), 0)) + time.Hour))
	r.eventually("evicted after expiry", 5*time.Second, func() bool { return !p.store.Has(root) })
}

// A funding tree signed and never on the chain comes back: its coin is
// returned and its record kept for one more start, then dropped.
func TestTreeCoinReturned(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	if err := cfg.Check(); err != nil {
		t.Fatal(err)
	}
	if _, err := Init(cfg.Data); err != nil {
		t.Fatal(err)
	}
	open := func(s *refusing) *Provider {
		o := Options{Tick: time.Hour, Logf: r.logf}
		if s != nil {
			o.Settler = s
		}
		p, err := New(ctx, cfg, o)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := open(&refusing{})
	if _, _, err := fundProvider(ctx, p, &nodeapi.RPC{URL: r.srv.URL + "/rpc", ID: "p"}, &nodeapi.Asset{Base: r.srv.URL}); err != nil {
		t.Fatal(err)
	}
	if err := p.recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.spendTree(ctx); err == nil {
		t.Fatal("a refused tree was spent")
	} else {
		t.Logf("spend: %v", err)
	}
	p.mu.Lock()
	n := len(p.st.PendingTrees)
	p.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d tree records", n)
	}
	_ = p.Close()
	for i, want := range []bool{true, false} {
		p = open(&refusing{})
		if err := p.recoverTrees(ctx); err != nil {
			t.Fatal(err)
		}
		p.mu.Lock()
		kept := len(p.st.PendingTrees) == 1 && p.st.PendingTrees[0].ReturnedOnce
		p.mu.Unlock()
		if kept != want {
			t.Fatalf("start %d: kept %v\n%s", i, kept, r.said())
		}
		_ = p.Close()
	}
}

type refusing struct{}

func (refusing) Submit(context.Context, *transaction.Transaction) error {
	return fmt.Errorf("unreachable")
}
func (refusing) Name() string { return "refusing" }

// A remittance is taken once, and only what its entry makes it.
func TestSettleRefusals(t *testing.T) {
	r, p, cfg, put := held(t, nil, 5000, bounty)
	ctx := context.Background()
	me := p.signer.Identity.Compressed()
	s, err := r.writer.Settle(ctx, put, []testwriter.Payee{{Identity: me, Rank: 1, Sats: 5000}})
	if err != nil {
		t.Fatal(err)
	}
	var rm map[string]any
	_ = json.Unmarshal(s.Remittances[1], &rm)
	try := func(edit func(m map[string]any)) int {
		m := map[string]any{}
		for k, v := range rm {
			m[k] = v
		}
		edit(m)
		b, _ := json.Marshal(m)
		call, err := r.writer.Post(ctx, cfg.FrontURL()+"/settle", b, 0)
		if err != nil {
			t.Fatal(err)
		}
		return call.Status
	}
	for name, edit := range map[string]func(map[string]any){
		"prefix":  func(m map[string]any) { m["derivationPrefix"] = base64.StdEncoding.EncodeToString(make([]byte, 32)) },
		"sender":  func(m map[string]any) { m["senderIdentityKey"] = hex.EncodeToString(me) },
		"rank 17": func(m map[string]any) { m["rank"] = 17 },
		"entry":   func(m map[string]any) { m["entry"] = strings.Repeat("0", 64) },
		"output":  func(m map[string]any) { m["outputIndex"] = 1 },
	} {
		if st := try(edit); st != 400 {
			t.Errorf("%s: %d", name, st)
		}
	}
	for range 2 {
		if st := try(func(map[string]any) {}); st != 200 {
			t.Fatalf("remittance: %d", st)
		}
	}
	n := 0
	for _, x := range p.ledger.payments() {
		if x.Class == "settle" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d ledger lines", n)
	}
	_ = payee.LedgerFile
}

// What is not a writer's signed piece, on a blob topic, is not stored.
func TestPieceRefusals(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.Buckets = append(cfg.Buckets, BucketConfig{Bucket: "other_qzvmkwertz", Writers: []string{"02" + strings.Repeat("11", 32)}})
	cfg.Buckets[1].Writers = nil
	p := r.start(cfg, true)
	base := "https://" + p.Addr()
	o, err := r.writer.Build(ctx, content(100, 1))
	if err != nil {
		t.Fatal(err)
	}
	put, err := r.writer.Entry(ctx, o, "x", testwriter.Terms{NoHold: true})
	if err != nil {
		t.Fatal(err)
	}
	blob := storerec.BlobTopic(testBucket)
	// What breaks a piece's rules is refused as an engine refuses, a 400
	// naming the rule: on any blob topic, served or not, before any policy.
	for name, c := range map[string]struct {
		topic string
		beef  []byte
		want  string
	}{
		"an entry on the blob topic":   {blob, put.BEEF, "shape"},
		"another bucket's piece":       {storerec.BlobTopic("other_qzvmkwertz"), o.Pieces[0], "bucket"},
		"a piece on an unserved topic": {"tm_bstoreblob_nobody_abcdefghij", o.Pieces[0], "bucket"},
		"garbage":                      {blob, []byte("not beef"), "beef"},
	} {
		_, err := r.writer.Submit(ctx, base, c.topic, c.beef)
		if err == nil || !strings.Contains(err.Error(), ": 400 ") || !strings.Contains(err.Error(), "piece: "+c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A writer the bucket does not take.
	p.cfg.Buckets[0].Writers = []string{"02" + strings.Repeat("22", 32)}
	ans, _ := r.writer.Submit(ctx, base, blob, o.Pieces[0])
	if ok, why := testwriter.Admitted(ans, blob); ok || !strings.Contains(why, "writer") {
		t.Errorf("a writer not taken: %v %q", ok, why)
	}
}
