package main

// The provider end to end, on a reference overlay host loading the bstore
// module and a local test chain (bcommon testchain), with two
// bstore-provider processes and one test-only writer (internal/testwriter):
//
//	BSTORE_E2E_REFHOST=/path/to/reference-host BSTORE_E2E_NODE=/path/to/node24 \
//	  go test -run TestProviderE2E -v ./cmd/bstore-provider/
//
// make provider-e2e runs it. It needs Docker for the host's MySQL. It puts
// a 100 MiB object (25 blobs) with a bounty of t = 2, K = 2; both providers
// receive, verify and advertise it; the second is killed the instant its
// first funding tree reaches the settlement leg (mid-advertise) and
// restarted, and recovers the tree and advertises; the race is ranked by
// first sight at the host; each passes a free settlement challenge (the
// second answers by reference behind a server whose Range handling is
// wrong, and is not blamed); a third free one is refused; a renewal is
// bought; the bounty is settled to both by rank, one by remittance and one
// found from the hold output's spend; and a blob dropped from the first
// provider's store is caught by a paid challenge of c = 113, its nonce
// chosen so that a sample falls in the dropped blob.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/provider"
	"github.com/lightwebinc/bstore/storerec"
)

const e2eBucket = "example_photos_hmqzvtkwrx"

type e2e struct {
	t      *testing.T
	ctx    context.Context
	dir    string
	chain  *testchain.Chain
	cs     *httptest.Server
	client *http.Client
	cert   string
	key    string
	bin    string
	host   string
}

func TestProviderE2E(t *testing.T) {
	refhost := os.Getenv("BSTORE_E2E_REFHOST")
	node := os.Getenv("BSTORE_E2E_NODE")
	if refhost == "" || node == "" {
		t.Skip("set BSTORE_E2E_REFHOST and BSTORE_E2E_NODE (make provider-e2e)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	e := &e2e{t: t, ctx: ctx, dir: t.TempDir()}
	if d := os.Getenv("BSTORE_E2E_LOGS"); d != "" {
		e.dir = d
		_ = os.MkdirAll(d, 0o700)
	}
	e.chain = testchain.New(700)
	e.cs = httptest.NewServer(e.chain)
	defer e.cs.Close()
	var pool *x509.CertPool
	e.cert, e.key, pool = writeCert(t, e.dir)
	e.client = &http.Client{Timeout: 2 * time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	e.bin = filepath.Join(e.dir, "bstore-provider")
	build := exec.Command("go", "build", "-o", e.bin, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	e.host = e.startHost(refhost, node)
	e.ok("the reference host loads the bstore module")

	// The writer.
	hc, _ := headers.NewSource(e.cs.URL)
	rpc := &nodeapi.RPC{URL: e.cs.URL + "/rpc", ID: "writer"}
	asset := &nodeapi.Asset{Base: e.cs.URL}
	w, err := testwriter.New(ctx, filepath.Join(e.dir, "writer"), e2eBucket, rpc, asset, hc, e.client)
	if err != nil {
		t.Fatal(err)
	}

	// Provider A, plain; provider B answers by reference behind a server
	// whose Range handling is wrong, and its settlement leg is a proxy that
	// kills it the instant its first transaction reaches the chain.
	a := e.provider("a", nil, "")
	var killB atomic.Bool
	var bproc atomic.Pointer[exec.Cmd]
	legB := e.killingLeg(func() bool { return killB.Load() }, func() {
		if c := bproc.Load(); c != nil && c.Process != nil {
			_ = c.Process.Signal(syscall.SIGKILL)
		}
	})
	faulty := e.rangeFaulty()
	b := e.provider("b", &faulty.port, legB)
	bu, _ := url.Parse(b.direct)
	faulty.target.Store(bu)
	e.fund(a)
	e.fund(b)
	a.start(e)
	killB.Store(true)
	b.start(e)
	bproc.Store(b.cmd)

	// The object: 100 MiB, 25 blobs.
	content := make([]byte, 25*storerec.BlobSize)
	h := sha256.Sum256([]byte("bstore e2e"))
	for i := 0; i < len(content); i += 32 {
		copy(content[i:], h[:])
		h = sha256.Sum256(h[:])
	}
	o, err := w.Build(ctx, content)
	if err != nil {
		t.Fatal(err)
	}
	put, err := w.Entry(ctx, o, "photos/big.bin", testwriter.Terms{Sats: 20000, T: 2, K: 2, Retention: 86400, DeadlineAhead: 600, CheckC: 30})
	if err != nil {
		t.Fatal(err)
	}
	root := o.Built.RootHash
	records := storerec.RecordsTopic(e2eBucket)
	holdBEEF, err := testwriter.AloneBEEF(put.Hold.Tx)
	if err != nil {
		t.Fatal(err)
	}
	e.submit(w, e.host, records, holdBEEF, "the hold output at the host")
	e.submit(w, e.host, records, put.BEEF, "the entry at the host")
	for _, p := range []*proc{a, b} {
		e.submit(w, p.direct, records, put.BEEF, "the entry at provider "+p.name)
		for _, pc := range o.Pieces {
			e.submit(w, p.direct, storerec.BlobTopic(e2eBucket), pc, "")
		}
		e.ok("provider %s received the 26 pieces", p.name)
	}

	// The race, judged by first sight at the host.
	first := map[string]time.Time{}
	ads := map[string]testwriter.Ad{}
	watch := func(want int, within time.Duration) {
		end := time.Now().Add(within)
		for time.Now().Before(end) {
			got, err := w.Ads(ctx, e.host, root)
			if err == nil {
				for _, ad := range got {
					k := hex.EncodeToString(ad.Ad.Host)
					if _, ok := first[k]; !ok {
						first[k] = time.Now()
						ads[k] = ad
					}
				}
			}
			if len(first) >= want {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		e.fail("%d advertisement(s) at the host within %s, saw %d", want, within, len(first))
	}
	watch(1, 3*time.Minute)
	if _, ok := first[a.identity]; !ok {
		e.fail("the first advertisement is not provider a's")
	}
	e.ok("provider a verified and advertised; the host answers it")
	if err := b.wait(2 * time.Minute); err == nil || !strings.Contains(err.Error(), "killed") {
		e.fail("provider b was not killed mid-advertise: %v", err)
	}
	e.ok("provider b was killed as its funding tree reached the settlement leg")
	killB.Store(false)
	b.start(e)
	bproc.Store(b.cmd)
	watch(2, 3*time.Minute)
	e.ok("provider b restarted, recovered its funding tree and advertised")
	if !b.logged("funding tree") || !b.logged("tree adopted") {
		e.fail("provider b's log does not show the tree recovered:\n%s", b.log())
	}
	var cands []storerec.Candidate
	for k, ad := range ads {
		id, _ := hex.DecodeString(k)
		txid, _ := displayHash(ad.Txid)
		cands = append(cands, storerec.Candidate{Ad: &storerec.Ad{Txid: txid, Vout: ad.Vout, Advertiser: id, Root: root, Expiry: ad.Ad.Expiry},
			FirstSeen: uint64(first[k].UnixNano()), Passed: true})
	}
	putC, _ := displayHash(put.C)
	ranked := storerec.Rank(putC, cands)
	if hex.EncodeToString(ranked[0].Ad.Advertiser) != a.identity || hex.EncodeToString(ranked[1].Ad.Advertiser) != b.identity {
		e.fail("ranking")
	}
	e.ok("the race is ranked by first sight: a then b")

	// Settlement challenges, free, c = min(checkC, freeC) = 30.
	for _, p := range []*proc{a, b} {
		ad := ads[p.identity]
		front, ok := storerec.DerivedFront(ad.Ad.URL)
		if !ok {
			e.fail("no derived front for %s", ad.Ad.URL)
		}
		id, _ := hex.DecodeString(p.identity)
		q, raw, err := w.Request(ctx, o.Built, id, 30, [16]byte{byte(p.name[0])})
		if err != nil {
			t.Fatal(err)
		}
		call, err := w.Challenge(ctx, front, raw, put.C, 0)
		if err != nil || call.Status != 200 {
			e.fail("settlement challenge of %s: %v %+v", p.name, err, call)
		}
		f := &testwriter.Fetcher{HTTP: e.client, URL: ad.Ad.URL}
		v := testwriter.Verify(q, raw, call.Body, o, f)
		if v.Outcome != storerec.OutcomePass {
			e.fail("settlement challenge of %s: verdict %+v", p.name, v)
		}
		e.ok("provider %s passed its free settlement challenge (c = 30)", p.name)
		if p == b {
			if len(v.Rechecked) == 0 || f.Whole == 0 || faulty.mangled.Load() == 0 {
				e.fail("b's by-reference samples were not rechecked from the whole blob: %+v, %d whole", v, f.Whole)
			}
			e.ok("the Range-faulty server's samples were rechecked from the whole blob: %d of 30, a pass, nobody blamed (M1)", len(v.Rechecked))
		}
	}
	// The second free one passes, the third is priced.
	ida, _ := hex.DecodeString(a.identity)
	frontA, _ := storerec.DerivedFront(ads[a.identity].Ad.URL)
	_, raw2, _ := w.Request(ctx, o.Built, ida, 30, [16]byte{9})
	if call, err := w.Challenge(ctx, frontA, raw2, put.C, 0); err != nil || call.Status != 200 {
		e.fail("second free challenge: %v %+v", err, call)
	}
	_, raw3, _ := w.Request(ctx, o.Built, ida, 30, [16]byte{10})
	if _, err := w.Challenge(ctx, frontA, raw3, put.C, 0); err == nil || !strings.Contains(err.Error(), "satoshis asked") {
		e.fail("a third free settlement challenge was not priced: %v", err)
	}
	e.ok("two free settlement challenges per hold outpoint, the third priced")

	// A renewal of a's advertisement, bought by the writer.
	adA := ads[a.identity]
	until := adA.Ad.Expiry + 86400
	body, _ := json.Marshal(map[string]any{"root": hex.EncodeToString(root[:]), "until": until})
	call, err := w.Post(ctx, frontA+"/renew", body, 1_000_000)
	if err != nil || call.Status != 200 {
		e.fail("renewal: %v %+v", err, call)
	}
	var ren struct {
		Txid string `json:"txid"`
	}
	_ = json.Unmarshal(call.Body, &ren)
	e.eventually("the host answers the renewal", time.Minute, func() bool {
		got, err := w.Ads(ctx, e.host, root)
		if err != nil {
			return false
		}
		for _, ad := range got {
			if ad.Txid == ren.Txid && ad.Ad.Expiry == until && hex.EncodeToString(ad.Ad.Host) == a.identity {
				// The successor spends the advertisement it renews.
				for _, in := range ad.Tx.Inputs {
					if in.SourceTXID.String() == adA.Txid && in.SourceTxOutIndex == adA.Vout {
						return true
					}
				}
			}
		}
		return false
	})
	e.ok("the renewal's successor spends the advertisement, same key and root, a later expiry")

	// Settlement by rank: a by remittance, b found from the hold's spend.
	pay := storerec.Payouts(put.Hold.Sats, 2)
	idb, _ := hex.DecodeString(b.identity)
	s, err := w.Settle(ctx, put, []testwriter.Payee{{Identity: ida, Rank: 1, Sats: pay[0]}, {Identity: idb, Rank: 2, Sats: pay[1]}})
	if err != nil {
		t.Fatal(err)
	}
	if call, err := w.Post(ctx, frontA+"/settle", s.Remittances[1], 0); err != nil || call.Status != 200 {
		e.fail("remittance to a: %v %+v", err, call)
	}
	e.eventually("a's payout is pooled", 2*time.Minute, func() bool { return a.metric("bstore_provider_payouts_total", "remittance") >= 1 })
	e.eventually("b finds its payout from the hold output's spend", 2*time.Minute, func() bool { return b.metric("bstore_provider_payouts_total", "scan") >= 1 })
	e.ok("settlement paid both by rank: %d to a (remittance), %d to b (found)", pay[0], pay[1])

	// A dropped blob, caught by a paid challenge of c = 113.
	const dropped = 7
	closure, _ := storerec.VerifyCHIRP(root, func(x [32]byte) ([]byte, error) {
		for _, ob := range o.Built.Objects() {
			if sha256.Sum256(ob) == x {
				return ob, nil
			}
		}
		return nil, fmt.Errorf("none")
	})
	blobHash := closure.Blobs[dropped].Hash
	hh := hex.EncodeToString(blobHash[:])
	if err := os.Remove(filepath.Join(a.data, "store", "objects", hh[:2], hh)); err != nil {
		t.Fatal(err)
	}
	q0, _, _ := w.Request(ctx, o.Built, ida, 113, [16]byte{})
	hits, chosen := 0, -1
	for n := 0; n < 2000; n++ {
		q := *q0
		var nonce [16]byte
		copy(nonce[:], strconv.Itoa(n))
		q.Nonce = nonce
		hit := false
		for k := uint32(0); k < 113 && !hit; k++ {
			hit = q.Index(k)/storerec.SegmentsPerBlob == dropped
		}
		if hit {
			hits++
			if chosen < 0 {
				chosen = n
			}
		}
	}
	rate := float64(hits) / 2000
	if math.Abs(rate-0.99) > 0.01 {
		e.fail("detection over 2000 nonces is %.3f, not about 0.99", rate)
	}
	q := *q0
	var nonce [16]byte
	copy(nonce[:], strconv.Itoa(chosen))
	q.Nonce = nonce
	raw, _ := q.Encode()
	call, err = w.Challenge(ctx, frontA, raw, put.C, 1_000_000)
	if err != nil || call.Status != 422 || !bytes.Contains(call.Body, []byte("unknown-root")) {
		e.fail("the paid challenge on a dropped blob: %v %+v", err, call)
	}
	res, err := e.client.Get(adA.Ad.URL)
	if err == nil {
		res.Body.Close()
	}
	if err != nil || res.StatusCode != 404 {
		e.fail("a damaged root is still served")
	}
	e.ok("a dropped blob (1 of 25) is caught by a paid challenge of c = 113 (%.1f%% of nonces catch it; nonce %d chosen); the root is no longer served or answered for", rate*100, chosen)

	a.stop()
	b.stop()
}

// proc is one bstore-provider process.
type proc struct {
	name     string
	data     string
	config   string
	direct   string // its own listener
	metrics  string
	identity string
	cmd      *exec.Cmd
	out      *lockedBuf
	done     chan error
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
	f  *os.File
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		_, _ = l.f.Write(p)
	}
	return l.b.Write(p)
}

func (p *proc) log() string {
	p.out.mu.Lock()
	defer p.out.mu.Unlock()
	return p.out.b.String()
}

func (p *proc) logged(s string) bool { return strings.Contains(p.log(), s) }

// provider writes a provider's configuration; its public URL is its own
// listener, or the port given (a proxy in front of it).
func (e *e2e) provider(name string, publicPort *int, settle string) *proc {
	data := filepath.Join(e.dir, "provider-"+name)
	out, err := exec.Command(e.bin, "init", "-data", data).CombinedOutput()
	if err != nil {
		e.fail("init %s: %v %s", name, err, out)
	}
	id := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(out)), "identity"))
	port := freePort(e.t)
	mport := freePort(e.t)
	pub := port
	if publicPort != nil {
		pub = *publicPort
	}
	if settle == "" {
		settle = "rpc:" + e.cs.URL + "/rpc"
	}
	cfg := map[string]any{
		"data": data, "listen": fmt.Sprintf("127.0.0.1:%d", port), "tls_cert": e.cert, "tls_key": e.key,
		"public_url": fmt.Sprintf("https://127.0.0.1:%d/store", pub), "node": e.cs.URL, "settle": settle, "headers": e.cs.URL,
		"hosts": []string{e.host}, "buckets": []map[string]any{{"bucket": e2eBucket}}, "tree_count": 4,
		"terms":          map[string]any{"challenge_base": 10, "challenge_per_sample": 1, "free_c": 30, "min_bounty": 1000, "min_age": 2, "renew_per_gib_day": 100, "renew_minimum": 300},
		"limits":         map[string]any{"challenge_burst": 20},
		"policy":         map[string]any{"answer_inline": publicPort == nil},
		"metrics_listen": fmt.Sprintf("127.0.0.1:%d", mport),
	}
	raw, _ := json.MarshalIndent(cfg, "", "  ")
	path := filepath.Join(e.dir, "provider-"+name+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		e.t.Fatal(err)
	}
	f, _ := os.Create(filepath.Join(e.dir, "provider-"+name+".log"))
	return &proc{name: name, data: data, config: path, direct: fmt.Sprintf("https://127.0.0.1:%d", port), metrics: fmt.Sprintf("http://127.0.0.1:%d/metrics", mport), identity: id, out: &lockedBuf{f: f}}
}

func (e *e2e) fund(p *proc) {
	out, err := exec.Command(e.bin, "fund", "-config", p.config, "-blocks", "104").CombinedOutput()
	if err != nil {
		e.fail("fund %s: %v %s", p.name, err, out)
	}
	arcadeAfterFund(e.t, p.config, e.cs.URL)
}

// arcadeAfterFund moves a provider's configuration from the chain's RPC
// leg (which fund needs, to mine its coins) to the chain's arcade leg,
// where a payment can be taken on the network's acceptance.
func arcadeAfterFund(t *testing.T, path, chain string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["settle"] != "rpc:"+chain+"/rpc" {
		return
	}
	cfg["settle"] = "arcade:" + chain + "/arcade"
	raw, _ = json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (p *proc) start(e *e2e) {
	p.cmd = exec.Command(e.bin, "run", "-config", p.config)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	p.done = make(chan error, 1)
	cmd := p.cmd
	go func() { p.done <- cmd.Wait() }()
	e.eventually("provider "+p.name+" listens", 30*time.Second, func() bool {
		res, err := e.client.Get(p.direct + "/healthz")
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == 200
	})
	e.t.Cleanup(func() { _ = cmd.Process.Kill() })
}

func (p *proc) wait(d time.Duration) error {
	select {
	case err := <-p.done:
		if err == nil {
			return fmt.Errorf("exited")
		}
		return err
	case <-time.After(d):
		return fmt.Errorf("still running")
	}
}

func (p *proc) stop() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		_ = p.wait(20 * time.Second)
	}
}

// metric reads a counter of the provider's /metrics with a label value.
func (p *proc) metric(name, label string) float64 {
	res, err := http.Get(p.metrics)
	if err != nil {
		return 0
	}
	defer res.Body.Close()
	sc := bufio.NewScanner(res.Body)
	total := 0.0
	for sc.Scan() {
		l := sc.Text()
		if strings.HasPrefix(l, name) && strings.Contains(l, label) {
			f := strings.Fields(l)
			v, _ := strconv.ParseFloat(f[len(f)-1], 64)
			total += v
		}
	}
	return total
}

// killingLeg is a settlement leg in front of the chain's RPC: when armed,
// the first transaction sent through it reaches the chain and then the
// process that sent it is killed, before it hears the answer.
func (e *e2e) killingLeg(armed func() bool, kill func()) string {
	target, _ := url.Parse(e.cs.URL)
	rp := httputil.NewSingleHostReverseProxy(target)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if armed() && bytes.Contains(body, []byte("sendrawtransaction")) {
			rec := httptest.NewRecorder()
			rp.ServeHTTP(rec, r)
			kill()
			time.Sleep(200 * time.Millisecond)
			http.Error(w, "gone", http.StatusBadGateway)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	e.t.Cleanup(srv.Close)
	return "rpc:" + srv.URL + "/rpc"
}

// faultyServer is a TLS reverse proxy in front of a provider whose Range
// answers are off by one byte; everything else passes through.
type faultyServer struct {
	port    int
	target  atomic.Pointer[url.URL]
	mangled atomic.Int64
}

func (e *e2e) rangeFaulty() *faultyServer {
	f := &faultyServer{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.t.Fatal(err)
	}
	f.port = ln.Addr().(*net.TCPAddr).Port
	cert, err := tls.LoadX509KeyPair(e.cert, e.key)
	if err != nil {
		e.t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := f.target.Load()
		if t == nil {
			http.Error(w, "no target", 502)
			return
		}
		rp := httputil.NewSingleHostReverseProxy(t)
		rp.Transport = e.client.Transport
		if r.Method == http.MethodGet && r.Header.Get("Range") != "" && strings.Contains(r.URL.Path, "/chirp/v1/") {
			var a, b int
			if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &a, &b); err == nil {
				r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", a+1, b+1))
				f.mangled.Add(1)
			}
		}
		rp.ServeHTTP(w, r)
	})}
	go func() { _ = srv.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})) }()
	e.t.Cleanup(func() { _ = srv.Close() })
	return f
}

func (e *e2e) submit(w *testwriter.Writer, base, topic string, beef []byte, what string) {
	ans, err := w.Submit(e.ctx, base, topic, beef)
	if err != nil {
		e.fail("%s: %v", what, err)
	}
	if ok, why := testwriter.Admitted(ans, topic); !ok {
		e.fail("%s: not admitted (%s)", what, why)
	}
	if what != "" {
		e.ok("%s", what)
	}
}

func (e *e2e) startHost(refhost, node string) string {
	t := e.t
	pw := "bstore-e2e"
	id := strings.TrimSpace(runOut(t, "docker", "run", "-d", "--rm", "-e", "MYSQL_ROOT_PASSWORD="+pw, "-e", "MYSQL_DATABASE=overlay",
		"-p", "127.0.0.1::3306", "--tmpfs", "/var/lib/mysql", "mysql:9"))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	portLine := strings.Split(strings.TrimSpace(runOut(t, "docker", "port", id, "3306/tcp")), "\n")[0]
	dbPort := portLine[strings.LastIndex(portLine, ":")+1:]
	for i := 0; ; i++ {
		if exec.Command("docker", "exec", id, "mysql", "-h127.0.0.1", "-uroot", "-p"+pw, "-e", "select 1").Run() == nil {
			break
		}
		if i == 120 {
			t.Fatal("MySQL did not come up")
		}
		time.Sleep(time.Second)
	}
	stage := filepath.Join(e.dir, "host")
	_ = os.MkdirAll(filepath.Join(stage, "modules", "bstore"), 0o700)
	_ = os.MkdirAll(filepath.Join(stage, "state"), 0o700)
	_ = os.Remove(filepath.Join(stage, "node_modules"))
	if err := os.Symlink(filepath.Join(refhost, "node_modules"), filepath.Join(stage, "node_modules")); err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile(filepath.Join("..", "..", "host", "bundle", "bstore-module.js"))
	if err != nil {
		t.Fatalf("the module bundle (make module): %v", err)
	}
	mod := filepath.Join(stage, "modules", "bstore", "bstore-module.js")
	if err := os.WriteFile(mod, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	cmd := exec.Command(node, filepath.Join(refhost, "dist", "index.js"))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"),
		"OVERLAY_TOPICS=" + storerec.RecordsTopic(e2eBucket), "OVERLAY_MODULES=" + mod,
		"OVERLAY_KNEX_URL=mysql://root:" + pw + "@127.0.0.1:" + dbPort + "/overlay",
		"OVERLAY_CHAIN_TRACKER_URL=" + e.cs.URL, "OVERLAY_ADMIN_TOKEN=bstore-e2e", "OVERLAY_LISTEN=127.0.0.1",
		"OVERLAY_PORT=" + strconv.Itoa(port), "BSTORE_BUCKETS=" + e2eBucket, "BSTORE_STATE_DIR=" + filepath.Join(stage, "state"),
		"BSTORE_HEADERS_URL=" + e.cs.URL}
	f, _ := os.Create(filepath.Join(e.dir, "host.log"))
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	e.eventually("the host is ready", time.Minute, func() bool {
		res, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == 200
	})
	return base
}

func runOut(t *testing.T, name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		t.Fatalf("%s %v: %v", name, args, err)
	}
	return string(out)
}

func (e *e2e) ok(format string, args ...any) { e.t.Logf("ok  "+format, args...) }

func (e *e2e) fail(format string, args ...any) {
	e.t.Helper()
	e.t.Fatalf("e2e: "+format+"\n(logs under %s)", append(args, e.dir)...)
}

func (e *e2e) eventually(what string, d time.Duration, f func() bool) {
	e.t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if f() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	e.fail("%s: did not happen", what)
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func displayHash(s string) ([32]byte, bool) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return [32]byte{}, false
	}
	var out [32]byte
	for i := range b {
		out[i] = b[31-i]
	}
	return out, true
}

func writeCert(t *testing.T, dir string) (string, string, *x509.CertPool) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "bstore e2e"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(k)
	cp, kp := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	_ = os.WriteFile(cp, pemBlock("CERTIFICATE", der), 0o600)
	_ = os.WriteFile(kp, pemBlock("EC PRIVATE KEY", kb), 0o600)
	c, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(c)
	return cp, kp, pool
}

func pemBlock(typ string, b []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b})
}

var (
	_ = chirp.Identifier
	_ = transaction.NewTransaction
	_ = provider.Profile
)
