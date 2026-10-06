package main

// The publisher client end to end, through the command line alone, on a
// reference overlay host loading the bstore module and a local test chain
// (bcommon testchain), with two bstore-provider processes:
//
//	BSTORE_E2E_REFHOST=/path/to/reference-host BSTORE_E2E_NODE=/path/to/node24 \
//	  go test -run TestPublisherE2E -v ./cmd/bstore/
//
// make publisher-e2e runs it. It needs Docker for the host's MySQL. An
// owner creates a bucket while the host is down (the token waits in the
// outbox) and fills the host once it is up; a writer the owner named puts
// 100 MiB with a bounty of t = 2, K = 2, its closure going to both
// providers by BRC-167 upload session; the writer judges the race (first
// sight, a free settlement challenge of each candidate at c = 30) and
// settles it by rank, the settlement killed the instant it reaches the
// settlement leg and finished by running settle again (the same bytes, one
// settlement), both providers paid by remittance; the owner anchors; the
// object is read back, every check made; a blob dropped from one provider
// is caught by a paid challenge of c = 113 whose no-answer outcome is
// recorded and published; a reader buys a renewal from the other provider;
// a copy is read; rm, anchor and NoSuchKey; a bounty nobody can win lapses
// at its deadline and is reclaimed; and the
// kill switch sweeps every tree, after which the writer's unanchored entry
// is retracted and the owner's next anchoring skips it.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/storerec"
)

type pubRig struct {
	*rig
	chain *testchain.Chain
}

// cli runs the bstore command line in home and returns its output and
// exit code.
func (e *pubRig) cli(home string, args ...string) (string, int) {
	e.t.Helper()
	c := exec.CommandContext(e.ctx, e.bin, append([]string{"-home", home}, args...)...)
	out, err := c.CombinedOutput()
	code := 0
	var x *exec.ExitError
	if errors.As(err, &x) {
		code = x.ExitCode()
	} else if err != nil {
		e.fail("bstore %s: %v", strings.Join(args, " "), err)
	}
	e.t.Logf("$ bstore %s  (exit %d)\n%s", strings.Join(args, " "), code, indent(string(out)))
	return string(out), code
}

func (e *pubRig) must(home string, want int, args ...string) string {
	e.t.Helper()
	out, code := e.cli(home, args...)
	if code != want {
		e.fail("bstore %s: exit %d, want %d\n%s", strings.Join(args, " "), code, want, out)
	}
	return out
}

func indent(s string) string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return ""
	}
	return "    " + strings.ReplaceAll(s, "\n", "\n    ")
}

func field(out, name string) string {
	m := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `\s+(\S+)`).FindStringSubmatch(out)
	if m == nil {
		return ""
	}
	return m[1]
}

func TestPublisherE2E(t *testing.T) {
	refhost := os.Getenv("BSTORE_E2E_REFHOST")
	node := os.Getenv("BSTORE_E2E_NODE")
	if refhost == "" || node == "" {
		t.Skip("set BSTORE_E2E_REFHOST and BSTORE_E2E_NODE (make publisher-e2e)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	base := &rig{t: t, ctx: ctx, dir: t.TempDir()}
	if d := os.Getenv("BSTORE_E2E_LOGS"); d != "" {
		base.dir = d
		_ = os.MkdirAll(d, 0o700)
	}
	e := &pubRig{rig: base, chain: testchain.New(700)}
	e.cs = httptest.NewServer(e.chain)
	defer e.cs.Close()
	var pool *x509.CertPool
	e.cert, e.key, pool = writeCert(t, e.dir)
	e.client = &http.Client{Timeout: 2 * time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	e.pbin = filepath.Join(e.dir, "bstore-provider")
	e.bin = filepath.Join(e.dir, "bstore")
	for _, b := range [][2]string{{e.pbin, "../bstore-provider"}, {e.bin, "."}} {
		build := exec.Command("go", "build", "-o", b[0], b[1])
		build.Env = append(os.Environ(), "GOWORK=off")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", b[1], err, out)
		}
	}
	hostPort := freePort(t)
	e.host = fmt.Sprintf("http://127.0.0.1:%d", hostPort)
	portA, portB := freePort(t), freePort(t)
	directA, directB := fmt.Sprintf("https://127.0.0.1:%d", portA), fmt.Sprintf("https://127.0.0.1:%d", portB)

	// The settlement leg the writer uses: the chain's RPC, behind a proxy
	// that, when armed, kills the writer's command line the instant a
	// settlement reaches it, before it is forwarded.
	var victim atomic.Pointer[exec.Cmd]
	var armHold atomic.Value // the hold txid a settlement spends
	armHold.Store("")
	killed := atomic.Bool{}
	leg := e.killingLeg(func(raw []byte) bool {
		h, _ := armHold.Load().(string)
		if h == "" {
			return false
		}
		tx, err := transaction.NewTransactionFromBytes(raw)
		if err != nil {
			return false
		}
		for _, in := range tx.Inputs {
			if in.SourceTXID != nil && in.SourceTXID.String() == h {
				return true
			}
		}
		return false
	}, func() {
		if c := victim.Load(); c != nil && c.Process != nil {
			_ = c.Process.Kill()
			killed.Store(true)
			armHold.Store("")
		}
	})

	// Two homes: the bucket's owner and a writer it names.
	owner, writer := filepath.Join(e.dir, "owner"), filepath.Join(e.dir, "writer")
	common := []string{"-network", "regtest", "-node", e.cs.URL, "-headers", e.cs.URL, "-hosts", e.host, "-ca", e.cert, "-fetch-private"}
	e.must(owner, 0, append([]string{"init", "-settle", "rpc:" + e.cs.URL + "/rpc"}, common...)...)
	wout := e.must(writer, 0, append([]string{"init", "-settle", leg, "-providers", directA + "," + directB,
		"-uploads", directA + "/store," + directB + "/store"}, common...)...)
	writerKey := field(wout, "identity")
	e.must(owner, 0, "fund", "-blocks", "104")
	e.must(writer, 0, "fund", "-blocks", "104")
	e.must(writer, 0, "doctor")
	e.ok("two homes, funded: an owner and a writer %s", writerKey[:12])

	// The bucket, created while the host is down: the token waits.
	out := e.must(owner, 3, "bucket", "new", "photos", "-writers", writerKey)
	bucket, ownerKey, id := field(out, "bucket"), field(out, "owner"), field(out, "id")
	if bucket == "" || id == "" || !strings.HasPrefix(bucket, "photos_") {
		e.fail("bucket new: %s", out)
	}
	e.ok("bucket new: %s, id %s; the host is down and the first token waits (exit 3)", bucket, id[:12])
	if out, code := e.cli(owner, "due"); code != 3 || !strings.Contains(out, "outbox") {
		e.fail("due with the token undelivered: %d %s", code, out)
	}
	served := []string{bucket + ":" + ownerKey + ":" + id}
	e.startHostAt(refhost, node, []string{bucket}, served, hostPort)
	e.ok("the reference host serves the chain")
	e.must(owner, 0, "sync")
	e.must(owner, 0, "fill")
	e.must(owner, 0, "due")
	e.ok("sync sent the first token from the outbox; fill finds nothing the host lacks; nothing is due")
	e.must(writer, 0, "bucket", "add", bucket, "-owner", ownerKey, "-id", id)

	// Two providers on the bucket.
	pa := e.pubProvider("a", portA, bucket)
	pb := e.pubProvider("b", portB, bucket)
	for _, p := range []*proc{pa, pb} {
		e.run(e.pbin, "fund", "-config", p.config, "-blocks", "104")
		arcadeAfterFund(e.t, p.config, e.cs.URL)
		p.start(e.rig, p.direct+"/healthz")
	}
	e.ok("two providers run")
	if out := e.must(writer, 0, "terms", directA+"/store/bstore/v1"); !strings.Contains(out, `"freeC": 30`) {
		e.fail("terms: %s", out)
	}

	// Put 100 MiB with a bounty of t = 2, K = 2: the closure by upload
	// session to both providers.
	content := make([]byte, 25*storerec.BlobSize)
	h := sha256.Sum256([]byte("bstore publisher e2e"))
	for i := 0; i < len(content); i += 32 {
		copy(content[i:], h[:])
		h = sha256.Sum256(h[:])
	}
	bigPath := filepath.Join(e.dir, "big.bin")
	if err := os.WriteFile(bigPath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	out = e.must(writer, 0, "put", "photos/big.bin", bigPath, "-bounty", "20000", "-t", "2", "-k", "2", "-retention", "86400",
		"-deadline", "600", "-check-c", "30")
	entry := field(out, "entry")
	holdTxid, _, _ := strings.Cut(field(out, "hold"), ".")
	if entry == "" || holdTxid == "" || strings.Count(out, "upload ") != 2 {
		e.fail("put: %s", out)
	}
	e.ok("put 100 MiB: entry %s, the closure uploaded to both providers by session", entry[:12])

	// The race, judged by the writer: both providers seen, each passes a
	// free settlement challenge of c = 30.
	out = e.must(writer, 0, "holds", "-watch", "5m")
	if strings.Count(out, " passed") < 2 || !strings.Contains(out, "c=30") {
		e.fail("holds: %s", out)
	}
	e.ok("the race: both providers advertised, ranked by first sight, and passed their settlement challenges")

	// Settle, killed the instant the settlement reaches the leg.
	armHold.Store(holdTxid)
	cmd := exec.CommandContext(ctx, e.bin, "-home", writer, "settle", "-entry", entry)
	var sout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &sout, &sout
	victim.Store(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !killed.Load() {
		e.fail("settle was not killed at the leg:\n%s", sout.String())
	}
	race := readRace(t, writer, entry)
	if race.State != publish.RaceSettling || race.Settlement == nil {
		e.fail("after the kill the race is %s", race.State)
	}
	saved := race.Settlement.Txid
	if by, _ := (&nodeapi.Asset{Base: e.cs.URL}).Spender(ctx, holdTxid, 0); by != "" {
		e.fail("the hold output is spent before the settlement was forwarded: %s", by)
	}
	e.ok("settle killed mid-settlement: settlement %s recorded and never sent", saved[:12])
	out = e.must(writer, 0, "settle", "-entry", entry)
	race = readRace(t, writer, entry)
	if race.State != publish.RaceSettled || race.Settlement.Txid != saved || len(race.Settlement.Payees) != 2 {
		e.fail("after the restart: %s %+v", race.State, race.Settlement)
	}
	var sum uint64
	for _, p := range race.Settlement.Payees {
		if !p.Remitted {
			e.fail("rank %d not remitted: %s", p.Rank, p.Last)
		}
		sum += p.Satoshis
	}
	pay := storerec.Payouts(20000, 2)
	if sum != 20000 || race.Settlement.Payees[0].Satoshis != pay[0] || race.Settlement.Payees[1].Satoshis != pay[1] {
		e.fail("payouts %+v, want %v", race.Settlement.Payees, pay)
	}
	if by, _ := (&nodeapi.Asset{Base: e.cs.URL}).Spender(ctx, holdTxid, 0); by != saved {
		e.fail("the hold output is spent by %s, not the recorded settlement", by)
	}
	e.eventually("both providers record their payout", 2*time.Minute, func() bool {
		return pa.logged("pays rank") && pb.logged("pays rank")
	})
	e.ok("settle again: the same settlement %s sent, mined and remitted; %d and %d satoshis by square-root rank", saved[:12], pay[0], pay[1])

	// The owner anchors; the object reads back, every check made.
	out = e.must(owner, 0, "anchor")
	if !strings.Contains(out, "1 change(s)") {
		e.fail("anchor: %s", out)
	}
	got := filepath.Join(e.dir, "got.bin")
	// The owner's home holds no copy: every byte comes from the providers.
	e.must(owner, 0, "get", "photos/big.bin", got, "-proof-root")
	if b, _ := os.ReadFile(got); !bytes.Equal(b, content) {
		e.fail("get: the bytes differ")
	}
	if out := e.must(owner, 0, "ls", "photos/"); !strings.Contains(out, "photos/big.bin") {
		e.fail("ls: %s", out)
	}
	e.ok("the owner anchored; get read 100 MiB from the providers, content hash and proof root checked; ls proves the page")

	// A blob dropped by provider a, caught by a paid challenge of c = 113.
	blobs := blobHashes(t, content)
	drop := hex.EncodeToString(blobs[7][:])
	if err := os.Remove(filepath.Join(pa.data, "store", "objects", drop[:2], drop)); err != nil {
		t.Fatal(err)
	}
	caught := false
	for try := 0; try < 4 && !caught; try++ {
		out, code := e.cli(writer, "challenge", "photos/big.bin", "-provider", pa.identity, "-c", "113", "-budget", "1000")
		switch code {
		case 0:
			// No sample fell in the dropped blob (1 in 100): again.
		case 3:
			if !strings.Contains(out, "no-answer") || !strings.Contains(out, "unknown-root") || !strings.Contains(out, "outcome record") {
				e.fail("the caught challenge: %s", out)
			}
			caught = true
		default:
			e.fail("challenge exit %d: %s", code, out)
		}
	}
	if !caught {
		e.fail("four challenges of c = 113 missed a dropped blob")
	}
	logged, _ := os.ReadFile(filepath.Join(writer, "outcomes.jsonl"))
	if !bytes.Contains(logged, []byte(`"outcome":"no-answer"`)) || !bytes.Contains(logged, []byte(`"published":"`)) {
		e.fail("outcomes.jsonl: %s", logged)
	}
	e.ok("a dropped blob (1 of 25) caught by a paid challenge of c = 113: no-answer, recorded and published")
	if !strings.Contains(e.must(writer, 0, "challenge", "photos/big.bin", "-provider", pb.identity, "-c", "113", "-budget", "1000"), "pass") {
		e.fail("provider b's paid challenge did not pass")
	}
	e.ok("provider b passes the same paid challenge")

	// A renewal of provider b's advertisement, bought by a reader (the
	// owner's home, which wrote nothing of the object).
	out = e.must(owner, 0, "renew", "photos/big.bin", "-provider", pb.identity, "-days", "1", "-budget", "2000")
	if !strings.Contains(out, "successor ") || !strings.Contains(out, ": 200 (paid ") {
		e.fail("renew: %s", out)
	}
	e.ok("a reader bought a renewal of provider b's advertisement: the successor spends it, a day later")

	// A copy, rm, anchor, NoSuchKey.
	e.must(writer, 0, "cp", "photos/big.bin", "photos/copy.bin", "-no-bounty")
	e.must(writer, 0, "rm", "photos/big.bin")
	e.must(owner, 0, "anchor")
	if out, code := e.cli(writer, "get", "photos/big.bin", filepath.Join(e.dir, "gone.bin")); code != 1 || !strings.Contains(out, "NoSuchKey") {
		e.fail("get after rm: %d %s", code, out)
	}
	e.must(owner, 0, "get", "photos/copy.bin", got)
	if b, _ := os.ReadFile(got); !bytes.Equal(b, content) {
		e.fail("the copy's bytes differ")
	}
	e.ok("cp names the same content; rm and anchor: NoSuchKey (exit 1); the copy reads")

	// A bounty nobody can win (a deadline nearer than the providers' own
	// minimum): it lapses and is reclaimed.
	small := filepath.Join(e.dir, "small.txt")
	_ = os.WriteFile(small, []byte("a note nobody is paid to keep\n"), 0o600)
	out = e.must(writer, 0, "put", "notes/a.txt", small, "-bounty", "3000", "-t", "1", "-k", "1", "-deadline", "3")
	lone := field(out, "entry")
	loneHold, _, _ := strings.Cut(field(out, "hold"), ".")
	e.must(owner, 0, "fund", "-blocks", "4")
	e.must(writer, 0, "holds", "-entry", lone)
	if r := readRace(t, writer, lone); r.State != publish.RaceLapsed || len(r.Passed()) != 0 {
		e.fail("the lone race is %s", r.State)
	}
	if out, code := e.cli(writer, "due"); code != 3 || !strings.Contains(out, "reclaim") {
		e.fail("due before the reclaim: %d %s", code, out)
	}
	out = e.must(writer, 0, "reclaim", "-entry", lone)
	r := readRace(t, writer, lone)
	if r.State != publish.RaceReclaimd || r.Reclaim == nil {
		e.fail("reclaim: %s %s", r.State, out)
	}
	if by, _ := (&nodeapi.Asset{Base: e.cs.URL}).Spender(ctx, loneHold, 0); by != r.Reclaim.Txid {
		e.fail("the lone hold output is spent by %s", by)
	}
	if _, code := e.cli(writer, "settle", "-entry", lone); code == 0 && readRace(t, writer, lone).Settlement != nil {
		e.fail("a reclaimed hold output was settled")
	}
	e.ok("a bounty nobody won lapsed at its deadline and was reclaimed; it is never settled after")

	// The kill switch: an entry no token anchored is retracted.
	_ = os.WriteFile(small, []byte("pending, then swept\n"), 0o600)
	out = e.must(writer, 0, "put", "notes/pending.txt", small, "-no-bounty")
	pending := field(out, "entry")
	if !e.hostHas(bucket, pending) {
		e.fail("the host does not answer the pending entry")
	}
	if out := e.must(writer, 0, "kill", "-confirm", "KILL"); !strings.Contains(out, "nothing sent") {
		e.fail("kill without -yes: %s", out)
	}
	e.must(writer, 0, "kill", "-confirm", "KILL", "-yes")
	e.eventually("the host retracts the swept entry", time.Minute, func() bool { return !e.hostHas(bucket, pending) })
	if _, code := e.cli(writer, "put", "notes/after.txt", small, "-no-bounty"); code == 0 {
		e.fail("a put after the kill was published")
	}
	out = e.must(owner, 0, "anchor")
	if strings.Contains(out, pending) && !strings.Contains(out, "swept") {
		e.fail("the owner anchored a swept entry: %s", out)
	}
	if _, code := e.cli(writer, "get", "notes/pending.txt", filepath.Join(e.dir, "p.txt")); code != 1 {
		e.fail("the swept entry was anchored")
	}
	e.ok("kill swept every tree: the unanchored entry is retracted and never anchored; the identity publishes nothing more")

	pa.stop()
	pb.stop()
}

// pubProvider writes a provider's configuration for bucket on port.
func (e *pubRig) pubProvider(name string, port int, bucket string) *proc {
	data := filepath.Join(e.dir, "provider-"+name)
	out := e.run(e.pbin, "init", "-data", data)
	id := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(out), "identity"))
	cfg := map[string]any{
		"data": data, "listen": fmt.Sprintf("127.0.0.1:%d", port), "tls_cert": e.cert, "tls_key": e.key,
		"public_url": fmt.Sprintf("https://127.0.0.1:%d/store", port), "node": e.cs.URL, "settle": "rpc:" + e.cs.URL + "/rpc",
		"headers": e.cs.URL, "hosts": []string{e.host}, "buckets": []map[string]any{{"bucket": bucket}}, "tree_count": 8,
		"terms": map[string]any{"challenge_base": 10, "challenge_per_sample": 1, "free_c": 30, "min_bounty": 1000, "min_age": 2,
			"renew_per_gib_day": 100, "renew_minimum": 300},
		"limits": map[string]any{"challenge_burst": 20, "handshakes_per_addr_per_sec": 100, "handshake_addr_burst": 200,
			"object_reads_per_addr_per_sec": 1000, "object_read_burst": 1000, "submits_per_addr_per_sec": 1000, "submit_burst": 2000},
	}
	raw, _ := json.MarshalIndent(cfg, "", "  ")
	path := filepath.Join(e.dir, "provider-"+name+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		e.t.Fatal(err)
	}
	f, _ := os.Create(filepath.Join(e.dir, "provider-"+name+".log"))
	return &proc{name: "provider " + name, data: data, config: path, identity: id, out: &lockedBuf{f: f}, bin: e.pbin,
		args: []string{"run", "-config", path}, direct: fmt.Sprintf("https://127.0.0.1:%d", port)}
}

// killingLeg is the chain's RPC behind a proxy: a transaction that hit
// answers true for is not forwarded, and kill runs.
func (e *pubRig) killingLeg(hit func(raw []byte) bool, kill func()) string {
	target, _ := url.Parse(e.cs.URL)
	rp := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		var call struct {
			Method string `json:"method"`
			Params []any  `json:"params"`
		}
		if json.Unmarshal(body, &call) == nil && call.Method == "sendrawtransaction" && len(call.Params) > 0 {
			rawHex, _ := call.Params[0].(string)
			if raw, err := hex.DecodeString(rawHex); err == nil {
				mu.Lock()
				h := hit(raw)
				if h {
					kill()
				}
				mu.Unlock()
				if h {
					time.Sleep(200 * time.Millisecond)
					http.Error(w, "gone", http.StatusBadGateway)
					return
				}
			}
		}
		rp.ServeHTTP(w, r)
	}))
	e.t.Cleanup(srv.Close)
	return "rpc:" + srv.URL + "/rpc"
}

// hostHas reports whether the host answers an entry by its txid.
func (e *pubRig) hostHas(bucket, txid string) bool {
	q, _ := json.Marshal(map[string]any{"service": storerec.LookupService, "query": map[string]string{"bucket": bucket, "entry": txid}})
	res, err := http.Post(e.host+"/lookup", "application/json", bytes.NewReader(q))
	if err != nil {
		return false
	}
	defer res.Body.Close()
	var a struct {
		Outputs []json.RawMessage `json:"outputs"`
	}
	b, _ := io.ReadAll(res.Body)
	return json.Unmarshal(b, &a) == nil && len(a.Outputs) > 0
}

func readRace(t *testing.T, home, entry string) *publish.Race {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(home, "races", entry+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var r publish.Race
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

func blobHashes(t *testing.T, content []byte) [][32]byte {
	var out [][32]byte
	for i := 0; i < len(content); i += storerec.BlobSize {
		out = append(out, sha256.Sum256(content[i:min(i+storerec.BlobSize, len(content))]))
	}
	return out
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
