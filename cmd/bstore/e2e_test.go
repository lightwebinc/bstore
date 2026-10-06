package main

// The S3 gateway end to end, on a reference overlay host loading the
// bstore module and a local test chain (bcommon testchain), with two
// bstore-provider processes, a test-only bucket owner
// (internal/testwriter) and the AWS CLI and Terraform as the clients:
//
//	BSTORE_E2E_REFHOST=/path/to/reference-host BSTORE_E2E_NODE=/path/to/node24 \
//	  go test -run TestS3E2E -v ./cmd/bstore/
//
// make s3-e2e runs it. It needs Docker for the host's MySQL and the aws
// command; terraform (or tofu) when present. It puts an object with
// metadata (and one whose Content-MD5 is wrong), reads its own write at
// once, uploads a 13 MiB object in three parts, refuses a conditional
// write, deletes a key and reads NoSuchKey; the owner anchors; the
// providers advertise; it reads a range that crosses a blob boundary,
// restarts the gateway with its local copies gone and reads again while
// one provider serves corrupt bytes (skipped and named); copies within a
// bucket and across buckets; lists 300 keys over two host pages, with a
// delimiter and with pagination; and runs Terraform's S3 backend, which
// writes its state and gets a clean 501 for use_lockfile.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/md5"
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
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

const (
	bucketA = "example_photos_hmqzvtkwrx"
	bucketB = "example_backup_qwhzkmvtrx"
	akid    = "BSTOREE2EACCESSKEY"
	secret  = "bstore-e2e-secret-key-0123456789abcdef"
)

type rig struct {
	t      *testing.T
	ctx    context.Context
	dir    string
	cs     *httptest.Server
	client *http.Client
	cert   string
	key    string
	pbin   string
	bin    string
	host   string
}

func TestS3E2E(t *testing.T) {
	refhost := os.Getenv("BSTORE_E2E_REFHOST")
	node := os.Getenv("BSTORE_E2E_NODE")
	if refhost == "" || node == "" {
		t.Skip("set BSTORE_E2E_REFHOST and BSTORE_E2E_NODE (make s3-e2e)")
	}
	if _, err := exec.LookPath("aws"); err != nil {
		t.Skip("the aws command is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	e := &rig{t: t, ctx: ctx, dir: t.TempDir()}
	if d := os.Getenv("BSTORE_E2E_LOGS"); d != "" {
		e.dir = d
		_ = os.MkdirAll(d, 0o700)
	}
	chain := testchain.New(700)
	e.cs = httptest.NewServer(chain)
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

	// The owner, and the gateway's identity, which the owner's chains name
	// as a writer.
	hc, _ := headers.NewSource(e.cs.URL)
	rpc := &nodeapi.RPC{URL: e.cs.URL + "/rpc", ID: "owner"}
	asset := &nodeapi.Asset{Base: e.cs.URL}
	owner, err := testwriter.New(ctx, filepath.Join(e.dir, "owner"), bucketA, rpc, asset, hc, e.client)
	if err != nil {
		t.Fatal(err)
	}
	gwPort := freePort(t)
	gwCfg := filepath.Join(e.dir, "s3d.json")
	gwData := filepath.Join(e.dir, "s3d")
	e.writeGateway(gwCfg, gwData, gwPort, owner.Identity(), [32]byte{}, [32]byte{}, nil, "")
	out := e.run(e.bin, "s3d", "init", "-config", gwCfg)
	f := strings.Fields(out)
	if len(f) < 3 {
		t.Fatalf("init: %s", out)
	}
	gwKey, _ := hex.DecodeString(f[2])
	chainA, tokA, err := owner.NewChain(ctx, bucketA, [][]byte{gwKey})
	if err != nil {
		t.Fatal(err)
	}
	chainB, tokB, err := owner.NewChain(ctx, bucketB, [][]byte{gwKey})
	if err != nil {
		t.Fatal(err)
	}
	served := []string{
		bucketA + ":" + hex.EncodeToString(owner.Identity()) + ":" + hex.EncodeToString(chainA.ID[:]),
		bucketB + ":" + hex.EncodeToString(owner.Identity()) + ":" + hex.EncodeToString(chainB.ID[:]),
	}
	e.host = e.startHost(refhost, node, []string{bucketA, bucketB}, served)
	e.ok("the reference host loads the bstore module and serves two chains")
	e.submit(owner, storerec.RecordsTopic(bucketA), tokA, "chain A's first token at the host")
	e.submit(owner, storerec.RecordsTopic(bucketB), tokB, "chain B's first token at the host")

	// Two providers; the one whose key sorts first (the one a reader asks
	// first) sits behind a proxy that can be made to corrupt blobs.
	pa, pb := e.provider("a"), e.provider("b")
	bad, good := pa, pb
	if pb.identity < pa.identity {
		bad, good = pb, pa
	}
	corrupt := e.corrupting()
	e.configProvider(good, 0)
	e.configProvider(bad, corrupt.port)
	u, _ := url.Parse(bad.direct)
	corrupt.target.Store(u)
	for _, p := range []*proc{pa, pb} {
		e.run(e.pbin, "fund", "-config", p.config, "-blocks", "104")
		p.start(e, p.direct+"/healthz")
	}
	e.ok("two providers run; %s is behind a proxy that can corrupt what it serves", bad.name)

	// The gateway.
	e.writeGateway(gwCfg, gwData, gwPort, owner.Identity(), chainA.ID, chainB.ID, []string{pa.direct, pb.direct}, e.host)
	e.run(e.bin, "s3d", "fund", "-config", gwCfg, "-identity", "main", "-blocks", "104")
	gw := e.gateway(gwCfg)
	endpoint := fmt.Sprintf("https://127.0.0.1:%d", gwPort)
	gw.start(e, endpoint+"/healthz")
	e.ok("the gateway serves S3 over TLS for identity %s", f[2][:12])
	a := e.awsCLI(endpoint)
	s3A := strings.ReplaceAll(bucketA, "_", "-")
	s3B := strings.ReplaceAll(bucketB, "_", "-")

	// ListBuckets, HeadBucket, GetBucketLocation.
	var lb struct {
		Buckets []struct{ Name string }
	}
	a.json(&lb, "s3api", "list-buckets")
	if len(lb.Buckets) != 2 || lb.Buckets[0].Name != s3B || lb.Buckets[1].Name != s3A {
		e.fail("list-buckets: %+v", lb)
	}
	a.ok("s3api", "head-bucket", "--bucket", s3A)
	var loc struct{ LocationConstraint *string }
	a.json(&loc, "s3api", "get-bucket-location", "--bucket", s3A)
	e.ok("ListBuckets, HeadBucket and GetBucketLocation answer")

	// PutObject with metadata, and a Content-MD5 that does not match.
	hello := filepath.Join(e.dir, "hello.txt")
	helloBytes := []byte("hello, bstore: an object put through S3\n")
	_ = os.WriteFile(hello, helloBytes, 0o600)
	var put struct{ ETag string }
	a.json(&put, "s3api", "put-object", "--bucket", s3A, "--key", "docs/hello.txt", "--body", hello,
		"--content-type", "Text/Plain; charset=utf-8", "--metadata", "color=blue,shape=round")
	sum := md5.Sum(helloBytes)
	if put.ETag != `"`+hex.EncodeToString(sum[:])+`"` {
		e.fail("put-object ETag %s", put.ETag)
	}
	e.ok("PutObject: 200, ETag the body's MD5")
	if out := a.fails("s3api", "put-object", "--bucket", s3A, "--key", "docs/bad.txt", "--body", hello,
		"--content-md5", "AAAAAAAAAAAAAAAAAAAAAA=="); !strings.Contains(out, "BadDigest") {
		e.fail("a wrong Content-MD5: %s", out)
	}
	e.ok("a Content-MD5 that does not match is BadDigest")

	// Read your writes, before any token anchors the entry.
	var head struct {
		ContentLength int64
		ETag          string
		ContentType   string
		Metadata      map[string]string
	}
	a.json(&head, "s3api", "head-object", "--bucket", s3A, "--key", "docs/hello.txt")
	if head.ContentLength != int64(len(helloBytes)) || head.ETag != put.ETag || head.ContentType != "text/plain" ||
		head.Metadata["color"] != "blue" || head.Metadata["shape"] != "round" || len(head.Metadata["bstore-root"]) != 64 {
		e.fail("head-object of a pending put: %+v", head)
	}
	got := a.get(s3A, "docs/hello.txt", "")
	if !bytes.Equal(got, helloBytes) {
		e.fail("get-object of a pending put")
	}
	e.ok("the writing gateway reads its own pending write at once (HeadObject, GetObject)")

	// Multipart: 13 MiB in parts of 5, 5 and 3 MiB.
	big := make([]byte, 13<<20)
	h := sha256.Sum256([]byte("bstore s3 e2e"))
	for i := 0; i < len(big); i += 32 {
		copy(big[i:], h[:])
		h = sha256.Sum256(h[:])
	}
	bigPath := filepath.Join(e.dir, "big.bin")
	_ = os.WriteFile(bigPath, big, 0o600)
	a.ok("s3", "cp", bigPath, "s3://"+s3A+"/big/blob.bin")
	a.json(&head, "s3api", "head-object", "--bucket", s3A, "--key", "big/blob.bin")
	if head.ContentLength != int64(len(big)) || !strings.HasSuffix(head.ETag, `-3"`) {
		e.fail("the multipart object: %+v", head)
	}
	e.ok("a 13 MiB multipart upload in three parts: ETag %s", head.ETag)

	// A conditional write is refused, never ignored.
	if out := a.fails("s3api", "put-object", "--bucket", s3A, "--key", "docs/lock", "--body", hello, "--if-none-match", "*"); !strings.Contains(out, "NotImplemented") {
		e.fail("a conditional write: %s", out)
	}
	e.ok("PutObject with If-None-Match: 501 NotImplemented")

	// Delete, then NoSuchKey at the writing gateway.
	a.ok("s3api", "put-object", "--bucket", s3A, "--key", "docs/gone.txt", "--body", hello)
	a.ok("s3api", "delete-object", "--bucket", s3A, "--key", "docs/gone.txt")
	if out := a.fails("s3api", "head-object", "--bucket", s3A, "--key", "docs/gone.txt"); !strings.Contains(out, "404") {
		e.fail("head after delete: %s", out)
	}
	if out := a.fails("s3api", "get-object", "--bucket", s3A, "--key", "docs/gone.txt", filepath.Join(e.dir, "gone.out")); !strings.Contains(out, "NoSuchKey") {
		e.fail("get after delete: %s", out)
	}
	e.ok("DeleteObject, then NoSuchKey (the gateway's own pending delete)")

	// The owner anchors what the gateway wrote.
	e.anchor(owner, chainA, gwKey)
	e.ok("the owner anchored chain A")

	// The providers advertise both roots.
	helloRoot := e.rootOf(a, s3A, "docs/hello.txt")
	bigRoot := e.rootOf(a, s3A, "big/blob.bin")
	for _, r := range [][32]byte{helloRoot, bigRoot} {
		e.eventually("both providers advertise", 3*time.Minute, func() bool {
			ads, err := owner.Ads(ctx, e.host, r)
			return err == nil && len(ads) >= 2
		})
	}
	e.ok("both providers received, checked and advertised both objects")

	// A range across the first blob boundary.
	got = a.get(s3A, "big/blob.bin", "bytes=4194000-4195000")
	if !bytes.Equal(got, big[4194000:4195001]) {
		e.fail("a range across the blob boundary")
	}
	got = a.get(s3A, "big/blob.bin", "bytes=-100")
	if !bytes.Equal(got, big[len(big)-100:]) {
		e.fail("a suffix range")
	}
	if out := a.fails("s3api", "get-object", "--bucket", s3A, "--key", "big/blob.bin", "--range", "bytes=99999999-", filepath.Join(e.dir, "r.out")); !strings.Contains(out, "InvalidRange") {
		e.fail("a range past the end: %s", out)
	}
	e.ok("GetObject with Range: across a blob boundary, a suffix, and 416 past the end")

	// Restart the gateway without its local copies; one provider now
	// serves corrupt bytes.
	gw.stop()
	if err := os.RemoveAll(filepath.Join(gwData, "store")); err != nil {
		t.Fatal(err)
	}
	gw.start(e, endpoint+"/healthz")
	corrupt.armed.Store(true)
	got = a.get(s3A, "big/blob.bin", "")
	if !bytes.Equal(got, big) {
		e.fail("the whole multipart object after the restart")
	}
	if !bytes.Equal(a.get(s3A, "docs/hello.txt", ""), helloBytes) {
		e.fail("the small object after the restart")
	}
	if corrupt.mangled.Load() == 0 || !gw.logged("served bytes that do not hash") || !gw.logged(bad.identity[:12]) {
		e.fail("the corrupting provider was not asked, or not named:\n%s", gw.log())
	}
	e.ok("after a restart with no local copy, every byte came from the providers; %s served corrupt bytes %d time(s), was skipped and named", bad.name, corrupt.mangled.Load())
	corrupt.armed.Store(false)

	// CopyObject within the bucket (COPY and REPLACE) and across buckets.
	var cp struct {
		CopyObjectResult struct{ ETag string }
	}
	a.json(&cp, "s3api", "copy-object", "--bucket", s3A, "--key", "docs/copy.txt", "--copy-source", s3A+"/docs/hello.txt")
	a.json(&head, "s3api", "head-object", "--bucket", s3A, "--key", "docs/copy.txt")
	if cp.CopyObjectResult.ETag != put.ETag || head.ETag != put.ETag || head.Metadata["color"] != "blue" || head.ContentType != "text/plain" {
		e.fail("copy (COPY): %+v %+v", cp, head)
	}
	a.ok("s3api", "copy-object", "--bucket", s3A, "--key", "docs/copy2.txt", "--copy-source", s3A+"/docs/hello.txt",
		"--metadata-directive", "REPLACE", "--content-type", "application/json", "--metadata", "kind=replaced")
	a.json(&head, "s3api", "head-object", "--bucket", s3A, "--key", "docs/copy2.txt")
	if head.ETag != put.ETag || head.ContentType != "application/json" || head.Metadata["kind"] != "replaced" || head.Metadata["color"] != "" {
		e.fail("copy (REPLACE): %+v", head)
	}
	// The two copies are pending: the listing merges them with the proven
	// page.
	var pl struct{ Contents []struct{ Key string } }
	a.json(&pl, "s3api", "list-objects-v2", "--bucket", s3A, "--prefix", "docs/")
	if len(pl.Contents) != 3 || pl.Contents[0].Key != "docs/copy.txt" || pl.Contents[1].Key != "docs/copy2.txt" || pl.Contents[2].Key != "docs/hello.txt" {
		e.fail("a listing with pending entries: %+v", pl.Contents)
	}
	e.ok("ListObjectsV2 at the writing gateway merges its pending copies with the proven page")
	a.ok("s3api", "copy-object", "--bucket", s3B, "--key", "copied/big.bin", "--copy-source", s3A+"/big/blob.bin")
	e.anchor(owner, chainB, gwKey)
	e.eventually("providers advertise the copy in bucket B", 3*time.Minute, func() bool {
		ads, err := (&testwriter.Writer{HTTP: e.client, Bucket: bucketB}).Ads(ctx, e.host, bigRoot)
		return err == nil && len(ads) >= 2
	})
	gw.stop()
	_ = os.RemoveAll(filepath.Join(gwData, "store"))
	gw.start(e, endpoint+"/healthz")
	if !bytes.Equal(a.get(s3B, "copied/big.bin", ""), big) {
		e.fail("the copy across buckets")
	}
	e.ok("CopyObject: COPY keeps type, ETag and metadata; REPLACE takes the request's; across buckets the pieces are published again and the copy is read from bucket B's providers")

	// Listing: 300 more keys over two host pages, a delimiter, pagination.
	small, _ := storerec.Build([]byte("x"))
	var keys []string
	for i := 0; i < 300; i++ {
		keys = append(keys, fmt.Sprintf("many/k%03d", i))
	}
	beefs, err := chainA.Entries(ctx, keys, small)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range beefs {
		e.submit(owner, storerec.RecordsTopic(bucketA), b, "")
	}
	e.anchor(owner, chainA, gwKey)
	var lst struct {
		Contents []struct {
			Key  string
			Size int64
			ETag string
		}
		CommonPrefixes []struct{ Prefix string }
		NextToken      string
	}
	a.json(&lst, "s3api", "list-objects-v2", "--bucket", s3A, "--prefix", "many/", "--page-size", "120")
	if len(lst.Contents) != 300 || lst.Contents[0].Key != "many/k000" || lst.Contents[299].Key != "many/k299" {
		e.fail("list of 300 keys: %d", len(lst.Contents))
	}
	for i := 1; i < len(lst.Contents); i++ {
		if lst.Contents[i-1].Key >= lst.Contents[i].Key {
			e.fail("the listing is not in key order")
		}
	}
	a.json(&lst, "s3api", "list-objects-v2", "--bucket", s3A, "--prefix", "many/", "--max-items", "130", "--page-size", "50")
	if len(lst.Contents) != 130 || lst.NextToken == "" {
		e.fail("a page of 130 with a next token: %d %q", len(lst.Contents), lst.NextToken)
	}
	next := lst.NextToken
	a.json(&lst, "s3api", "list-objects-v2", "--bucket", s3A, "--prefix", "many/", "--starting-token", next, "--page-size", "1000")
	if len(lst.Contents) != 170 || lst.Contents[0].Key != "many/k130" {
		e.fail("the rest after the token: %d", len(lst.Contents))
	}
	a.json(&lst, "s3api", "list-objects-v2", "--bucket", s3A, "--delimiter", "/")
	var cps []string
	for _, c := range lst.CommonPrefixes {
		cps = append(cps, c.Prefix)
	}
	sort.Strings(cps)
	if strings.Join(cps, ",") != "big/,docs/,many/" || len(lst.Contents) != 0 {
		e.fail("the delimiter: %v %d", cps, len(lst.Contents))
	}
	a.json(&lst, "s3api", "list-objects-v2", "--bucket", s3A, "--prefix", "docs/", "--start-after", "docs/copy.txt")
	if len(lst.Contents) != 2 || lst.Contents[0].Key != "docs/copy2.txt" || lst.Contents[1].Key != "docs/hello.txt" {
		e.fail("start-after: %+v", lst.Contents)
	}
	if out := a.ok("s3", "ls", "s3://"+s3A+"/docs/"); !strings.Contains(out, "hello.txt") || strings.Contains(out, "gone.txt") {
		e.fail("s3 ls: %s", out)
	}
	e.ok("ListObjectsV2: 300 keys over two host pages, each proven; pagination by token; a delimiter; start-after; s3 ls")

	// Proven absence after anchoring.
	if out := a.fails("s3api", "head-object", "--bucket", s3A, "--key", "docs/gone.txt"); !strings.Contains(out, "404") {
		e.fail("head of an anchored delete: %s", out)
	}
	if out := a.fails("s3api", "get-object", "--bucket", s3A, "--key", "never/there", filepath.Join(e.dir, "n.out")); !strings.Contains(out, "NoSuchKey") {
		e.fail("get of a key never written: %s", out)
	}
	e.ok("NoSuchKey from the map's proof of absence")

	// Terraform's S3 backend.
	e.terraform(endpoint, s3A)

	gw.stop()
	pa.stop()
	pb.stop()
}

// anchor mints chain c's next token from every entry the gateway's
// identity and the owner wrote that no token names, and submits it.
func (e *rig) anchor(owner *testwriter.Writer, c *testwriter.Chain, writer []byte) {
	var batch []storerec.Applicable
	for _, w := range [][]byte{writer, owner.Identity()} {
		got, err := c.Unanchored(e.ctx, e.host, w)
		if err != nil {
			e.fail("unanchored: %v", err)
		}
		batch = append(batch, got...)
	}
	beef, err := c.Anchor(e.ctx, batch)
	if err != nil {
		e.fail("anchor: %v", err)
	}
	e.submit(owner, storerec.RecordsTopic(c.Bucket), beef, "")
}

func (e *rig) rootOf(a *awsCLI, bucket, key string) [32]byte {
	var head struct{ Metadata map[string]string }
	a.json(&head, "s3api", "head-object", "--bucket", bucket, "--key", key)
	b, err := hex.DecodeString(head.Metadata["bstore-root"])
	if err != nil || len(b) != 32 {
		e.fail("no bstore-root for %s", key)
	}
	return [32]byte(b)
}

// terraform runs Terraform's S3 backend against the gateway: without a
// lock file it writes its state; with use_lockfile its conditional write
// gets a clean 501.
func (e *rig) terraform(endpoint, bucket string) {
	bin := ""
	for _, n := range []string{"terraform", "tofu"} {
		if p, err := exec.LookPath(n); err == nil {
			bin = p
			break
		}
	}
	if bin == "" {
		e.t.Log("skip  terraform: neither terraform nor tofu is installed")
		return
	}
	for _, lock := range []bool{false, true} {
		dir := filepath.Join(e.dir, fmt.Sprintf("tf-%v", lock))
		_ = os.MkdirAll(dir, 0o700)
		cfg := fmt.Sprintf(`terraform {
  backend "s3" {
    bucket                      = %q
    key                         = "state/%v.tfstate"
    region                      = "us-east-1"
    endpoints                   = { s3 = %q }
    use_path_style              = true
    skip_credentials_validation = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    skip_metadata_api_check     = true
    use_lockfile                = %v
  }
}
output "hello" { value = "bstore" }
`, bucket, lock, endpoint, lock)
		_ = os.WriteFile(filepath.Join(dir, "main.tf"), []byte(cfg), 0o600)
		env := append(e.awsEnv(), "TF_IN_AUTOMATION=1", "CHECKPOINT_DISABLE=1", "TF_INPUT=0", "HOME="+dir)
		run := func(args ...string) (string, error) {
			c := exec.CommandContext(e.ctx, bin, args...)
			c.Dir, c.Env = dir, env
			out, err := c.CombinedOutput()
			return string(out), err
		}
		if out, err := run("init", "-no-color"); err != nil {
			e.fail("%s init (lock %v): %v\n%s", filepath.Base(bin), lock, err, out)
		}
		out, err := run("apply", "-auto-approve", "-no-color", "-lock-timeout=0s")
		if !lock {
			if err != nil {
				e.fail("%s apply: %v\n%s", filepath.Base(bin), err, out)
			}
			e.ok("%s's S3 backend wrote its state through the gateway", filepath.Base(bin))
			continue
		}
		if err == nil || !strings.Contains(out, "NotImplemented") {
			e.fail("%s apply with use_lockfile: %v\n%s", filepath.Base(bin), err, out)
		}
		e.ok("%s's S3 backend with use_lockfile: a clean 501 NotImplemented, no lock believed held", filepath.Base(bin))
	}
}

// ---- the AWS CLI ----

type awsCLI struct {
	e        *rig
	endpoint string
}

func (e *rig) awsEnv() []string {
	cfg := filepath.Join(e.dir, "aws-config")
	_ = os.WriteFile(cfg, []byte("[default]\nregion = us-east-1\ns3 =\n  multipart_threshold = 8MB\n  multipart_chunksize = 5MB\n  addressing_style = path\n"), 0o600)
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + e.dir, "AWS_CONFIG_FILE=" + cfg,
		"AWS_SHARED_CREDENTIALS_FILE=" + filepath.Join(e.dir, "no-credentials"), "AWS_ACCESS_KEY_ID=" + akid,
		"AWS_SECRET_ACCESS_KEY=" + secret, "AWS_DEFAULT_REGION=us-east-1", "AWS_REGION=us-east-1", "AWS_CA_BUNDLE=" + e.cert,
		"AWS_EC2_METADATA_DISABLED=true", "AWS_PAGER=", "AWS_MAX_ATTEMPTS=2"}
}

func (e *rig) awsCLI(endpoint string) *awsCLI { return &awsCLI{e: e, endpoint: endpoint} }

func (a *awsCLI) exec(args ...string) (string, error) {
	c := exec.CommandContext(a.e.ctx, "aws", append([]string{"--endpoint-url", a.endpoint, "--output", "json"}, args...)...)
	c.Env = a.e.awsEnv()
	out, err := c.CombinedOutput()
	return string(out), err
}

func (a *awsCLI) ok(args ...string) string {
	a.e.t.Helper()
	out, err := a.exec(args...)
	if err != nil {
		a.e.fail("aws %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func (a *awsCLI) fails(args ...string) string {
	a.e.t.Helper()
	out, err := a.exec(args...)
	if err == nil {
		a.e.fail("aws %s succeeded:\n%s", strings.Join(args, " "), out)
	}
	return out
}

func (a *awsCLI) json(v any, args ...string) {
	a.e.t.Helper()
	out := a.ok(args...)
	reflect.ValueOf(v).Elem().SetZero()
	if err := json.Unmarshal([]byte(out), v); err != nil {
		a.e.fail("aws %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func (a *awsCLI) get(bucket, key, rng string) []byte {
	a.e.t.Helper()
	f := filepath.Join(a.e.dir, "get.out")
	_ = os.Remove(f)
	args := []string{"s3api", "get-object", "--bucket", bucket, "--key", key}
	if rng != "" {
		args = append(args, "--range", rng)
	}
	a.ok(append(args, f)...)
	b, err := os.ReadFile(f)
	if err != nil {
		a.e.fail("get %s: %v", key, err)
	}
	return b
}

// ---- processes ----

type proc struct {
	name     string
	data     string
	config   string
	direct   string
	identity string
	cmd      *exec.Cmd
	out      *lockedBuf
	done     chan error
	args     []string
	bin      string
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

func (p *proc) start(e *rig, health string) {
	p.cmd = exec.Command(p.bin, p.args...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	p.done = make(chan error, 1)
	cmd := p.cmd
	go func() { p.done <- cmd.Wait() }()
	e.eventually(p.name+" listens", time.Minute, func() bool {
		res, err := e.client.Get(health)
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == 200
	})
	e.t.Cleanup(func() { _ = cmd.Process.Kill() })
}

func (p *proc) stop() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(30 * time.Second):
			_ = p.cmd.Process.Kill()
		}
	}
}

func (e *rig) provider(name string) *proc {
	data := filepath.Join(e.dir, "provider-"+name)
	out := e.run(e.pbin, "init", "-data", data)
	id := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(out), "identity"))
	f, _ := os.Create(filepath.Join(e.dir, "provider-"+name+".log"))
	cfg := filepath.Join(e.dir, "provider-"+name+".json")
	return &proc{name: "provider " + name, data: data, config: cfg, identity: id, out: &lockedBuf{f: f}, bin: e.pbin,
		args: []string{"run", "-config", cfg}}
}

// configProvider writes a provider's configuration; its public URL is its
// own listener, or the port given (a proxy in front of it).
func (e *rig) configProvider(p *proc, publicPort int) {
	port := freePort(e.t)
	pub := port
	if publicPort != 0 {
		pub = publicPort
	}
	p.direct = fmt.Sprintf("https://127.0.0.1:%d", port)
	cfg := map[string]any{
		"data": p.data, "listen": fmt.Sprintf("127.0.0.1:%d", port), "tls_cert": e.cert, "tls_key": e.key,
		"public_url": fmt.Sprintf("https://127.0.0.1:%d/store", pub), "node": e.cs.URL, "settle": "rpc:" + e.cs.URL + "/rpc",
		"headers": e.cs.URL, "hosts": []string{e.host},
		"buckets":    []map[string]any{{"bucket": bucketA}, {"bucket": bucketB}},
		"tree_count": 8,
		"limits":     map[string]any{"object_reads_per_addr_per_sec": 1000, "object_read_burst": 1000, "submits_per_addr_per_sec": 1000, "submit_burst": 2000},
	}
	raw, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(p.config, raw, 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func (e *rig) writeGateway(path, data string, port int, owner []byte, idA, idB [32]byte, providers []string, host string) {
	if host == "" {
		host = "http://127.0.0.1:1"
	}
	if providers == nil {
		providers = []string{"https://127.0.0.1:1"}
	}
	cfg := map[string]any{
		"data": data, "listen": fmt.Sprintf("127.0.0.1:%d", port), "tls_cert": e.cert, "tls_key": e.key,
		"node": e.cs.URL, "settle": "rpc:" + e.cs.URL + "/rpc", "headers": e.cs.URL,
		"hosts": []string{host}, "providers": providers, "ca": e.cert, "fetch_private": true,
		"identities":  []string{"main"},
		"access_keys": []map[string]any{{"id": akid, "secret": secret, "identity": "main"}},
		"buckets": []map[string]any{
			{"bucket": bucketA, "owner": hex.EncodeToString(owner), "id": hex.EncodeToString(idA[:])},
			{"bucket": bucketB, "owner": hex.EncodeToString(owner), "id": hex.EncodeToString(idB[:])},
		},
		"bounty":     map[string]any{"satoshis": 2000, "t": 1, "k": 2, "retention_seconds": 86400, "deadline_blocks": 600, "check_c": 30},
		"piece_tree": 8, "entry_tree": 8,
		"limits": map[string]any{"requests_per_sec": 1000, "burst": 2000},
	}
	raw, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func (e *rig) gateway(cfg string) *proc {
	f, _ := os.Create(filepath.Join(e.dir, "s3d.log"))
	return &proc{name: "the gateway", config: cfg, out: &lockedBuf{f: f}, bin: e.bin, args: []string{"s3d", "run", "-config", cfg}}
}

// corruptServer is a TLS reverse proxy in front of a provider that, when
// armed, flips a byte of every object it serves.
type corruptServer struct {
	port    int
	target  atomic.Pointer[url.URL]
	armed   atomic.Bool
	mangled atomic.Int64
}

func (e *rig) corrupting() *corruptServer {
	c := &corruptServer{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.t.Fatal(err)
	}
	c.port = ln.Addr().(*net.TCPAddr).Port
	cert, err := tls.LoadX509KeyPair(e.cert, e.key)
	if err != nil {
		e.t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := c.target.Load()
		if t == nil {
			http.Error(w, "no target", 502)
			return
		}
		rp := httputil.NewSingleHostReverseProxy(t)
		rp.Transport = e.client.Transport
		rp.ModifyResponse = func(res *http.Response) error {
			if !c.armed.Load() || r.Method != http.MethodGet || res.StatusCode != 200 || !strings.Contains(r.URL.Path, "/chirp/v1/") {
				return nil
			}
			b, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				return err
			}
			if len(b) > 0 {
				b[len(b)/2] ^= 0xff
				c.mangled.Add(1)
			}
			res.Body = io.NopCloser(bytes.NewReader(b))
			return nil
		}
		rp.ServeHTTP(w, r)
	})}
	go func() { _ = srv.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})) }()
	e.t.Cleanup(func() { _ = srv.Close() })
	return c
}

func (e *rig) submit(w *testwriter.Writer, topic string, beef []byte, what string) {
	ans, err := w.Submit(e.ctx, e.host, topic, beef)
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

func (e *rig) run(bin string, args ...string) string {
	e.t.Helper()
	out, err := exec.CommandContext(e.ctx, bin, args...).CombinedOutput()
	if err != nil {
		e.fail("%s %s: %v\n%s", filepath.Base(bin), strings.Join(args, " "), err, out)
	}
	return string(out)
}

func (e *rig) startHost(refhost, node string, buckets, served []string) string {
	return e.startHostAt(refhost, node, buckets, served, freePort(e.t))
}

// startHostAt starts the reference host on port.
func (e *rig) startHostAt(refhost, node string, buckets, served []string, port int) string {
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
	var topics []string
	for _, b := range buckets {
		topics = append(topics, storerec.RecordsTopic(b))
	}
	cmd := exec.Command(node, filepath.Join(refhost, "dist", "index.js"))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"),
		"OVERLAY_TOPICS=" + strings.Join(topics, ","), "OVERLAY_MODULES=" + mod,
		"OVERLAY_KNEX_URL=mysql://root:" + pw + "@127.0.0.1:" + dbPort + "/overlay",
		"OVERLAY_CHAIN_TRACKER_URL=" + e.cs.URL, "OVERLAY_ADMIN_TOKEN=bstore-e2e", "OVERLAY_LISTEN=127.0.0.1",
		"OVERLAY_PORT=" + strconv.Itoa(port), "BSTORE_BUCKETS=" + strings.Join(buckets, ","), "BSTORE_STATE_DIR=" + filepath.Join(stage, "state"),
		"BSTORE_HEADERS_URL=" + e.cs.URL, "BSTORE_SERVED=" + strings.Join(served, ",")}
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

func (e *rig) ok(format string, args ...any) { e.t.Logf("ok  "+format, args...) }

func (e *rig) fail(format string, args ...any) {
	e.t.Helper()
	e.t.Fatalf("e2e: "+format+"\n(logs under %s)", append(args, e.dir)...)
}

func (e *rig) eventually(what string, d time.Duration, f func() bool) {
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

func writeCert(t *testing.T, dir string) (string, string, *x509.CertPool) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "bstore e2e"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(k)
	cp, kp := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	_ = os.WriteFile(cp, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(kp, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
	c, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(c)
	return cp, kp, pool
}
