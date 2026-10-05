package provider

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/testwriter"
)

const testBucket = "photos_qzvmkwerty"

// rig is a local chain, a TLS certificate for 127.0.0.1, and a writer.
type rig struct {
	t      *testing.T
	chain  *testchain.Chain
	srv    *httptest.Server
	cert   string
	key    string
	client *http.Client
	writer *testwriter.Writer

	mu   sync.Mutex
	logs []string
}

func newRig(t *testing.T) *rig {
	t.Helper()
	chain := testchain.New(700)
	srv := httptest.NewServer(chain)
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	cert, key, pool := testCert(t, dir)
	client := &http.Client{Timeout: time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	r := &rig{t: t, chain: chain, srv: srv, cert: cert, key: key, client: client}
	hc, err := headers.NewSource(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := testwriter.New(context.Background(), filepath.Join(dir, "writer"), testBucket,
		&nodeapi.RPC{URL: srv.URL + "/rpc", ID: "w"}, &nodeapi.Asset{Base: srv.URL}, hc, client)
	if err != nil {
		t.Fatal(err)
	}
	r.writer = w
	return r
}

func (r *rig) logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func (r *rig) said() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.logs, "\n")
}

// config is a provider's configuration on a free port, its data in dir.
func (r *rig) config(dir string) *Config {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		r.t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	c := &Config{Data: dir, Listen: fmt.Sprintf("127.0.0.1:%d", port), TLSCert: r.cert, TLSKey: r.key,
		PublicURL: fmt.Sprintf("https://127.0.0.1:%d/store", port), Node: r.srv.URL, Settle: "rpc:" + r.srv.URL + "/rpc",
		Headers: r.srv.URL, Buckets: []BucketConfig{{Bucket: testBucket}}, TreeCount: 4,
		Limits: Limits{ChallengeBurst: 20},
		Terms:  Terms{ChallengeBase: 10, ChallengePerSample: 1, FreeC: 30, MinBounty: 1000, MinAge: 1, RenewPerGiBDay: 100, RenewMinimum: 300}}
	return c
}

// start funds and starts a provider on cfg.
func (r *rig) start(cfg *Config, fund bool) *Provider {
	r.t.Helper()
	if err := cfg.Check(); err != nil {
		r.t.Fatal(err)
	}
	if _, err := os.Stat(WalletDir(cfg.Data)); err != nil {
		if _, err := Init(cfg.Data); err != nil {
			r.t.Fatal(err)
		}
	}
	ctx := context.Background()
	p, err := New(ctx, cfg, Options{Tick: 100 * time.Millisecond, Logf: r.logf})
	if err != nil {
		r.t.Fatal(err)
	}
	if fund {
		asset := &nodeapi.Asset{Base: r.srv.URL}
		if _, _, err := fundProvider(ctx, p, &nodeapi.RPC{URL: r.srv.URL + "/rpc", ID: "p"}, asset); err != nil {
			r.t.Fatal(err)
		}
	}
	if err := p.Start(ctx); err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { _ = p.Close() })
	return p
}

// eventually waits up to d for f.
func (r *rig) eventually(what string, d time.Duration, f func() bool) {
	r.t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if f() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	r.t.Fatalf("%s: did not happen\n%s", what, r.said())
}

func testCert(t *testing.T, dir string) (string, string, *x509.CertPool) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "bstore test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	cp, kp := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(cp, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kp, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(c)
	return cp, kp, pool
}
