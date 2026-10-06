package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lightwebinc/bcommon/acceptance"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	bpub "github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/bstore/internal/acceptcfg"
	"github.com/lightwebinc/bstore/internal/anchor"
	"github.com/lightwebinc/bstore/internal/efleg"
	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

// Config is a home's config.json; an unknown key is refused.
type Config struct {
	// Network is main, test or regtest.
	Network string `json:"network"`
	// Node is a Teranode asset API (proofs, spends, the tip); Settle the
	// settlement leg (rpc:, tcp: or arcade:); Headers a header source.
	Node      string `json:"node"`
	Settle    string `json:"settle"`
	RPCUser   string `json:"rpc_user,omitempty"`
	RPCPass   string `json:"rpc_pass,omitempty"`
	ArcadeKey string `json:"arcade_key,omitempty"`
	Headers   string `json:"headers"`
	// Hosts are the overlay hosts asked and written to; Facade, when set,
	// is the plane; Providers the providers' submit routes; Uploads their
	// public URLs, where a closure goes by upload session when there is no
	// facade. Quorum is how many hosts must take an entry without a facade
	// (0: one).
	Hosts     []string `json:"hosts"`
	Facade    string   `json:"facade,omitempty"`
	Providers []string `json:"providers,omitempty"`
	Uploads   []string `json:"uploads,omitempty"`
	Quorum    int      `json:"quorum,omitempty"`
	// CA is a PEM file of certificates trusted besides the system's;
	// FetchPrivate lets reads and challenges reach advertised URLs on
	// loopback or private addresses.
	CA           string `json:"ca,omitempty"`
	FetchPrivate bool   `json:"fetch_private,omitempty"`
	// Bounty is what a put asks for unless told otherwise; HoldAhead how
	// many mined hold outputs of it are kept ready.
	Bounty    *BountyConfig `json:"bounty,omitempty"`
	HoldAhead int           `json:"hold_ahead,omitempty"`
	// RaceWindowSeconds is the writer's race window (0: an hour).
	RaceWindowSeconds int64 `json:"race_window_seconds,omitempty"`
	// PublishOutcomes publishes failed settlement challenges and every
	// challenge's outcome as outcome records (default true).
	PublishOutcomes *bool  `json:"publish_outcomes,omitempty"`
	PieceTree       int    `json:"piece_tree,omitempty"`
	EntryTree       int    `json:"entry_tree,omitempty"`
	FeeRate         uint64 `json:"fee_sat_per_byte,omitempty"`
	FeeFloor        uint64 `json:"fee_floor,omitempty"`
	// The payment acceptance rule a settlement is remitted by (accept_threshold_sats
	// and the other accept_ keys, docs/usage.md).
	acceptcfg.Config
	// Buckets are the chains this home reads and writes; Bucket the one a
	// command uses unless -bucket names another.
	Buckets []BucketRef `json:"buckets,omitempty"`
	Bucket  string      `json:"bucket,omitempty"`
}

// BucketRef is a bucket chain: its identifier, owner and id.
type BucketRef struct {
	Bucket string `json:"bucket"`
	Owner  string `json:"owner"`
	ID     string `json:"id"`
}

// BountyConfig is a put's hold terms (spec section 4.3).
type BountyConfig struct {
	Satoshis         uint64 `json:"satoshis"`
	T                uint64 `json:"t"`
	K                uint64 `json:"k"`
	RetentionSeconds uint64 `json:"retention_seconds"`
	DeadlineBlocks   uint64 `json:"deadline_blocks"`
	CheckC           uint64 `json:"check_c"`
}

func (g *global) configPath() string { return filepath.Join(g.home, "config.json") }

func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, c.check()
}

func (c *Config) check() error {
	switch c.Network {
	case "main", "test", "regtest":
	default:
		return fmt.Errorf("network is main, test or regtest, not %q", c.Network)
	}
	if c.Node == "" || c.Settle == "" || c.Headers == "" {
		return errors.New("config.json needs node, settle and headers")
	}
	if b := c.Bounty; b != nil {
		h := storerec.Hold{Satoshis: b.Satoshis, T: b.T, K: b.K, Retention: b.RetentionSeconds, Deadline: 1, CheckC: b.CheckC}
		if err := checkTerms(&h); err != nil {
			return fmt.Errorf("bounty: %w", err)
		}
		if b.DeadlineBlocks < 1 {
			return errors.New("bounty: deadline_blocks is at least 1")
		}
	}
	for _, b := range c.Buckets {
		if _, err := b.chain(); err != nil {
			return err
		}
	}
	return nil
}

// checkTerms holds hold terms to spec section 4.3's bounds.
func checkTerms(h *storerec.Hold) error {
	switch {
	case h.Satoshis < 1 || h.Satoshis > 2_100_000_000_000_000:
		return errors.New("satoshis is 1 to 2,100,000,000,000,000")
	case h.T < 1 || h.T > 16:
		return errors.New("t is 1 to 16")
	case h.K < h.T || h.K > 16:
		return errors.New("k is t to 16")
	case h.Retention < 1 || h.Retention > 1<<32-1:
		return errors.New("retention is 1 to 2^32 - 1 seconds")
	case h.CheckC < 1 || h.CheckC > 1024:
		return errors.New("check_c is 1 to 1024")
	}
	return nil
}

func (b BucketRef) chain() (reader.Chain, error) {
	if err := storerec.CheckBucket(b.Bucket); err != nil {
		return reader.Chain{}, fmt.Errorf("bucket %q: %w", b.Bucket, err)
	}
	owner, err := hex.DecodeString(b.Owner)
	if err == nil {
		_, err = storerec.CheckKey(owner)
	}
	if err != nil {
		return reader.Chain{}, fmt.Errorf("bucket %s: owner: not an identity key", b.Bucket)
	}
	id, err := hex.DecodeString(b.ID)
	if err != nil || len(id) != 32 {
		return reader.Chain{}, fmt.Errorf("bucket %s: id is 64 hex digits", b.Bucket)
	}
	return reader.Chain{Bucket: b.Bucket, Owner: owner, ID: [32]byte(id)}, nil
}

func saveConfig(path string, c *Config) error {
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// home is an opened home: its configuration, its publisher (wallet,
// trees, outbox, races), a reader of its hosts and the owner's tooling.
type home struct {
	g      *global
	cfg    *Config
	pub    *publish.Publisher
	rd     *reader.Reader
	tool   *anchor.Tool
	store  *publish.Store
	fetch  *http.Client
	hostHC *http.Client
}

// open opens the home for a command: the publisher holds the home's lock
// until the process ends.
func (g *global) open(ctx context.Context) (*home, error) {
	cfg, err := loadConfig(g.configPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, usage("no home at %s: bstore init creates it", g.home)
	}
	if err != nil {
		return nil, usage("%v", err)
	}
	if g.hosts != "" {
		cfg.Hosts = splitList(g.hosts)
	}
	pool, err := certPool(cfg.CA)
	if err != nil {
		return nil, usage("%v", err)
	}
	hostHC := &http.Client{Timeout: 2 * time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}, MaxIdleConnsPerHost: 8},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	fetchHC := &http.Client{Timeout: 2 * time.Minute, Transport: fetchTransport(pool, cfg.FetchPrivate),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	hc, err := headers.NewSource(cfg.Headers)
	if err != nil {
		return nil, usage("headers: %v", err)
	}
	asset := &nodeapi.Asset{Base: cfg.Node}
	settler, err := settlerOf(cfg)
	if err != nil {
		return nil, usage("%v", err)
	}
	st, err := publish.OpenStore(filepath.Join(g.home, "store"))
	if err != nil {
		return nil, err
	}
	rd := &reader.Reader{Hosts: cfg.Hosts, HTTP: hostHC, Fetch: fetchHC, Headers: hc, Logf: g.logf, Spender: asset.Spender}
	d := &publish.Delivery{Facade: cfg.Facade, Hosts: cfg.Hosts, Providers: cfg.Providers, Uploads: cfg.Uploads, HTTP: hostHC, Quorum: cfg.Quorum}
	fees := mint.DefaultFees
	if cfg.FeeRate > 0 {
		fees.SatPerByte = cfg.FeeRate
	}
	if cfg.FeeFloor > 0 {
		fees.Floor = cfg.FeeFloor
	}
	// Fronts are named by advertisements and statements anyone can make:
	// reached through the same guard as advertised URLs.
	frontHC := &http.Client{Timeout: 2 * time.Minute, Transport: fetchTransport(pool, cfg.FetchPrivate),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	pub, err := publish.Open(ctx, publish.Config{Dir: g.home, Asset: asset, Settler: settler, Fees: fees, Mainnet: cfg.Network == "main",
		Accept: cfg.AcceptPolicy(), Status: statusOf(cfg),
		PieceTree: cfg.PieceTree, EntryTree: cfg.EntryTree, HoldAhead: cfg.HoldAhead, Store: st, Delivery: d, Reader: rd, FrontHTTP: frontHC, Logf: g.logf})
	if errors.Is(err, publish.ErrInUse) {
		return nil, usage("%v: one process at a time uses a home; a running gateway (bstore s3d) holds its identities and judges, settles and reclaims their races itself, so stop it to run this command there", err)
	}
	if err != nil {
		return nil, err
	}
	h := &home{g: g, cfg: cfg, pub: pub, rd: rd, store: st, fetch: fetchHC, hostHC: hostHC}
	h.tool = &anchor.Tool{Pub: pub, Reader: rd, Store: anchor.OpenStore(g.home), Logf: g.logf}
	return h, nil
}

func (h *home) close() { h.pub.Close() }

// chain is the bucket a command names (-bucket, else the default).
func (h *home) chain() (reader.Chain, error) {
	want := h.g.bucket
	if want == "" {
		want = h.cfg.Bucket
	}
	if want == "" {
		if len(h.cfg.Buckets) == 1 {
			return h.cfg.Buckets[0].chain()
		}
		return reader.Chain{}, usage("name a bucket: -bucket, or bucket in config.json")
	}
	want = strings.ReplaceAll(want, "-", "_")
	for _, b := range h.cfg.Buckets {
		if b.Bucket == want {
			return b.chain()
		}
	}
	return reader.Chain{}, usage("bucket %s is not in config.json (bstore bucket add, or bucket new)", want)
}

// statusOf is arcade's status for a settlement remitted before it mines,
// when the settlement leg is arcade; nil otherwise.
func statusOf(c *Config) acceptance.StatusSource {
	if kind, addr, _ := strings.Cut(c.Settle, ":"); kind == "arcade" {
		return &bpub.Arcade{Base: addr, Key: c.ArcadeKey}
	}
	return nil
}

func settlerOf(c *Config) (bpub.Settler, error) {
	kind, addr, _ := strings.Cut(c.Settle, ":")
	switch kind {
	case "tcp":
		return efleg.Wrap(&bpub.TCPIngress{Addr: addr}, &nodeapi.Asset{Base: c.Node}), nil
	case "rpc":
		return &bpub.RPCSettler{RPC: &nodeapi.RPC{URL: addr, User: c.RPCUser, Pass: c.RPCPass, ID: "bstore"}}, nil
	case "arcade":
		return efleg.Wrap(&bpub.Arcade{Base: addr, Key: c.ArcadeKey}, &nodeapi.Asset{Base: c.Node}), nil
	}
	return nil, errors.New("settle is tcp:, rpc: or arcade:")
}

func certPool(path string) (*x509.CertPool, error) {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if path == "" {
		return pool, nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca %s: no certificate", path)
	}
	return pool, nil
}

// fetchTransport reaches advertised URLs. Unless private addresses are
// allowed, a connection to a loopback, private, link-local or unspecified
// address is refused after resolution, so an advertisement cannot point
// this client at its own network.
func fetchTransport(pool *x509.CertPool, private bool) *http.Transport {
	d := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	if !private {
		d.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
				ip.IsUnspecified() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
				return fmt.Errorf("an advertised address %s is not public", host)
			}
			return nil
		}
	}
	return &http.Transport{DialContext: d.DialContext, TLSClientConfig: &tls.Config{RootCAs: pool},
		MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second, ResponseHeaderTimeout: time.Minute}
}

func splitList(s string) []string {
	var out []string
	for _, x := range strings.Split(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, strings.TrimRight(x, "/"))
		}
	}
	return out
}

// raceOptions are the writer's race policy from config.json.
func (h *home) raceOptions(budget uint64) publish.RaceOptions {
	w := time.Duration(h.cfg.RaceWindowSeconds) * time.Second
	if w <= 0 {
		w = time.Hour
	}
	pub := h.cfg.PublishOutcomes == nil || *h.cfg.PublishOutcomes
	return publish.RaceOptions{Window: w, Budget: budget, Fetch: h.fetch, Publish: pub}
}
