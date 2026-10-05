// Package provider is bstore-provider: the market front of spec section 17
// over a hold backend. It receives pieces from a bucket's blob topic (the
// plane's delivery, or a writer's unicast through BRC-167 upload
// sessions) and entries from its records topic; checks each closure as
// spec section 5 and rows C1 to C4 of frozen.md section 14 have it, and
// recomputes the proof root; decides by its hold policy and the bounty's
// own checks (spec section 9.3); advertises at once, because the writer
// ranks by first sight; answers challenges, renewals and settlement
// remittances on its front (spec section 10.1); and serves the closure on
// BRC-167's object routes, with HTTP Range.
package provider

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/bstore/internal/auth104"
	"github.com/lightwebinc/bstore/internal/backend"
	"github.com/lightwebinc/bstore/internal/storerec"
)

// Profile is the embedded wallet's profile: the fund key under
// [1, "bstore"] key id fund, counterparty self, and the registered basket.
var Profile = bwallet.Profile{
	FundProtocol:   storerec.Protocol,
	FundKeyID:      storerec.KeyFund,
	FundBasket:     "bstore fund",
	Version:        "bstore-provider-1",
	LegacyPoolFile: "bstore-provider-pool.json",
}

// WalletDir is the wallet's directory under a data directory.
func WalletDir(data string) string { return filepath.Join(data, "wallet") }

// Options are what a caller embedding a provider may set beyond the
// configuration.
type Options struct {
	// Now is the clock; Tick paces the background work (resuming,
	// publishing, collecting, settling, evicting); Logf receives the log.
	Now  func() time.Time
	Tick time.Duration
	Logf func(format string, args ...any)
	// Settler, when set, is the settlement leg instead of the configured
	// one.
	Settler publish.Settler
}

// Provider is one running provider.
type Provider struct {
	cfg  *Config
	opts Options

	st     *State
	mu     sync.Mutex // the state, and every save
	wallet *bwallet.Embedded
	signer *bwallet.Signer
	pool   *bwallet.Pool

	asset   *nodeapi.Asset
	settler publish.Settler
	arcade  *publish.Arcade
	hc      *headers.Client
	hosts   []*publish.Facade
	facade  *publish.Facade

	store  *backend.Native
	be     backend.Backend
	export *backend.Export

	auth    *auth104.Server
	limits  *routeLimits
	ledger  *ledger
	uploads *uploads

	kept   *producer.Kept
	payer  *producer.Payer
	trees  *producer.Trees
	mintMu sync.Mutex // one advertisement or renewal minted at a time

	queue   chan string
	queued  map[string]bool
	qmu     sync.Mutex
	metrics *metrics

	hmu    sync.Mutex
	hcache map[uint64]headerEntry
	// lastTip is the node's tip as last read; the Payer's Tip is set from
	// it under mintMu only.
	lastTip atomic.Uint32
	// scanned is when each entry's hold output was last looked at for a
	// payout (under mu).
	scanned map[string]time.Time
	// dirty is a change to the state not saved yet (under mu): pieces and
	// funding records are saved with the next background pass.
	dirty bool

	server  *http.Server
	ln      net.Listener
	msrv    *http.Server
	proxies []*net.IPNet
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Init creates a provider's wallet under data and returns its identity
// key; it refuses a directory that holds one.
func Init(data string) (string, error) {
	if err := os.MkdirAll(data, 0o700); err != nil {
		return "", err
	}
	w, err := bwallet.Create(WalletDir(data), Profile)
	if err != nil {
		return "", err
	}
	return w.Signer().IdentityHex(), nil
}

// New opens a provider over a checked configuration; Start serves it.
func New(ctx context.Context, cfg *Config, opts Options) (*Provider, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Tick == 0 {
		opts.Tick = 5 * time.Second
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	w, err := bwallet.Open(WalletDir(cfg.Data), Profile)
	if err != nil {
		return nil, fmt.Errorf("wallet (run bstore-provider init first): %w", err)
	}
	w.Mainnet = cfg.Mainnet
	w.Originator = "bstore-provider"
	st, err := loadState(cfg.Data)
	if err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	signer := w.Signer()
	if st.Identity == "" {
		st.Identity = signer.IdentityHex()
	} else if st.Identity != signer.IdentityHex() {
		return nil, fmt.Errorf("the state belongs to identity %s, not this wallet's %s", st.Identity, signer.IdentityHex())
	}
	p := &Provider{cfg: cfg, opts: opts, st: st, wallet: w, signer: signer, pool: w.Pool,
		queue: make(chan string, 4096), queued: map[string]bool{}, metrics: newMetrics()}
	if err := p.legs(); err != nil {
		return nil, err
	}
	if opts.Settler != nil {
		p.settler = opts.Settler
	}
	store, err := backend.OpenNative(filepath.Join(cfg.Data, "store"), cfg.PublicURL)
	if err != nil {
		return nil, err
	}
	store.Advertiser = p.advertise
	p.store, p.be = store, store
	if cfg.Export != "" {
		if p.export, err = backend.OpenExport(cfg.Export, store); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Join(cfg.Data, "roots"), 0o700); err != nil {
		return nil, err
	}
	var refused []string
	for _, u := range st.Unsettleable {
		refused = append(refused, u.Txid)
	}
	if p.ledger, err = openLedger(cfg.Data, opts.Now, refused); err != nil {
		return nil, err
	}
	l := cfg.Limits
	p.auth, err = auth104.New(ctx, auth104.Config{
		Wallet: w, MaxSessions: l.Sessions, SessionTTL: time.Duration(l.SessionTTLSeconds) * time.Second,
		Handshakes: auth104.Budget{Rate: l.HandshakesPerSec, Burst: l.HandshakeBurst},
		PerAddress: auth104.Budget{Rate: l.HandshakesPerAddrPerSec, Burst: l.HandshakeAddrBurst},
		Responses:  auth104.Budget{Rate: l.ResponsesPerSec, Burst: l.ResponseBurst},
		Now:        opts.Now, Count: p.metrics.count, ClientAddr: p.clientAddr,
	})
	if err != nil {
		return nil, err
	}
	for _, c := range cfg.TrustedProxies {
		_, n, _ := net.ParseCIDR(c)
		p.proxies = append(p.proxies, n)
	}
	if p.limits, err = newRouteLimits(cfg, opts.Now); err != nil {
		return nil, err
	}
	p.uploads = newUploads(cfg, opts.Now)
	p.newTrees()
	return p, nil
}

// legs builds the node, the settlement leg, the header source and the
// object legs from the configuration.
func (p *Provider) legs() error {
	c := p.cfg
	p.asset = &nodeapi.Asset{Base: c.Node}
	kind, addr, _ := strings.Cut(c.Settle, ":")
	switch kind {
	case "tcp":
		p.settler = &publish.TCPIngress{Addr: addr}
	case "rpc":
		p.settler = &publish.RPCSettler{RPC: &nodeapi.RPC{URL: addr, User: c.RPCUser, Pass: c.RPCPass, ID: "bstore-provider"}}
	case "arcade":
		p.arcade = &publish.Arcade{Base: addr, Key: c.ArcadeKey}
		p.settler = p.arcade
	}
	hc, err := headers.NewSource(c.Headers)
	if err != nil {
		return err
	}
	p.hc = hc
	for _, h := range c.Hosts {
		p.hosts = append(p.hosts, &publish.Facade{Base: strings.TrimRight(h, "/")})
	}
	if c.Facade != "" {
		p.facade = &publish.Facade{Base: strings.TrimRight(c.Facade, "/")}
	}
	return nil
}

// Identity is the provider's identity key, 66 hex digits.
func (p *Provider) Identity() string { return p.signer.IdentityHex() }

// FundAddress is where coin for the provider's fees is paid.
func (p *Provider) FundAddress() (string, error) { return p.signer.FundAddress(p.cfg.Mainnet) }

// Wallet is the provider's wallet.
func (p *Provider) Wallet() wallet.Interface { return p.wallet }

// Start listens, recovers what a previous run left, and starts the
// background work. It returns once the routes are served.
func (p *Provider) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	if err := p.recover(ctx); err != nil {
		cancel()
		return err
	}
	ln, err := net.Listen("tcp", p.cfg.Listen)
	if err != nil {
		cancel()
		return err
	}
	p.ln = ln
	p.server = &http.Server{
		Handler:           p.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    maxHeaderBytes,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	if p.cfg.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(p.cfg.TLSCert, p.cfg.TLSKey)
		if err != nil {
			cancel()
			ln.Close()
			return err
		}
		p.server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		ln = tls.NewListener(ln, p.server.TLSConfig)
	}
	if p.cfg.MetricsListen != "" {
		mln, err := net.Listen("tcp", p.cfg.MetricsListen)
		if err != nil {
			cancel()
			ln.Close()
			return err
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", p.metrics.serve)
		p.msrv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = p.msrv.Serve(mln) }()
	}
	p.wg.Add(3)
	go func() {
		defer p.wg.Done()
		if err := p.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.logf("serve: %v", err)
		}
	}()
	go func() { defer p.wg.Done(); p.worker(ctx) }()
	go func() { defer p.wg.Done(); p.background(ctx) }()
	// Everything already held or waiting is looked at again.
	p.mu.Lock()
	var roots []string
	for _, e := range p.st.Entries {
		if e.Decision == "" || e.Decision == decisionHeld {
			roots = append(roots, e.Root)
		}
	}
	p.mu.Unlock()
	for _, r := range roots {
		p.enqueue(r)
	}
	return nil
}

// Addr is the address the routes are served on.
func (p *Provider) Addr() string {
	if p.ln == nil {
		return ""
	}
	return p.ln.Addr().String()
}

// Close stops serving and the background work, and saves.
func (p *Provider) Close() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.server != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.server.Shutdown(sctx)
		cancel()
	}
	if p.msrv != nil {
		_ = p.msrv.Close()
	}
	p.wg.Wait()
	_ = p.trees.Wait(context.Background())
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.trees.Held() {
		p.st.Ahead = producer.Index(p.st.Ahead, &h)
	}
	err := p.st.save()
	if lerr := p.ledger.close(); err == nil {
		err = lerr
	}
	return err
}

func (p *Provider) logf(format string, args ...any) { p.opts.Logf("bstore-provider: "+format, args...) }

func (p *Provider) now() time.Time { return p.opts.Now() }

// save writes the state; the caller holds p.mu.
func (p *Provider) save() error {
	p.dirty = false
	if err := p.st.save(); err != nil {
		p.dirty = true
		p.logf("saving the state: %v", err)
		return err
	}
	return nil
}

func (p *Provider) fees() mint.Fees {
	return mint.Fees{SatPerByte: p.cfg.FeeRate, Floor: p.cfg.FeeFloor}
}

// tip is the node's best height.
func (p *Provider) tip(ctx context.Context) (uint32, error) {
	h, err := p.asset.BestHeader(ctx)
	if err != nil {
		return 0, fmt.Errorf("node tip: %w", err)
	}
	p.lastTip.Store(h.Height)
	return h.Height, nil
}

// loadKept rebuilds a funding tree the state keeps.
func (p *Provider) loadKept(txid string) (*transaction.Transaction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var all []funding.Tree
	if p.st.Tree != nil {
		all = append(all, *p.st.Tree)
	}
	all = append(append(all, p.st.Trees...), p.st.Ahead...)
	for _, pt := range p.st.PendingTrees {
		all = append(all, pt.Tree)
	}
	for _, t := range all {
		if t.Txid == txid {
			return funding.Rebuild(t.RawHex, t.BumpHex, t.BeefHex)
		}
	}
	return nil, fmt.Errorf("transaction %s is not a funding tree this provider keeps", txid)
}
