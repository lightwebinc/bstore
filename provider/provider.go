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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/acceptance"
	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/purse"

	"github.com/lightwebinc/bstore/auth104"
	"github.com/lightwebinc/bstore/backend"
	"github.com/lightwebinc/bstore/chaincfg"
	"github.com/lightwebinc/bstore/storerec"
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
	// FreshSpends reads the node for every hold and spend asked about,
	// rather than answering from one read of a transaction for a moment
	// (chaincfg CacheSpends, minedHolds): for a test that changes what the
	// node holds and looks at once.
	FreshSpends bool
	// Now is the clock; Tick paces the background work (resuming,
	// publishing, collecting, settling, evicting); Logf receives the log.
	Now  func() time.Time
	Tick time.Duration
	Logf func(format string, args ...any)
	// Settler, when set, is the settlement leg instead of the configured
	// one.
	Settler publish.Settler
	// Front, when set, makes the provider a front for another advertiser
	// (configuration backend "bridge"): see Front.
	Front *Front
	// Accept, when set, adjusts the payment verifier once it is built
	// (tests: a broadcaster or a status source of their own).
	Accept func(*acceptance.Verifier)
}

// Front makes a provider the market front of spec section 17 for an
// advertiser that holds, serves and advertises the bytes itself: an
// incumbent UHRP host joined through a bridge backend. The provider still
// receives, checks the closure, recomputes the proof root, decides and
// answers; then, for each closure it takes, it hands every object to the
// backend (Put), has the backend make the closure held (Advertise: the
// advertiser commits or renews through its own routes), checks the
// advertisement the backend returns, publishes it to the bucket's records
// topic unchanged, and drops its own copy of the bytes, keeping the proof
// tree's segment hashes. It answers challenges by reference only, every
// sampled segment read through the backend (from the advertiser's URL) and
// checked against the kept hashes before anything is signed; it serves no
// object routes and takes no uploads; a bounty pays the advertiser, so a
// settlement it finds is passed to Forward rather than taken.
type Front struct {
	// Backend is the bridge backend. Its Advertise returns the
	// advertiser's advertisement mined, with its own proof in the BEEF.
	Backend backend.Backend
	// Advertiser is the advertiser's identity key, 33 bytes compressed:
	// every advertisement the backend returns must name it.
	Advertiser []byte
	// Forward is told of every spend of a held entry's hold output (a
	// settlement, or a reclaim) once it is mined. An error is retried.
	Forward func(context.Context, Settlement) error
}

// Settlement is a spend of the hold output of an entry a front took, as a
// front finds it: the payee of a bounty is the advertiser, whose key the
// front does not hold, so the front forwards what the advertiser needs to
// find and take its output (spec section 9.5): the transaction, and the
// BRC-29 derivation's prefix and sender; the suffix is the rank's, for
// ranks 1 to K.
type Settlement struct {
	Entry            string `json:"entry"`
	Bucket           string `json:"bucket"`
	Root             string `json:"root"`
	Hold             string `json:"hold"`
	Writer           string `json:"senderIdentityKey"`
	K                uint64 `json:"k"`
	Txid             string `json:"txid"`
	Height           uint32 `json:"height"`
	DerivationPrefix string `json:"derivationPrefix"`
	// BEEF is the spending transaction as Atomic BEEF with its proof.
	BEEF []byte `json:"beef"`
}

// Provider is one running provider.
type Provider struct {
	cfg  *Config
	opts Options

	st *State
	mu sync.Mutex // the state
	// A save's write, one at a time (save). asked counts the saves called
	// for, written the ones a finished write covers; saveEnd wakes those
	// waiting on one.
	saving  bool
	asked   uint64
	written uint64
	saveEnd *sync.Cond
	// index answers what the hot paths ask of the entries (index.go);
	// heldObjects counts the verified closures that name each object; room
	// keeps the store's bytes in used, walked once a second from roomAt.
	index       *entryIndex
	heldObjects map[[32]byte]int32
	roomAt      time.Time
	used        uint64
	// fundings is the funding log; fundMu keeps its writes one at a time.
	fundings *fundingBook
	fundMu   sync.Mutex
	wallet   *bwallet.Embedded
	signer   *bwallet.Signer
	pool     *bwallet.Pool

	// view is the chain view, header source, settlement leg and fee
	// policy the configuration names (chaincfg); chain is its chain view.
	view  *chaincfg.View
	chain *nodeapi.Sources
	// minedHolds are the hold transactions checked lately (policy.go).
	holdsMu    sync.Mutex
	minedHolds map[string]*minedHold
	settler    publish.Settler
	arcade     *publish.Arcade
	// acc decides every payment offered; mon watches the fast ones until
	// they mine (accept.go).
	acc    *acceptance.Verifier
	mon    *acceptance.Monitor
	reflag sync.Once
	inc    *incremental
	hc     *headers.Client
	hosts  []*publish.Facade
	facade *publish.Facade

	store  *backend.Native
	be     backend.Backend
	export *backend.Export
	// front is set when the provider fronts another advertiser.
	front *Front

	auth    *auth104.Server
	limits  *routeLimits
	ledger  *ledger
	uploads *uploads

	kept  *producer.Kept
	payer *producer.Payer
	trees *producer.Trees
	// mintMu keeps the funding outputs taken one at a time (spendTree);
	// minting are the roots an advertisement is being signed and
	// journaled for (under mu).
	mintMu  sync.Mutex
	minting map[string]bool
	// adSends bounds the advertisements being sent behind the worker;
	// adSweep is the tip resumeAds last looked at every pending one for;
	// adFiles are the journal rows the next save covers (journal.go).
	adSends chan struct{}
	adSweep uint32
	adDir   string
	adFiles []string
	// outboxRun keeps publishOutbox one run at a time, and outboxAgain
	// asks that run for another pass.
	outboxRun   atomic.Bool
	outboxAgain atomic.Bool

	// queue holds the roots to look at; queued is each one's place in the
	// pipeline (hold.go), under qmu.
	queue   chan string
	queued  map[string]uint8
	qmu     sync.Mutex
	hooks   pipelineHooks
	metrics *metrics

	hmu    sync.Mutex
	hcache map[uint64]headerEntry
	// noteMu orders a payment-not-taken note against taking the same
	// payment: notTaken are the payments a note was signed for, refused
	// from then on (row C30).
	noteMu   sync.Mutex
	notTaken map[string]bool
	notesBy  map[string]int
	notesDay string
	// lastTip is the node's tip as last read; the Payer's Tip is set from
	// it under mintMu only.
	lastTip atomic.Uint32
	// scanned is when each entry's hold output was last looked at for a
	// payout (under mu).
	scanned map[string]scanMark
	// claims are the hold outputs the entries being decided name, each
	// the entry deciding on it (under mu): two workers never take two
	// entries on one output.
	claims map[string]string
	// rechecked is when each damaged root was last looked at again, for a
	// front (under mu).
	rechecked map[string]time.Time
	// dirty is a change to the state not saved yet (under mu): pieces and
	// funding records are saved with the next background pass.
	dirty bool

	server  *http.Server
	ln      net.Listener
	msrv    *http.Server
	psrv    *http.Server
	fsrv    *http.Server
	proxies []*net.IPNet
	feeders []*net.IPNet
	// ctx is the run, cancelled by Close; wg holds every goroutine of the
	// run, so that Close returns once nothing of it is still writing.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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
	fundings, err := loadFundings(cfg.Data, st.Fundings, opts.Now())
	if err != nil {
		return nil, fmt.Errorf("funding log: %w", err)
	}
	st.Fundings = nil
	adDir := filepath.Join(cfg.Data, "ads")
	adFiles, err := st.replayAds(adDir, opts.Logf)
	if err != nil {
		return nil, fmt.Errorf("advertisement journal: %w", err)
	}
	p := &Provider{cfg: cfg, inc: newIncremental(), opts: opts, st: st, index: newEntryIndex(st.Entries), fundings: fundings, wallet: w, signer: signer, pool: w.Pool,
		queue: make(chan string, 4096), queued: map[string]uint8{}, metrics: newMetrics(),
		adSends: make(chan struct{}, maxAdSends), adDir: adDir, adFiles: adFiles, heldObjects: map[[32]byte]int32{}}
	p.saveEnd = sync.NewCond(&p.mu)
	p.slimPendingAds()
	for r, h := range st.Holdings {
		if !h.Verified {
			continue
		}
		objs, err := p.rootObjects(r)
		if err != nil {
			p.logf("root %s: its object list cannot be read: %v", short(r), err)
		}
		p.noteHeld(objs, 1)
	}
	for _, r := range []string{"taken", "refused", "error"} {
		p.metrics.preset("bstore_provider_async_takes_total", "result="+r)
	}
	if err := p.legs(); err != nil {
		return nil, err
	}
	if opts.Settler != nil {
		p.settler = opts.Settler
	}
	p.setupAcceptance()
	store, err := backend.OpenNative(filepath.Join(cfg.Data, "store"), cfg.PublicURL)
	if err != nil {
		return nil, err
	}
	store.Advertiser = p.advertise
	p.store, p.be = store, store
	switch {
	case cfg.Backend == "bridge" && (opts.Front == nil || opts.Front.Backend == nil):
		return nil, errors.New("backend bridge: the provider is run by a bridge, which supplies its backend")
	case cfg.Backend != "bridge" && opts.Front != nil:
		return nil, errors.New("a front needs backend bridge in the configuration")
	case opts.Front != nil:
		if _, err := guard.ParsePubKey(opts.Front.Advertiser); err != nil {
			return nil, fmt.Errorf("the advertiser's key: %w", err)
		}
		if bytes.Equal(opts.Front.Advertiser, signer.Identity.Compressed()) {
			return nil, errors.New("a front answers for another advertiser, not for its own key")
		}
		p.front, p.be = opts.Front, opts.Front.Backend
	}
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
	if p.notTaken, err = loadNotTaken(cfg.Data); err != nil {
		return nil, fmt.Errorf("nottaken.jsonl: %w", err)
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
	for _, c := range cfg.Feeders {
		_, n, _ := net.ParseCIDR(c)
		p.feeders = append(p.feeders, n)
	}
	if p.limits, err = newRouteLimits(cfg, opts.Now); err != nil {
		return nil, err
	}
	p.uploads = newUploads(cfg, opts.Now)
	p.newTrees()
	return p, nil
}

// legs builds the chain view, the settlement leg, the header source and
// the object legs from the configuration.
func (p *Provider) legs() error {
	c := p.cfg
	v, err := chaincfg.Open(c.Spec())
	if err != nil {
		return err
	}
	if !p.opts.FreshSpends {
		v.CacheSpends()
	}
	p.view, p.chain, p.settler, p.arcade, p.hc = v, v.Chain, v.Settler, v.Arcade, v.Headers
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

// Signer is the provider's key set: its identity, its fund key.
func (p *Provider) Signer() *bwallet.Signer { return p.signer }

// Purse pays from the provider's pool, up to maxPay for one payment (zero
// pays nothing): a BRC-105 402 answered as a client (the wallet of go-sdk
// AuthFetch).
func (p *Provider) Purse(maxPay uint64) *purse.Purse {
	pu := p.purse()
	pu.MaxPay = maxPay
	return pu
}

// Staged is an object of the provider's own store, whole: for a front,
// what a bridge backend uploads from between Put and Advertise, before the
// front drops its copy.
func (p *Provider) Staged(h [32]byte) ([]byte, error) { return p.fetch(h) }

// Settler is the settlement leg the provider broadcasts on.
func (p *Provider) Settler() publish.Settler { return p.settler }

// Fees is the provider's fee policy.
func (p *Provider) Fees() mint.Fees { return p.fees() }

// NewPayer is a payer over the provider's pool, for a transaction a caller
// funds from it (its fee coin taken, its change kept).
func (p *Provider) NewPayer() *producer.Payer { return p.newPayer() }

// Start listens, recovers what a previous run left, and starts the
// background work. It returns once the routes are served.
func (p *Provider) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.ctx, p.cancel = ctx, cancel
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
	if p.cfg.PprofListen != "" {
		pln, err := net.Listen("tcp", p.cfg.PprofListen)
		if err != nil {
			return fmt.Errorf("pprof_listen: %w", err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		p.psrv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = p.psrv.Serve(pln) }()
	}
	if p.cfg.FeedListen != "" {
		fln, err := net.Listen("tcp", p.cfg.FeedListen)
		if err != nil {
			cancel()
			ln.Close()
			return err
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
			p.handleSubmit(w, r.WithContext(context.WithValue(r.Context(), fedKey{}, true)))
		})
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
		p.fsrv = &http.Server{
			Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 2 * time.Minute,
			WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: maxHeaderBytes,
			BaseContext: func(net.Listener) context.Context { return ctx },
		}
		go func() { _ = p.fsrv.Serve(fln) }()
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

// spawn runs f on the run's context as a goroutine of the run: Close
// cancels it and waits for it, as it does the worker and the background
// pass. Work that writes under the data directory runs here, never on a
// bare go statement, so that nothing is still writing after Close returns.
func (p *Provider) spawn(f func(ctx context.Context)) {
	ctx := p.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		f(ctx)
	}()
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
	if p.psrv != nil {
		_ = p.psrv.Close()
	}
	if p.fsrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.fsrv.Shutdown(sctx)
		cancel()
	}
	p.wg.Wait()
	_ = p.trees.Wait(context.Background())
	err := p.flushFundings()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.trees.Held() {
		p.st.Ahead = producer.Index(p.st.Ahead, &h)
	}
	if serr := p.save(); err == nil {
		err = serr
	}
	if lerr := p.ledger.close(); err == nil {
		err = lerr
	}
	return err
}

func (p *Provider) logf(format string, args ...any) { p.opts.Logf("bstore-provider: "+format, args...) }

func (p *Provider) now() time.Time { return p.opts.Now() }

// save returns once the state as it stands is durable. The caller holds
// p.mu, and save lets it go while the write runs: what is around a save may
// have changed after it, so a caller reads the state again or is done with
// it.
//
// One write runs at a time: the copy is taken under the lock, and encoded
// and synced outside it, so a save blocks no take behind p.mu, whatever the
// state's size. A save that arrives during a write shares the next one,
// which is a group commit: a write's cost grows with the state, and a burst
// of saves pays for one. A change a crash may lose, which the next start
// rebuilds or the writer is told nothing of, sets p.dirty instead and goes
// with the next background pass.
func (p *Provider) save() error {
	p.asked++
	want := p.asked
	for p.written < want {
		if p.saving {
			p.saveEnd.Wait()
			continue
		}
		p.saving = true
		p.dirty = false
		asked := p.asked
		at := time.Now()
		snap := p.st.snapshot()
		p.mu.Unlock()
		err := snap.save()
		p.mu.Lock()
		p.saving = false
		p.saveEnd.Broadcast()
		if err != nil {
			p.dirty = true
			p.logf("saving the state: %v", err)
			return err
		}
		p.written = asked
		p.metrics.inc("bstore_provider_state_saves_total")
		p.metrics.set("bstore_provider_state_save_seconds", time.Since(at).Seconds())
		p.metrics.add("bstore_provider_state_save_seconds_total", time.Since(at).Seconds())
	}
	return nil
}

func (p *Provider) fees() mint.Fees {
	return p.view.Fees(context.Background())
}

// tip is the header source's height.
func (p *Provider) tip(ctx context.Context) (uint32, error) {
	h, err := p.view.Tip.CurrentHeight(ctx)
	if err != nil {
		return 0, fmt.Errorf("tip: %w", err)
	}
	p.lastTip.Store(h)
	return h, nil
}

// slimPendingAds turns every pending advertisement kept as a BEEF (rows
// before RawHex) into its transaction alone, where its inputs are kept:
// the state shrinks at the first start, not when those mine.
func (p *Provider) slimPendingAds() {
	for i := range p.st.PendingAds {
		pa := &p.st.PendingAds[i]
		if pa.RawHex != "" || pa.BeefHex == "" {
			continue
		}
		raw, err := hex.DecodeString(pa.BeefHex)
		if err != nil {
			continue
		}
		tx, err := transaction.NewTransactionFromBEEF(raw)
		if err != nil {
			continue
		}
		slim := *pa
		slim.RawHex, slim.BeefHex = hex.EncodeToString(tx.Bytes()), ""
		if _, err := p.pendingTx(slim); err != nil {
			continue
		}
		*pa = slim
		p.dirty = true
	}
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
