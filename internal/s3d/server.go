// Package s3d is `bstore s3d`, the S3 gateway of spec section 16: a reader
// and a writer for clients that speak S3, authenticated with AWS Signature
// Version 4 in gateway mode. Reads are proven against the bucket's chain
// (package reader); writes are published by the identity an access key
// maps to (package publish). docs/s3.md is the operator's guide and the
// compatibility matrix.
package s3d

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/bcommon/acceptance"

	"github.com/lightwebinc/bstore/chaincfg"
	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

// Options are what an embedding caller may set beyond the configuration.
type Options struct {
	Now  func() time.Time
	Logf func(format string, args ...any)
	// Tick paces the background work.
	Tick time.Duration
	// LockWait is how long an identity another process has open (a
	// command run against it) is waited for at start.
	LockWait time.Duration
}

// Server is one running gateway.
type Server struct {
	cfg     *Config
	opts    Options
	signer  *signer
	reader  *reader.Reader
	fetch   *http.Client
	store   *publish.Store
	pubs    map[string]*publish.Publisher
	buckets map[string]*bucket // by S3 name
	order   []string
	st      *state
	uploads *uploads
	replay  *replayCache
	limits  *keyLimits
	metrics *metrics

	writes, parts, reads, stages, lookups chan struct{}
	flights                               writeFlights

	srv    *http.Server
	msrv   *http.Server
	psrv   *http.Server
	ln     net.Listener
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type bucket struct {
	name  string // S3 name
	chain reader.Chain
}

// New opens a gateway over a checked configuration: every identity's
// wallet (bstore s3d init creates them), its funding trees and outbox, the
// local store and the state.
func New(ctx context.Context, cfg *Config, opts Options) (*Server, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	if opts.Tick == 0 {
		opts.Tick = 10 * time.Second
	}
	s := &Server{cfg: cfg, opts: opts, pubs: map[string]*publish.Publisher{}, buckets: map[string]*bucket{},
		replay: newReplayCache(opts.Now, cfg.Limits.ReplayPerKey), metrics: newMetrics(),
		writes: make(chan struct{}, cfg.Limits.Writes), parts: make(chan struct{}, cfg.Limits.PartWrites),
		reads: make(chan struct{}, cfg.Limits.Reads), stages: make(chan struct{}, cfg.Limits.Stages),
		lookups: make(chan struct{}, cfg.Limits.Lookups)}
	s.limits = newKeyLimits(cfg.Limits.RequestsPerSec, cfg.Limits.Burst, opts.Now)
	nameSlots(s)
	if err := os.MkdirAll(cfg.Data, 0o700); err != nil {
		return nil, err
	}
	var err error
	if s.st, err = openState(cfg.Data, opts.Now); err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	if s.store, err = publish.OpenStore(filepath.Join(cfg.Data, "store")); err != nil {
		return nil, err
	}
	if s.uploads, err = openUploads(filepath.Join(cfg.Data, "uploads"), opts.Now); err != nil {
		return nil, err
	}
	pool, err := certPool(cfg.CA)
	if err != nil {
		return nil, err
	}
	hostHTTP := &http.Client{Timeout: 2 * time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool},
		MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	fetchHTTP := &http.Client{Timeout: 2 * time.Minute, Transport: fetchTransport(pool, cfg.FetchPrivate),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	v, err := chaincfg.Open(cfg.Spec())
	if err != nil {
		return nil, err
	}
	v.CacheSpends()
	s.reader = &reader.Reader{Hosts: cfg.Hosts, HTTP: hostHTTP, Fetch: fetchHTTP, Headers: v.Headers, Held: s.st, Logf: opts.Logf, Now: opts.Now,
		WalkDir: filepath.Join(cfg.Data, "walks")}
	s.fetch = fetchHTTP
	s.reader.Spender = v.Chain.Spender
	s.reader.OnToken = s.st.named
	var status acceptance.StatusSource
	if v.Arcade != nil {
		status = v.Arcade
	}
	d := &publish.Delivery{Facade: cfg.Facade, Hosts: cfg.Hosts, Providers: cfg.Providers, HTTP: hostHTTP, Quorum: cfg.Quorum,
		Observe: s.metrics.observe}
	for _, name := range cfg.Identities {
		p, err := publish.Open(ctx, publish.Config{Dir: cfg.IdentityDir(name), OutboxMax: cfg.OutboxBytes,
			Chain: v.Chain, Known: v.Known, Tip: v.Tip, Settler: v.Settler, Accept: cfg.AcceptPolicy(), Status: status,
			FeeSource: v.FeeSource, Mainnet: cfg.Mainnet,
			PieceTree: cfg.PieceTree, EntryTree: cfg.EntryTree, PieceAhead: cfg.PieceAhead, EntryAhead: cfg.EntryAhead, PieceWorkers: cfg.PieceWorkers,
			HoldAhead: cfg.HoldAhead, HoldTree: cfg.HoldTree, Store: s.store, Delivery: d, Reader: s.reader, FrontHTTP: fetchHTTP,
			LockWait: opts.LockWait, Logf: opts.Logf, Now: opts.Now})
		if err != nil {
			return nil, fmt.Errorf("identity %s (bstore s3d init creates it): %w", name, err)
		}
		s.pubs[name] = p
	}
	for _, b := range cfg.Buckets {
		name, _ := storerec.S3Name(b.Bucket)
		owner, _ := hex.DecodeString(b.Owner)
		id, _ := hex.DecodeString(b.ID)
		s.buckets[name] = &bucket{name: name, chain: reader.Chain{Bucket: b.Bucket, Owner: owner, ID: [32]byte(id)}}
		s.order = append(s.order, name)
	}
	sort.Strings(s.order)
	keys := map[string]*Key{}
	for _, k := range cfg.AccessKeys {
		key := &Key{ID: k.ID, Secret: k.Secret, Identity: k.Identity}
		if len(k.Buckets) > 0 {
			key.Buckets = map[string]bool{}
			for _, b := range k.Buckets {
				key.Buckets[b] = true
			}
		}
		keys[k.ID] = key
	}
	s.signer = &signer{keys: keys, region: cfg.Region, now: opts.Now}
	return s, nil
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

// fetchTransport reaches advertisers' URLs. Unless private addresses are
// allowed, a connection to a loopback, private, link-local or unspecified
// address is refused after resolution, so an advertisement cannot point
// the gateway at its own network.
func fetchTransport(pool *x509.CertPool, private bool) *http.Transport {
	d := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	if !private {
		d.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if !publicAddr(net.ParseIP(host)) {
				return fmt.Errorf("refused: %s is not a public address", host)
			}
			return nil
		}
	}
	return &http.Transport{DialContext: d.DialContext, TLSClientConfig: &tls.Config{RootCAs: pool},
		MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second}
}

// nonPublic are the ranges an advertised URL is never fetched from, beside
// what net.IP classifies as loopback, private, link-local, multicast or
// unspecified: shared address space, benchmarking, "this network", and the
// IPv6 forms that carry an IPv4 address (NAT64, 6to4, Teredo) or are
// discard-only and documentation.
var nonPublic = func() []*net.IPNet {
	var out []*net.IPNet
	for _, c := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24",
		"203.0.113.0/24", "240.0.0.0/4", "64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32", "100::/64", "2001:db8::/32"} {
		_, n, _ := net.ParseCIDR(c)
		out = append(out, n)
	}
	return out
}()

func publicAddr(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return false
	}
	for _, n := range nonPublic {
		if n.Contains(ip) {
			return false
		}
	}
	return true
}

// Identity is an identity's key, 66 hex digits.
func (s *Server) Identity(name string) string {
	if p := s.pubs[name]; p != nil {
		return p.IdentityHex()
	}
	return ""
}

// Addr is where the gateway listens, once started.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Start listens and starts the background work.
func (s *Server) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		cancel()
		return err
	}
	s.ln = ln
	// No ReadTimeout or WriteTimeout: a body or an object may be large.
	// Each read and write is held to the idle bound instead (idleConn).
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
		MaxHeaderBytes: 64 << 10, BaseContext: func(net.Listener) context.Context { return ctx },
		ErrorLog: log.New(logWriter{s.opts.Logf}, "", 0)}
	if s.cfg.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
		if err != nil {
			cancel()
			ln.Close()
			return err
		}
		s.srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		ln = tls.NewListener(ln, s.srv.TLSConfig)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.opts.Logf("s3d: %v", err)
		}
	}()
	if s.cfg.MetricsListen != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", s.metrics.serve)
		s.msrv = &http.Server{Addr: s.cfg.MetricsListen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		mln, err := net.Listen("tcp", s.cfg.MetricsListen)
		if err != nil {
			cancel()
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			_ = s.msrv.Serve(mln)
		}()
	}
	if s.cfg.PprofListen != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		s.psrv = &http.Server{Addr: s.cfg.PprofListen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		pln, err := net.Listen("tcp", s.cfg.PprofListen)
		if err != nil {
			cancel()
			return err
		}
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(int(time.Millisecond))
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			_ = s.psrv.Serve(pln)
		}()
	}
	s.wg.Add(1)
	go s.background(ctx)
	return nil
}

// Close stops serving, waits for the background work, and saves.
func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	sctx, c := context.WithTimeout(context.Background(), 30*time.Second)
	defer c()
	if s.srv != nil {
		_ = s.srv.Shutdown(sctx)
	}
	if s.msrv != nil {
		_ = s.msrv.Shutdown(sctx)
	}
	if s.psrv != nil {
		_ = s.psrv.Shutdown(sctx)
	}
	s.wg.Wait()
	for _, p := range s.pubs {
		p.Close()
	}
	return nil
}

func (s *Server) background(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.Tick)
	defer t.Stop()
	// The outbox is sent again on a run of its own, so a backlog never
	// holds up the store's trimming or the hold outputs minted ahead.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(s.opts.Tick)
		defer t.Stop()
		for {
			for _, p := range s.pubs {
				p.Outbox().Retry(ctx)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	// Each identity's races, settlements and reclaims, on a run of their
	// own: a settlement waits for its block.
	if !s.cfg.Race.Off {
		o := publish.RaceOptions{Window: time.Duration(s.cfg.Race.WindowSeconds) * time.Second, Budget: s.cfg.Race.Budget,
			Fetch: s.fetch, Publish: !s.cfg.Race.NoPublish}
		for _, p := range s.pubs {
			s.wg.Add(1)
			go func(p *publish.Publisher) {
				defer s.wg.Done()
				p.Background(ctx, o, time.Duration(s.cfg.Race.TickSeconds)*time.Second)
			}(p)
		}
	}
	lastAge := time.Time{}
	for {
		minute := time.Since(lastAge) >= time.Minute
		if minute {
			s.remittedAlarm()
			lastAge = time.Now()
		}
		for _, p := range s.pubs {
			p.CheckHolds(ctx)
			if b := s.cfg.Bounty; b != nil {
				go p.FillHolds(ctx, b.Satoshis)
			}
		}
		// A trim reads every manifest and walks the store, which a tick would
		// keep a core at once the store holds a day of small puts.
		if minute {
			if _, err := s.store.Trim(s.cfg.StoreBytes); err != nil {
				s.opts.Logf("store: %v", err)
			}
		}
		s.st.expire()
		s.uploads.expire(time.Duration(s.cfg.Limits.UploadSeconds) * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

type logWriter struct{ f func(string, ...any) }

func (l logWriter) Write(p []byte) (int, error) {
	l.f("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func requestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// ServeHTTP is the S3 surface. Paths are never cleaned: a key is the
// decoded path after the bucket, exactly.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := requestID()
	w.Header().Set("x-amz-request-id", reqID)
	w.Header().Set("Server", "bstore")
	op := "unknown"
	status := 0
	defer func() {
		if v := recover(); v != nil {
			if v == http.ErrAbortHandler {
				s.metrics.request(op, 0)
				panic(v)
			}
			s.opts.Logf("s3d: panic serving %s %s: %v", r.Method, r.URL.Path, v)
			writeError(w, r, errInternal, reqID)
			status = 500
		}
		s.metrics.request(op, status)
	}()
	fail := func(e *s3Error) {
		status = e.Status
		if e.Code == "SlowDown" {
			s.metrics.add(fmt.Sprintf("bstore_s3d_slowdown_total{reason=%q}", slowReason(e)), 1)
		}
		writeError(w, r, e, reqID)
	}
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet && r.URL.RawQuery == "" {
		_, _ = w.Write([]byte("ok\n"))
		op, status = "healthz", 200
		return
	}
	bname, key, err := s.route(r)
	if err != nil {
		fail(err)
		return
	}
	a, aerr := s.signer.verify(r)
	if aerr != nil {
		var ae *authError
		if errors.As(aerr, &ae) {
			fail(&s3Error{Code: ae.code, Message: ae.msg, Status: ae.status})
		} else {
			fail(errAccessDenied)
		}
		return
	}
	if !s.limits.allow(a.Key.ID) {
		fail(errSlowKeyRate)
		return
	}
	rw := &statusWriter{ResponseWriter: w}
	defer func() {
		if status == 0 {
			status = rw.status
		}
	}()
	if bname == "" {
		op = "ListBuckets"
		if r.Method != http.MethodGet {
			fail(errMethod)
			return
		}
		if err := checkSub(r, nil); err != nil {
			fail(err)
			return
		}
		s.listBuckets(rw, a)
		return
	}
	b := s.buckets[bname]
	if b == nil {
		fail(errNoSuchBucket)
		return
	}
	if a.Key.Buckets != nil && !a.Key.Buckets[bname] {
		fail(errAccessDenied)
		return
	}
	ctx := context.WithValue(r.Context(), rcKey{}, http.NewResponseController(w))
	r = r.WithContext(ctx)
	var e *s3Error
	if key == "" {
		op, e = s.bucketOp(ctx, rw, r, a, b, reqID)
	} else {
		op, e = s.objectOp(ctx, rw, r, a, b, key, reqID)
	}
	if e != nil {
		if rw.wrote {
			// The answer has begun: end it, so the client sees it cut short.
			panic(http.ErrAbortHandler)
		}
		fail(e)
	}
}

// route reads the bucket and the key: virtual-host style when the Host is
// <bucket>.<domain>, else path style.
func (s *Server) route(r *http.Request) (string, string, *s3Error) {
	p := r.URL.Path
	if !strings.HasPrefix(p, "/") {
		return "", "", invalidArgument("the path must begin with /")
	}
	if s.cfg.Domain != "" {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.ToLower(host)
		if b, ok := strings.CutSuffix(host, "."+s.cfg.Domain); ok && b != "" {
			return b, p[1:], nil
		}
	}
	rest := p[1:]
	b, key, _ := strings.Cut(rest, "/")
	return b, key, nil
}

// statusWriter notes whether the answer has begun.
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.status = http.StatusOK
		w.wrote = true
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (s *Server) listBuckets(w http.ResponseWriter, a *Auth) {
	res := listAllMyBuckets{NS: s3NS, Owner: owner{ID: a.Key.Identity, DisplayName: a.Key.Identity}}
	for _, n := range s.order {
		if a.Key.Buckets != nil && !a.Key.Buckets[n] {
			continue
		}
		res.Buckets = append(res.Buckets, bucketXML{Name: n, CreationDate: "1970-01-01T00:00:00.000Z"})
	}
	writeXML(w, http.StatusOK, res)
}

// The S3 subresources this gateway knows; every one it does not serve is
// NotImplemented (spec section 16.2), never ignored.
var subresources = []string{"accelerate", "acl", "analytics", "attributes", "cors", "delete", "encryption",
	"intelligent-tiering", "inventory", "legal-hold", "lifecycle", "location", "logging", "metadataConfiguration",
	"metrics", "notification", "object-lock", "ownershipControls", "partNumber", "policy", "policyStatus",
	"publicAccessBlock", "replication", "requestPayment", "restore", "retention", "select", "select-type",
	"session", "tagging", "torrent", "uploadId", "uploads", "versionId", "versioning", "versions", "website",
	"renameObject"}

// checkSub refuses a subresource the operation does not take.
func checkSub(r *http.Request, allowed map[string]bool) *s3Error {
	q := r.URL.Query()
	for _, n := range subresources {
		if _, ok := q[n]; ok && !allowed[n] {
			return notImplemented("the " + n + " subresource")
		}
	}
	return nil
}

func (s *Server) bucketOp(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, reqID string) (string, *s3Error) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodHead:
		if e := checkSub(r, nil); e != nil {
			return "HeadBucket", e
		}
		w.Header().Set("x-amz-bucket-region", s.cfg.Region)
		w.WriteHeader(http.StatusOK)
		return "HeadBucket", nil
	case http.MethodGet:
		if _, ok := q["location"]; ok {
			if e := checkSub(r, map[string]bool{"location": true}); e != nil {
				return "GetBucketLocation", e
			}
			v := s.cfg.Region
			if v == "us-east-1" {
				v = ""
			}
			writeXML(w, http.StatusOK, locationXML{NS: s3NS, Value: v})
			return "GetBucketLocation", nil
		}
		if e := checkSub(r, nil); e != nil {
			return "GetBucket", e
		}
		if q.Get("list-type") == "2" {
			return "ListObjectsV2", s.listObjects(ctx, w, r, b, true)
		}
		if q.Get("list-type") != "" {
			return "ListObjects", invalidArgument("list-type is 2 or absent")
		}
		return "ListObjects", s.listObjects(ctx, w, r, b, false)
	case http.MethodPost:
		if _, ok := q["delete"]; ok {
			if e := checkSub(r, map[string]bool{"delete": true}); e != nil {
				return "DeleteObjects", e
			}
			return "DeleteObjects", s.replayed(w, r, a, func(w http.ResponseWriter) *s3Error { return s.deleteObjects(ctx, w, r, a, b) })
		}
		return "PostBucket", errMethod
	case http.MethodPut:
		return "PutBucket", notImplemented("creating or configuring a bucket (buckets are configured by the operator)")
	case http.MethodDelete:
		return "DeleteBucket", notImplemented("deleting a bucket")
	}
	return "Bucket", errMethod
}

func (s *Server) objectOp(ctx context.Context, w http.ResponseWriter, r *http.Request, a *Auth, b *bucket, key, reqID string) (string, *s3Error) {
	q := r.URL.Query()
	if err := storerec.CheckObjectKey(key); err != nil {
		return "Object", invalidArgument("the key is not 1 to 1024 bytes of UTF-8 on one line (no control character)")
	}
	_, hasUpload := q["uploadId"]
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if hasUpload {
			return "ListParts", notImplemented("ListParts")
		}
		if e := checkSub(r, nil); e != nil {
			return "GetObject", e
		}
		op := "GetObject"
		if r.Method == http.MethodHead {
			op = "HeadObject"
		}
		return op, s.getObject(ctx, w, r, b, key)
	case http.MethodPut:
		if _, ok := q["partNumber"]; ok || hasUpload {
			if e := checkSub(r, map[string]bool{"partNumber": true, "uploadId": true}); e != nil {
				return "UploadPart", e
			}
			if r.Header.Get("X-Amz-Copy-Source") != "" {
				return "UploadPartCopy", notImplemented("UploadPartCopy")
			}
			// Not replay-cached: a part publishes nothing, and a part sent
			// again only rewrites itself (a completion names each part's
			// MD5, so a part swapped by a replay is refused there).
			return "UploadPart", s.uploadPart(ctx, w, r, a, b, key)
		}
		if e := checkSub(r, nil); e != nil {
			return "PutObject", e
		}
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			return "CopyObject", s.replayed(w, r, a, func(w http.ResponseWriter) *s3Error { return s.copyObject(ctx, w, r, a, b, key) })
		}
		return "PutObject", s.replayed(w, r, a, func(w http.ResponseWriter) *s3Error { return s.putObject(ctx, w, r, a, b, key) })
	case http.MethodPost:
		if _, ok := q["uploads"]; ok {
			if e := checkSub(r, map[string]bool{"uploads": true}); e != nil {
				return "CreateMultipartUpload", e
			}
			return "CreateMultipartUpload", s.createUpload(w, r, a, b, key)
		}
		if hasUpload {
			if e := checkSub(r, map[string]bool{"uploadId": true}); e != nil {
				return "CompleteMultipartUpload", e
			}
			return "CompleteMultipartUpload", s.replayed(w, r, a, func(w http.ResponseWriter) *s3Error { return s.completeUpload(ctx, w, r, a, b, key, reqID) })
		}
		if e := checkSub(r, nil); e != nil {
			return "PostObject", e
		}
		return "PostObject", notImplemented("POST object (browser uploads)")
	case http.MethodDelete:
		if hasUpload {
			if e := checkSub(r, map[string]bool{"uploadId": true}); e != nil {
				return "AbortMultipartUpload", e
			}
			return "AbortMultipartUpload", s.abortUpload(w, r, a, b, key)
		}
		if e := checkSub(r, nil); e != nil {
			return "DeleteObject", e
		}
		return "DeleteObject", s.replayed(w, r, a, func(w http.ResponseWriter) *s3Error { return s.deleteObject(ctx, w, r, a, b, key) })
	}
	return "Object", errMethod
}

// acquire takes a slot of sem or answers SlowDown at once: a full gateway
// sheds load rather than queueing it. The SlowDown names the bound (see
// semErrors).
func acquire(sem chan struct{}) (func(), *s3Error) {
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	default:
		if e, ok := semErrors.Load(sem); ok {
			return nil, e.(*s3Error)
		}
		return nil, errSlowDown
	}
}

// semErrors maps each server's slot channels to the SlowDown that names
// them, set when the server is made.
var semErrors sync.Map

func nameSlots(s *Server) {
	semErrors.Store(s.writes, errSlowWrites)
	semErrors.Store(s.parts, errSlowParts)
	semErrors.Store(s.reads, errSlowReads)
	semErrors.Store(s.stages, errSlowStages)
	semErrors.Store(s.lookups, errSlowLookups)
}

// remittedAlarm sets the gauges of races remitted before mining whose
// proof the node has not served within publish.RemittedStale: the
// settlement may never mine (an alert reads
// bstore_s3d_races_remitted_stale > 0; bstore doctor names each race).
func (s *Server) remittedAlarm() {
	n, oldest := 0, time.Duration(0)
	for _, p := range s.pubs {
		k, o := p.StaleRemitted(publish.RemittedStale)
		n += k
		oldest = max(oldest, o)
	}
	s.metrics.set("bstore_s3d_races_remitted_stale", int64(n))
	s.metrics.set("bstore_s3d_race_remitted_oldest_seconds", int64(oldest.Seconds()))
}
