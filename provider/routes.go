package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/auth104"
	"github.com/lightwebinc/bstore/backend"
	"github.com/lightwebinc/bstore/storerec"
)

// Bounds on what a route reads. A piece carrier's BEEF is the largest
// thing anyone sends; a payment rides in a header (BRC-105), so headers are
// allowed more than usual.
const (
	maxHeaderBytes  = 256 << 10
	maxChallengeReq = 2 << 10
	maxRenewReq     = 1 << 10
	maxSettleReq    = 1 << 20
	maxRecordsBEEF  = 262144
	maxUploadObject = 4 << 20
	maxCommitReq    = 1 << 10
)

// routeLimits are the budgets the routes take before any work.
type routeLimits struct {
	buyers    *auth104.Keyed // challenges per buyer (identity, or address unauthenticated)
	writers   *auth104.Keyed // free settlement challenges per writer
	roots     *auth104.Keyed // paid renewals per root
	submits   *auth104.Keyed // submissions per address
	settles   *auth104.Keyed // remittances per address
	objects   *auth104.Keyed // object reads per address
	uploads   *auth104.Keyed // upload sessions opened per address
	inflight  *perAddr       // submissions and uploads read at once, per address
	work      chan struct{}  // challenges answered at once
	readers   chan struct{}  // object reads at once
	ingest    chan struct{}  // submissions and uploads read at once
	verifyMu  chan struct{}  // closures verified at once
	commits   chan struct{}  // upload commits checked at once, apart from the worker
	bodies    chan struct{}  // submission bodies read or held at once
	commitsBy *auth104.Keyed // upload commits checked per identity
}

func newRouteLimits(c *Config, now func() time.Time) (*routeLimits, error) {
	l := c.Limits
	var err error
	r := &routeLimits{inflight: &perAddr{max: 4, n: map[string]int{}}, work: make(chan struct{}, l.ChallengeWorkers), readers: make(chan struct{}, l.ObjectReaders),
		ingest: make(chan struct{}, 16), verifyMu: make(chan struct{}, 2), commits: make(chan struct{}, 1), bodies: make(chan struct{}, maxBodies)}
	if r.buyers, err = auth104.NewKeyed(auth104.Budget{Rate: l.ChallengesPerBuyerPerMin / 60, Burst: l.ChallengeBurst}, 100000, now); err != nil {
		return nil, err
	}
	if r.writers, err = auth104.NewKeyed(auth104.Budget{Rate: l.FreePerWriterPerMin / 60, Burst: max(1, l.FreePerWriterPerMin)}, 100000, now); err != nil {
		return nil, err
	}
	if r.roots, err = auth104.NewKeyed(auth104.Budget{Rate: l.RenewalsPerRootPerHour / 3600, Burst: max(1, l.RenewalsPerRootPerHour)}, 100000, now); err != nil {
		return nil, err
	}
	if r.submits, err = auth104.NewKeyed(auth104.Budget{Rate: l.SubmitsPerAddrPerSec, Burst: l.SubmitBurst}, auth104.MaxAddresses, now); err != nil {
		return nil, err
	}
	if r.commitsBy, err = auth104.NewKeyed(auth104.Budget{Rate: 6.0 / 60, Burst: 4}, 100000, now); err != nil {
		return nil, err
	}
	if r.settles, err = auth104.NewKeyed(auth104.Budget{Rate: 1, Burst: 16}, auth104.MaxAddresses, now); err != nil {
		return nil, err
	}
	if r.objects, err = auth104.NewKeyed(auth104.Budget{Rate: l.ObjectReadsPerAddrPerSec, Burst: l.ObjectReadBurst}, auth104.MaxAddresses, now); err != nil {
		return nil, err
	}
	if r.uploads, err = auth104.NewKeyed(auth104.Budget{Rate: 60.0 / 3600, Burst: 16}, auth104.MaxAddresses, now); err != nil {
		return nil, err
	}
	return r, nil
}

// prefix is the public URL's path: the routes below it.
func (p *Provider) prefix() string {
	u, _ := url.Parse(p.cfg.PublicURL)
	return strings.TrimRight(u.Path, "/")
}

func (p *Provider) routes() http.Handler {
	pre := p.prefix()
	front := pre + storerec.FrontSuffix
	mux := http.NewServeMux()
	mux.HandleFunc(auth104.HandshakePath, p.auth.Handshake)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/submit", p.handleSubmit)
	mux.HandleFunc(front+"/terms", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			auth104.Write(w, auth104.Error(http.StatusMethodNotAllowed, "ERR_METHOD", "terms is a GET"))
			return
		}
		p.auth.Serve(w, r, 0, func(context.Context, *http.Request, []byte, *auth104.Caller) auth104.Reply {
			return auth104.JSON(http.StatusOK, p.termsDocument())
		})
	})
	mux.HandleFunc(front+"/challenge", p.post(maxChallengeReq, p.challenge))
	mux.HandleFunc(front+"/renew", p.post(maxRenewReq, p.renew))
	if p.front != nil {
		// A front is not the payee of a bounty, holds no bytes to serve and
		// takes no uploads: the advertiser serves its own URL, and a writer
		// remits to the advertiser's own route (row C24).
		mux.HandleFunc(front+"/settle", p.post(maxSettleReq, func(context.Context, *http.Request, []byte, *auth104.Caller) auth104.Reply {
			return auth104.Error(http.StatusBadRequest, "ERR_REMITTANCE", "this front is not a payee: a bounty pays the advertiser's key")
		}))
		return mux
	}
	mux.HandleFunc(front+"/settle", p.post(maxSettleReq, p.settle))
	mux.HandleFunc(pre+"/chirp/v1/uploads", p.post(maxCommitReq, p.uploadCreate))
	mux.HandleFunc(pre+"/chirp/v1/uploads/", p.handleUpload)
	mux.HandleFunc(pre+"/chirp/v1/", p.handleObject)
	return mux
}

// post serves a POST route through BRC-104 with a body bound.
func (p *Provider) post(maxBody int64, h auth104.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			auth104.Write(w, auth104.Error(http.StatusMethodNotAllowed, "ERR_METHOD", "this route is a POST"))
			return
		}
		p.auth.Serve(w, r, maxBody, h)
	}
}

// termsDocument is the provider's terms (spec section 10.1).
func (p *Provider) termsDocument() map[string]any {
	t := p.cfg.Terms
	return map[string]any{
		"service": "bstore",
		"terms":   1,
		"challenge": map[string]any{"base": t.ChallengeBase, "perSample": t.ChallengePerSample, "freeC": t.FreeC,
			"minBounty": t.MinBounty, "minAge": t.MinAge},
		"renew": map[string]any{"perGiBDay": t.RenewPerGiBDay, "minimum": t.RenewMinimum},
	}
}

// handleObject serves BRC-167's object route,
// <prefix>/chirp/v1/<root>/objects/<object>, for a root the provider holds
// verified and an object of its closure, with HTTP Range. Every object is
// read whole and hashed before a byte is sent: bytes on disk that went bad
// are never served, and the root is marked damaged.
func (p *Provider) handleObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Range")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, p.prefix()+"/chirp/v1/")
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[1] != "objects" {
		http.NotFound(w, r)
		return
	}
	root, err1 := chirp.ParseIdentifier(parts[0])
	obj, err2 := chirp.ParseIdentifier(parts[2])
	if err1 != nil || err2 != nil {
		http.NotFound(w, r)
		return
	}
	rootHex := hex.EncodeToString(root[:])
	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	ok := h != nil && h.Verified && !h.Damaged
	p.mu.Unlock()
	if !ok || !p.inClosure(rootHex, obj) {
		http.NotFound(w, r)
		return
	}
	if ok, retry := p.limits.objects.Take(auth104.AddressKey(p.clientAddr(r))); !ok {
		w.Header().Set("Retry-After", itoa(uint64(retry)))
		http.Error(w, "too many reads; try again later", http.StatusTooManyRequests)
		return
	}
	// The read and the hash hold a reader's slot; writing to a slow client
	// does not.
	data, status := p.readObject(r.Context(), rootHex, obj)
	if status != http.StatusOK {
		http.Error(w, http.StatusText(status), status)
		return
	}
	ct := "application/octet-stream"
	if obj == root || isNode(data) {
		ct = "application/vnd.bsv.chirp-node"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("ETag", `"`+hex.EncodeToString(obj[:])+`"`)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	p.metrics.inc("bstore_provider_objects_served_total")
}

// isNode reports whether b starts as a CHIRP node does.
func isNode(b []byte) bool {
	return len(b) >= len(chirp.Magic) && bytes.Equal(b[:len(chirp.Magic)], chirp.Magic[:])
}

// clientAddr is the address a request is budgeted under: the connection's,
// or, from a trusted proxy, the last address its X-Forwarded-For names
// that is not itself a trusted proxy.
func (p *Provider) clientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if !p.trusted(host) {
		return host
	}
	hops := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		h := strings.TrimSpace(hops[i])
		if net.ParseIP(h) == nil {
			break
		}
		if !p.trusted(h) {
			return h
		}
	}
	return host
}

// fedKey marks a request that arrived on the feed listener.
type fedKey struct{}

// feeder reports whether a request came from the plane's local feeder: on
// the feed listener, or over a connection from a configured feeder (the
// connection's own address, never a forwarded one).
func (p *Provider) feeder(r *http.Request) bool {
	if r.Context().Value(fedKey{}) != nil {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range p.feeders {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func (p *Provider) trusted(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, c := range p.proxies {
		if c.Contains(ip) {
			return true
		}
	}
	return false
}

// readObject reads an object of a held closure whole and checks its hash
// under a reader's slot. Only an object that is gone or reads back as
// other bytes marks the root damaged; any other failure (a file the system
// could not open now) is 503 and marks nothing.
func (p *Provider) readObject(ctx context.Context, rootHex string, obj [32]byte) ([]byte, int) {
	select {
	case p.limits.readers <- struct{}{}:
		defer func() { <-p.limits.readers }()
	case <-ctx.Done():
		return nil, http.StatusServiceUnavailable
	}
	size := p.store.Size(obj)
	if size < 0 {
		if !p.store.Has(obj) {
			p.damaged(rootHex, "an object of its closure is missing")
			return nil, http.StatusNotFound
		}
		return nil, http.StatusServiceUnavailable
	}
	if size > maxUploadObject {
		p.damaged(rootHex, "an object of its closure is over the bound")
		return nil, http.StatusNotFound
	}
	data, err := p.store.Get(obj, 0, uint64(size))
	switch {
	case errors.Is(err, backend.ErrMissing) || errors.Is(err, backend.ErrRange):
		p.damaged(rootHex, "an object of its closure is missing or short")
		return nil, http.StatusNotFound
	case err != nil:
		return nil, http.StatusServiceUnavailable
	case sha256.Sum256(data) != obj:
		p.damaged(rootHex, "an object of its closure read back wrong")
		return nil, http.StatusNotFound
	}
	return data, http.StatusOK
}

// bodyDeadline is how long a submission's or an upload's body may take to
// arrive: 4 MiB at about 70 KB a second.
const bodyDeadline = time.Minute

// maxBodies bounds the submission bodies in memory at once: about 270 MB of
// piece carriers.
const maxBodies = 64

// perAddr bounds the requests one address has in flight.
type perAddr struct {
	mu  sync.Mutex
	max int
	n   map[string]int
}

func (a *perAddr) enter(k string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.n[k] >= a.max {
		return false
	}
	a.n[k]++
	return true
}

func (a *perAddr) leave(k string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.n[k]--; a.n[k] <= 0 {
		delete(a.n, k)
	}
}
