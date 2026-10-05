package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/auth104"
	"github.com/lightwebinc/bstore/internal/storerec"
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
	buyers   *auth104.Keyed // challenges per buyer (identity, or address unauthenticated)
	writers  *auth104.Keyed // free settlement challenges per writer
	roots    *auth104.Keyed // paid renewals per root
	submits  *auth104.Keyed // submissions per address
	work     chan struct{}  // challenges answered at once
	readers  chan struct{}  // object reads at once
	ingest   chan struct{}  // submissions and uploads read at once
	verifyMu chan struct{}  // closures verified at once
}

func newRouteLimits(c *Config, now func() time.Time) (*routeLimits, error) {
	l := c.Limits
	var err error
	r := &routeLimits{work: make(chan struct{}, l.ChallengeWorkers), readers: make(chan struct{}, l.ObjectReaders),
		ingest: make(chan struct{}, 16), verifyMu: make(chan struct{}, 2)}
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
	mux.HandleFunc("/metrics", p.metrics.serve)
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
	select {
	case p.limits.readers <- struct{}{}:
		defer func() { <-p.limits.readers }()
	case <-r.Context().Done():
		return
	}
	size := p.store.Size(obj)
	if size < 0 || size > maxUploadObject {
		p.damaged(rootHex, "an object of its closure is missing")
		http.NotFound(w, r)
		return
	}
	data, err := p.store.Get(obj, 0, uint64(size))
	if err != nil || sha256.Sum256(data) != obj {
		p.damaged(rootHex, "an object of its closure read back wrong")
		http.NotFound(w, r)
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
