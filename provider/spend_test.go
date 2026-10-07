package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lightwebinc/bcommon/headers"

	"github.com/lightwebinc/bstore/internal/chaincfg"
)

// A spend source that does not say "unspent" is no evidence: the hold
// output's check decides nothing (it is retried), and only the node's
// explicit OK reads as unspent (bcommon nodeapi Spender, which fails
// closed on every other status).
func TestHoldOutputUnknownSpendStatus(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	h, err := r.writer.HoldOutput(ctx, 2000)
	if err != nil {
		t.Fatal(err)
	}
	txid := h.Tx.TxID().String()
	var status atomic.Value
	status.Store("")
	target, _ := url.Parse(r.srv.URL)
	proxy := httputil.NewSingleHostReverseProxy(target)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if s := status.Load().(string); s != "" && strings.Contains(req.URL.Path, "/utxos/") {
			w.Header().Set("Content-Type", "application/json")
			if s == "404" {
				http.Error(w, `{"message":"NOT_FOUND (3): not found"}`, http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, `[{"txid":"`+txid+`","vout":0,"lockingScript":"00","satoshis":2000,"utxoHash":"`+strings.Repeat("20", 32)+`","status":"`+s+`"}]`)
			return
		}
		proxy.ServeHTTP(w, req)
	}))
	defer node.Close()
	hc, _ := headers.NewSource(r.srv.URL)
	v, err := chaincfg.Open(chaincfg.Spec{Network: "regtest", Chain: "asset:" + node.URL, Settle: "rpc:" + node.URL + "/rpc", Headers: r.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{view: v, chain: v.Chain, hc: hc}
	e := &EntryRec{Writer: hex.EncodeToString(r.writer.Identity()), Hold: &HoldRec{Txid: txid, Vout: 0, Sats: 2000}}

	// The test chain's own answer: OK, unspent.
	if _, err := p.holdOutput(ctx, e); err != nil {
		t.Fatalf("an unspent hold output: %v", err)
	}
	for _, s := range []string{"IMMATURE", "FROZEN", "CONFLICTING", "LOCKED", "NOT_FOUND", "SOMETHING_NEW", "404"} {
		status.Store(s)
		_, err := p.holdOutput(ctx, e)
		var re *retryError
		if !errors.As(err, &re) {
			t.Fatalf("status %s: want a retry (no evidence), got %v", s, err)
		}
	}
}
