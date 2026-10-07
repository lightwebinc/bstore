package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lightwebinc/bcommon/bwallet"

	"github.com/lightwebinc/bstore/chaincfg"
)

// Doctor reports a provider's identity, its pool, its store, and whether
// the chain view, the header source, the settlement leg, the hosts, the
// facade and its own public routes answer, and the fee policy in force. It reads only: it opens the wallet to read the pool,
// walks the store's directory, and asks each endpoint one GET. ca, when
// set, is a PEM file trusted besides the system's for the provider's own
// public URL. It returns an error only when the configuration or the
// wallet cannot be read; an endpoint that does not answer is reported.
func Doctor(ctx context.Context, cfg *Config, ca string, w io.Writer) error {
	wl, err := bwallet.Open(WalletDir(cfg.Data), Profile)
	if err != nil {
		return err
	}
	addr, err := wl.Signer().FundAddress(cfg.Mainnet)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "identity     %s\n", wl.Signer().IdentityHex())
	fmt.Fprintf(w, "fund address %s\n", addr)
	fmt.Fprintf(w, "pool         %d output(s), %d satoshis\n", wl.Pool.Count(), wl.Pool.Balance())
	fmt.Fprintf(w, "backend      %s\n", cfg.Backend)
	n, b := storeSize(filepath.Join(cfg.Data, "store", "objects"))
	roots := 0
	if ents, err := os.ReadDir(filepath.Join(cfg.Data, "roots")); err == nil {
		roots = len(ents)
	}
	fmt.Fprintf(w, "store        %d object(s), %d bytes; %d root(s) with segment hashes; quota %d\n", n, b, roots, cfg.QuotaBytes)
	v, err := chaincfg.Open(cfg.Spec())
	if err != nil {
		return err
	}
	for _, l := range v.Report(ctx) {
		fmt.Fprintf(w, "%-12s %s\n", l.Label, l.Text)
	}
	plain := &http.Client{Timeout: 10 * time.Second}
	for _, h := range cfg.Hosts {
		fmt.Fprintf(w, "host         %s: %s\n", h, Reach(ctx, plain, strings.TrimRight(h, "/")+"/healthz", strings.TrimRight(h, "/")+"/"))
	}
	if cfg.Facade != "" {
		fmt.Fprintf(w, "facade       %s: %s\n", cfg.Facade, Reach(ctx, plain, strings.TrimRight(cfg.Facade, "/")+"/"))
	}
	own, err := clientWithCA(ca)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "front        %s: %s\n", cfg.FrontURL(), Reach(ctx, own, cfg.FrontURL()+"/terms"))
	for _, b := range cfg.Buckets {
		fmt.Fprintf(w, "bucket       %s\n", b.Bucket)
	}
	return nil
}

// Reach is one GET of each URL in turn: the first that answers below 500
// is reported with its status, else why the last did not.
func Reach(ctx context.Context, hc *http.Client, urls ...string) string {
	last := "nothing asked"
	for _, u := range urls {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		req, err := http.NewRequestWithContext(cctx, http.MethodGet, u, nil)
		if err != nil {
			cancel()
			last = firstLine(err.Error())
			continue
		}
		res, err := hc.Do(req)
		cancel()
		if err != nil {
			last = firstLine(err.Error())
			continue
		}
		res.Body.Close()
		if res.StatusCode < 500 {
			return fmt.Sprintf("answers (%d)", res.StatusCode)
		}
		last = fmt.Sprintf("%d", res.StatusCode)
	}
	return "does not answer: " + last
}

func clientWithCA(ca string) (*http.Client, error) {
	hc := &http.Client{Timeout: 10 * time.Second}
	if ca == "" {
		return hc, nil
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("doctor: %s holds no certificate", ca)
	}
	hc.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	return hc, nil
}

func storeSize(dir string) (n int, bytes int64) {
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if fi, err := d.Info(); err == nil {
			n++
			bytes += fi.Size()
		}
		return nil
	})
	return n, bytes
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}
