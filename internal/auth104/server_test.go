package auth104

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	authhttp "github.com/bsv-blockchain/go-sdk/auth/clients/authhttp"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/wallet"
)

func protoWallet(t *testing.T) *wallet.ProtoWallet {
	t.Helper()
	k, err := ec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	w, err := wallet.NewProtoWallet(wallet.ProtoWalletArgs{Type: wallet.ProtoWalletArgsTypePrivateKey, PrivateKey: k})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// clientWallet is a ProtoWallet with the rest of wallet.Interface stubbed:
// the SDK's client asks only the key methods for authentication.
type clientWallet struct {
	wallet.Interface
	pw *wallet.ProtoWallet
}

func (c clientWallet) GetPublicKey(ctx context.Context, a wallet.GetPublicKeyArgs, o string) (*wallet.GetPublicKeyResult, error) {
	return c.pw.GetPublicKey(ctx, a, o)
}
func (c clientWallet) CreateSignature(ctx context.Context, a wallet.CreateSignatureArgs, o string) (*wallet.CreateSignatureResult, error) {
	return c.pw.CreateSignature(ctx, a, o)
}
func (c clientWallet) VerifySignature(ctx context.Context, a wallet.VerifySignatureArgs, o string) (*wallet.VerifySignatureResult, error) {
	return c.pw.VerifySignature(ctx, a, o)
}
func (c clientWallet) CreateHMAC(ctx context.Context, a wallet.CreateHMACArgs, o string) (*wallet.CreateHMACResult, error) {
	return c.pw.CreateHMAC(ctx, a, o)
}
func (c clientWallet) VerifyHMAC(ctx context.Context, a wallet.VerifyHMACArgs, o string) (*wallet.VerifyHMACResult, error) {
	return c.pw.VerifyHMAC(ctx, a, o)
}

// recorder keeps a copy of every request the client sends.
type recorder struct {
	mu   sync.Mutex
	reqs []*http.Request
	body [][]byte
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	var b []byte
	if req.Body != nil {
		b, _ = io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	r.mu.Lock()
	r.reqs = append(r.reqs, req.Clone(context.Background()))
	r.body = append(r.body, b)
	r.mu.Unlock()
	return http.DefaultTransport.RoundTrip(req)
}

func newServer(t *testing.T, cfg Config) (*Server, *httptest.Server) {
	t.Helper()
	if cfg.Wallet == nil {
		cfg.Wallet = protoWallet(t)
	}
	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(HandshakePath, s.Handshake)
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		s.Serve(w, r, 1024, func(_ context.Context, _ *http.Request, body []byte, who *Caller) Reply {
			id := ""
			if who != nil {
				id = who.Hex
			}
			return JSON(200, map[string]string{"who": id, "body": string(body)}, "X-Bsv-Note", "signed")
		})
	})
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	return s, hs
}

func TestAuthenticatedRoundTrip(t *testing.T) {
	srv, hs := newServer(t, Config{})
	cw := protoWallet(t)
	rec := &recorder{}
	c := authhttp.New(clientWallet{pw: cw}, authhttp.WithoutLogging(), authhttp.WithHttpClient(&http.Client{Transport: rec}))
	ctx := context.Background()
	for i := range 3 {
		resp, err := c.Fetch(ctx, hs.URL+"/echo", &authhttp.SimplifiedFetchRequestOptions{Method: "POST",
			Headers: map[string]string{"content-type": "application/json"}, Body: []byte(`{"n":` + string(rune('0'+i)) + `}`)})
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&got)
		resp.Body.Close()
		me, _ := cw.GetPublicKey(ctx, wallet.GetPublicKeyArgs{IdentityKey: true}, "")
		if got["who"] != hexKey(me.PublicKey) || resp.StatusCode != 200 {
			t.Fatalf("answer %d %v", resp.StatusCode, got)
		}
	}
	if srv.Sessions() != 1 {
		t.Fatalf("sessions %d", srv.Sessions())
	}
	// The last general request, sent again byte for byte, is refused
	// without being answered.
	rec.mu.Lock()
	last, body := rec.reqs[len(rec.reqs)-1], rec.body[len(rec.body)-1]
	rec.mu.Unlock()
	again := last.Clone(ctx)
	again.Body = io.NopCloser(bytes.NewReader(body))
	again.RequestURI = ""
	res, err := http.DefaultClient.Do(again)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 401 || !strings.Contains(string(b), "ERR_AUTH_REPLAYED") {
		t.Fatalf("replay answered %d %s", res.StatusCode, b)
	}
	// A request altered after signing does not verify.
	forged := last.Clone(ctx)
	forged.Body = io.NopCloser(bytes.NewReader([]byte(`{"n":9}`)))
	forged.ContentLength = 7
	forged.RequestURI = ""
	forged.Header.Set("X-Bsv-Auth-Request-Id", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	res, err = http.DefaultClient.Do(forged)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 401 || !strings.Contains(string(b), "ERR_AUTH_FAILED") {
		t.Fatalf("forgery answered %d %s", res.StatusCode, b)
	}
}

func TestUnauthenticatedAndPartial(t *testing.T) {
	_, hs := newServer(t, Config{})
	res, err := http.Post(hs.URL+"/echo", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("plain %d", res.StatusCode)
	}
	req, _ := http.NewRequest("POST", hs.URL+"/echo", strings.NewReader(`{}`))
	req.Header.Set("X-Bsv-Auth-Nonce", "x")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("partial %d", res.StatusCode)
	}
	big := strings.Repeat("a", 2000)
	res, err = http.Post(hs.URL+"/echo", "application/json", strings.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 413 {
		t.Fatalf("over the cap %d", res.StatusCode)
	}
}

func TestHandshakeBudget(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	_, hs := newServer(t, Config{Now: clock, Handshakes: Budget{Rate: 1, Burst: 2}, PerAddress: Budget{Rate: 1, Burst: 1}})
	shake := func() int {
		k, _ := ec.NewPrivateKey()
		body := `{"version":"0.1","messageType":"initialRequest","identityKey":"` + hexKey(k.PubKey()) + `","initialNonce":"` + strings.Repeat("A", 43) + `=","requestedCertificates":{"certifiers":[],"types":{}}}`
		res, err := http.Post(hs.URL+HandshakePath, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	if s := shake(); s != 200 {
		t.Fatalf("first handshake %d", s)
	}
	if s := shake(); s != 429 {
		t.Fatalf("second handshake from one address %d", s)
	}
	now = now.Add(2 * time.Second)
	if s := shake(); s != 200 {
		t.Fatalf("after the refill %d", s)
	}
}

func TestResponseBudgetAndEviction(t *testing.T) {
	_, hs := newServer(t, Config{Responses: Budget{Rate: 0.001, Burst: 2}})
	cw := protoWallet(t)
	c := authhttp.New(clientWallet{pw: cw}, authhttp.WithoutLogging())
	ctx := context.Background()
	statuses := []int{}
	for range 3 {
		resp, err := c.Fetch(ctx, hs.URL+"/echo", &authhttp.SimplifiedFetchRequestOptions{Method: "POST",
			Headers: map[string]string{"content-type": "application/json"}, Body: []byte(`{}`)})
		if err != nil {
			statuses = append(statuses, -1)
			continue
		}
		statuses = append(statuses, resp.StatusCode)
		resp.Body.Close()
	}
	if statuses[0] != 200 || statuses[1] != 200 || statuses[2] == 200 {
		t.Fatalf("statuses %v", statuses)
	}
}

func TestSessionsForgetWholeSessions(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := newSessions(2, 3, time.Minute, func() time.Time { return now }, nil)
	k1, _ := ec.NewPrivateKey()
	s.add("a", "pa", k1.PubKey())
	s.add("b", "pb", k1.PubKey())
	if !s.take("a", "r1") || !s.take("a", "r2") || s.take("a", "r1") {
		t.Fatal("take")
	}
	if !s.take("b", "r1") {
		t.Fatal("take b")
	}
	// A fourth remembered request is over the cap of three: the least
	// recently used session (a) is forgotten whole, and its requests can no
	// longer verify, so nothing is replayable.
	if !s.take("b", "r2") {
		t.Fatal("take b2")
	}
	if s.get("a") != nil || s.get("b") == nil {
		t.Fatal("a should be forgotten, b kept")
	}
	s.add("c", "pc", k1.PubKey())
	s.add("d", "pd", k1.PubKey())
	if s.get("b") != nil || s.len() != 2 {
		t.Fatal("the cap on sessions")
	}
	now = now.Add(2 * time.Minute)
	if s.get("c") != nil {
		t.Fatal("idle session kept")
	}
}

func TestAddressKey(t *testing.T) {
	for in, want := range map[string]string{
		"192.0.2.1:443":             "192.0.2.1",
		"[::ffff:192.0.2.1]:80":     "192.0.2.1",
		"[2001:db8:1:2:3:4:5:6]:80": "2001:db8:1:2::/64",
		"2001:db8:1:2::9":           "2001:db8:1:2::/64",
	} {
		if got := AddressKey(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}
