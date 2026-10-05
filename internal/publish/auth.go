package publish

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	authhttp "github.com/bsv-blockchain/go-sdk/auth/clients/authhttp"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/purse"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// authClient is the identity's BRC-103 client over BRC-104 (go-sdk
// AuthFetch), one session per server, whose wallet is a bcommon purse: it
// pays a BRC-105 402 from the identity's pool, up to the bound of the call
// in flight and never more. Calls are made one at a time.
type authClient struct {
	p      *Publisher
	hc     *http.Client
	mu     sync.Mutex
	af     *authhttp.AuthFetch
	paying *swapPurse
}

// swapPurse is the AuthFetch's wallet: the purse of the call in flight.
type swapPurse struct{ *purse.Purse }

func newAuthClient(p *Publisher, hc *http.Client) *authClient {
	if hc == nil {
		hc = &http.Client{Timeout: 2 * time.Minute}
	}
	// The SDK reads a whole answer before a caller can bound it: the bound
	// is the transport's, so no server can stream an answer without end.
	c := *hc
	base := c.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	c.Transport = limitTransport{base: base, max: maxAnswer}
	return &authClient{p: p, hc: &c}
}

// maxAnswer is the most any authenticated answer may be: a signed
// response at its bound, and room for the envelope.
const maxAnswer = storerec.MaxSignedResponse + 1<<20

// limitTransport cuts every answer's body at max bytes, failing the read
// past it.
type limitTransport struct {
	base http.RoundTripper
	max  int64
}

func (t limitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if res.ContentLength > t.max {
		res.Body.Close()
		return nil, fmt.Errorf("an answer of %d bytes, over %d", res.ContentLength, t.max)
	}
	res.Body = &limitedBody{rc: res.Body, left: t.max}
	return res, nil
}

type limitedBody struct {
	rc   io.ReadCloser
	left int64
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, errors.New("an answer over the bound")
	}
	if int64(len(p)) > b.left {
		p = p[:b.left+1]
	}
	n, err := b.rc.Read(p)
	b.left -= int64(n)
	if b.left < 0 {
		return n, errors.New("an answer over the bound")
	}
	return n, err
}

func (b *limitedBody) Close() error { return b.rc.Close() }

// Call is one authenticated request's answer: its status, its body (at
// most max bytes), and what was paid for it (0 when nothing was).
type Call struct {
	Status int
	Body   []byte
	Paid   uint64
	// Payment is the BRC-105 payment made, when one was.
	Payment *transaction.Transaction

	retryAfter string
}

// ErrOverBudget is a 402 whose price is over the bound the call was given.
var ErrOverBudget = errors.New("publish: the price is over the budget")

func (a *authClient) purse(ctx context.Context, maxPay uint64) *purse.Purse {
	p := a.p
	tip := uint32(0)
	if h, err := p.cfg.Asset.BestHeader(ctx); err == nil {
		tip = h.Height
	}
	newPayer := func() *producer.Payer {
		return &producer.Payer{Pool: p.wallet.Pool, Tip: tip, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer},
			Kept: p.kept, Settler: p.cfg.Settler, Asset: p.cfg.Asset, Fees: p.cfg.Fees, Note: p.note}
	}
	return &purse.Purse{Embedded: p.wallet, NewPayer: newPayer, Fees: p.cfg.Fees, MaxPay: maxPay,
		Settler: p.cfg.Settler, Asset: p.cfg.Asset, Wait: 10 * time.Minute, Poll: 250 * time.Millisecond}
}

// do sends one request over BRC-104, paying a 402 up to maxPay. A payment
// the server accepted is broadcast from here too (it is this identity's own
// transaction; the server broadcasts the same bytes).
func (a *authClient) do(ctx context.Context, method, url string, headers map[string]string, body []byte, maxPay uint64, max int64) (*Call, error) {
	for try := 0; ; try++ {
		c, err := a.once(ctx, method, url, headers, body, maxPay, max)
		if err != nil || c.Status != http.StatusTooManyRequests || try == 2 {
			return c, err
		}
		// Rate limited: wait as asked, a few seconds at most, then again.
		wait := 2 * time.Second
		if v, err := strconv.Atoi(c.retryAfter); err == nil && v > 0 && v <= 10 {
			wait = time.Duration(v) * time.Second
		}
		select {
		case <-ctx.Done():
			return c, nil
		case <-time.After(wait):
		}
	}
}

func (a *authClient) once(ctx context.Context, method, url string, headers map[string]string, body []byte, maxPay uint64, max int64) (*Call, error) {
	pu := a.purse(ctx, maxPay)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.af == nil {
		a.paying = &swapPurse{}
		a.af = authhttp.New(a.paying, authhttp.WithoutLogging(), authhttp.WithHttpClient(a.hc))
	}
	a.paying.Purse = pu
	res, err := a.af.Fetch(ctx, url, &authhttp.SimplifiedFetchRequestOptions{Method: method, Headers: headers, Body: body})
	if err != nil {
		// A payment made may have reached the payee, which can broadcast
		// it whatever it answered: it is kept, never refunded, and its
		// coin comes back only once the node shows it unspent long after
		// (ReconcilePayments).
		if tx := pu.Settle(); tx != nil {
			a.p.notePayment(tx, false)
		}
		if errors.Is(err, purse.ErrOverMaxPay) || strings.Contains(err.Error(), "at most") && strings.Contains(err.Error(), "satoshis asked") {
			return nil, ErrOverBudget
		}
		return nil, err
	}
	defer res.Body.Close()
	if max <= 0 {
		max = 1 << 20
	}
	b, rerr := io.ReadAll(io.LimitReader(res.Body, max+1))
	c := &Call{Status: res.StatusCode, Body: b, retryAfter: res.Header.Get("Retry-After")}
	if rerr != nil || int64(len(b)) > max || res.StatusCode >= 400 {
		// Not answered, or refused: a payment sent is still the payee's
		// to broadcast, so it is kept as uncertain, never refunded.
		if tx := pu.Settle(); tx != nil {
			a.p.notePayment(tx, false)
			c.Payment = tx
		}
		if rerr != nil {
			return nil, rerr
		}
		if int64(len(b)) > max {
			c.Body = b[:max]
		}
		return c, nil
	}
	if tx := pu.Settle(); tx != nil {
		a.p.notePayment(tx, true)
		c.Payment = tx
		if len(tx.Outputs) > 0 {
			c.Paid = tx.Outputs[0].Satoshis
		}
		if a.p.cfg.Settler != nil {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			if err := a.p.cfg.Settler.Submit(sctx, tx); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
				a.p.cfg.Logf("payment %s: not broadcast from here (%v); the payee broadcasts it", short(tx.TxID().String()), err)
			}
			cancel()
		}
	}
	return c, nil
}

// Post sends a JSON body over BRC-104 to a front (a URL an advertisement
// or a statement named), paying a 402 up to maxPay.
func (p *Publisher) Post(ctx context.Context, url string, body []byte, maxPay uint64) (*Call, error) {
	return p.frontAuth.do(ctx, http.MethodPost, url, map[string]string{"content-type": "application/json"}, body, maxPay, storerec.MaxSignedResponse)
}

// Get sends a GET over BRC-104 and pays nothing.
func (p *Publisher) Get(ctx context.Context, url string) (*Call, error) {
	return p.frontAuth.do(ctx, http.MethodGet, url, nil, nil, 0, 1<<20)
}

// CollectChange asks the node for the proof of every transaction whose
// change the pool holds unproven (a payment this identity made), so that
// change becomes spendable once it has mined.
func (p *Publisher) CollectChange(ctx context.Context) {
	for _, txid := range p.wallet.Pool.UnprovenTxids() {
		mp, height, err := p.cfg.Asset.Proof(ctx, txid)
		if err != nil {
			continue
		}
		_, _ = p.wallet.Pool.Prove(txid, funding.BumpHex(mp), height)
	}
}

// PaymentRecord is a BRC-105 payment this identity made: its fee coin left
// the pool and its change waits, unproven, until it mines. One whose call
// was not answered may never be broadcast by its payee.
type PaymentRecord struct {
	Txid     string         `json:"txid"`
	Coin     bwallet.Output `json:"coin"`
	At       int64          `json:"at"`
	Answered bool           `json:"answered,omitempty"`
}

// paymentWait is how long a payment the node does not know is waited for
// before its coin, when the node shows it unspent, goes back to the pool.
const paymentWait = time.Hour

func (p *Publisher) notePayment(tx *transaction.Transaction, answered bool) {
	if len(tx.Inputs) == 0 || tx.Inputs[0].SourceTransaction == nil {
		return
	}
	in := tx.Inputs[0]
	coin := coinOf(mint.Input{Tx: in.SourceTransaction, Vout: in.SourceTxOutIndex})
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.Payments = append(p.st.Payments, PaymentRecord{Txid: tx.TxID().String(), Coin: coin, At: p.cfg.Now().Unix(), Answered: answered})
	_ = p.saveLocked()
}

// ReconcilePayments settles every payment recorded: one mined has its
// change proven; one the node has not seen for paymentWait, whose coin the
// node shows unspent, gives its coin back to the pool and its change up;
// one whose coin something else spent is forgotten. It returns how many
// are still open.
func (p *Publisher) ReconcilePayments(ctx context.Context) int {
	p.mu.Lock()
	pays := slices.Clone(p.st.Payments)
	p.mu.Unlock()
	var keep []PaymentRecord
	for _, r := range pays {
		if mp, height, err := p.cfg.Asset.Proof(ctx, r.Txid); err == nil {
			_, _ = p.wallet.Pool.Prove(r.Txid, funding.BumpHex(mp), height)
			continue
		}
		if p.cfg.Now().Sub(time.Unix(r.At, 0)) < paymentWait {
			keep = append(keep, r)
			continue
		}
		free, by, err := p.unspent(ctx, r.Coin.TxID, r.Coin.Vout)
		switch {
		case err != nil || (!free && by == r.Txid):
			keep = append(keep, r)
		case free:
			p.dropChange(r.Txid)
			_ = p.wallet.Pool.Return(r.Coin)
			p.cfg.Logf("payment %s: never broadcast; its coin is back in the pool", short(r.Txid))
		default:
			p.dropChange(r.Txid)
		}
	}
	p.mu.Lock()
	p.st.Payments = keep
	_ = p.saveLocked()
	p.mu.Unlock()
	return len(keep)
}

// dropChange removes from the pool the outputs of txid it holds.
func (p *Publisher) dropChange(txid string) {
	for _, o := range p.wallet.Pool.Outputs() {
		if o.TxID == txid {
			_, _ = p.wallet.Pool.Remove(o)
		}
	}
}
