package publish

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	authhttp "github.com/bsv-blockchain/go-sdk/auth/clients/authhttp"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
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
	return &authClient{p: p, hc: hc}
}

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
		pu.Refund()
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
		// Not answered, or refused: whatever was offered is not kept.
		pu.Refund()
		if rerr != nil {
			return nil, rerr
		}
		if int64(len(b)) > max {
			c.Body = b[:max]
		}
		return c, nil
	}
	if tx := pu.Settle(); tx != nil {
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

// Post sends a JSON body over BRC-104, paying a 402 up to maxPay.
func (p *Publisher) Post(ctx context.Context, url string, body []byte, maxPay uint64) (*Call, error) {
	return p.auth.do(ctx, http.MethodPost, url, map[string]string{"content-type": "application/json"}, body, maxPay, storerec.MaxSignedResponse)
}

// Get sends a GET over BRC-104 and pays nothing.
func (p *Publisher) Get(ctx context.Context, url string) (*Call, error) {
	return p.auth.do(ctx, http.MethodGet, url, nil, nil, 0, 1<<20)
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
