package publish

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
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

	"github.com/lightwebinc/bstore/storerec"
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
	// Payment is the BRC-105 payment made, when one was; Peer the
	// identity key (hex) the BRC-104 session authenticated the answer as.
	Payment *transaction.Transaction
	Peer    string

	retryAfter string
}

// ErrOverBudget is a 402 whose price is over the bound the call was given.
var ErrOverBudget = errors.New("publish: the price is over the budget")

// ErrCannotPay is a 402 this identity could not pay: its pool had no coin
// to spend (change waiting for its proof, or none at all). It is the
// buyer's own state, never the answerer's failure.
var ErrCannotPay = errors.New("publish: this identity cannot pay now")

// purse is the wallet of one call: it pays from pool, which is the
// identity's pool or a payment pool of one small coin (paycoins.go).
func (a *authClient) purse(ctx context.Context, maxPay uint64, pool *bwallet.Pool) *purse.Purse {
	p := a.p
	tip := uint32(0)
	if h, err := p.bestHeader(ctx); err == nil {
		tip = h.Height
	}
	newPayer := func() *producer.Payer {
		return &producer.Payer{Pool: pool, Tip: tip, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer},
			Kept: p.kept, Settler: p.cfg.Settler, Chain: p.cfg.Chain, Fees: p.fees(), Note: p.note, Poll: p.cfg.Poll}
	}
	return &purse.Purse{Embedded: p.wallet, NewPayer: newPayer, Fees: p.fees(), MaxPay: maxPay,
		Settler: p.cfg.Settler, Chain: p.cfg.Chain, Wait: 10 * time.Minute, Poll: 250 * time.Millisecond}
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
	// A call that may pay takes a small coin of its own, when one is
	// ready: a payment the payee refuses holds only that coin.
	pool, done := a.p.wallet.Pool, func() {}
	var coin *bwallet.Output
	if maxPay > 0 {
		pool, done, coin = a.p.payPool(ctx, maxPay)
	}
	defer done()
	small := pool != a.p.wallet.Pool
	if coin != nil {
		// The coin's use is recorded before the call: a stop after a
		// payment left and before it is recorded leaves this, and the coin
		// is settled as an unanswered payment's would be.
		a.p.noteIntent(*coin)
		defer a.p.dropIntent(*coin)
	}
	pu := a.purse(ctx, maxPay, pool)
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
			a.p.notePayment(tx, false, small)
		}
		return nil, payErr(err)
	}
	defer res.Body.Close()
	if max <= 0 {
		max = 1 << 20
	}
	b, rerr := io.ReadAll(io.LimitReader(res.Body, max+1))
	c := &Call{Status: res.StatusCode, Body: b, retryAfter: res.Header.Get("Retry-After"),
		Peer: strings.ToLower(res.Header.Get("x-bsv-auth-identity-key"))}
	if rerr != nil || int64(len(b)) > max || res.StatusCode >= 400 {
		// Not answered, or refused: a payment sent is still the payee's
		// to broadcast, so it is kept as uncertain, never refunded.
		if tx := pu.Settle(); tx != nil {
			a.p.notePayment(tx, false, small)
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
		a.p.notePayment(tx, true, small)
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

// payErr names a failed authenticated call's payment errors: a price over
// the call's bound (ErrOverBudget) and a pool with nothing to pay it from
// (ErrCannotPay); any other error is returned as it is.
func payErr(err error) error {
	switch {
	case errors.Is(err, purse.ErrOverMaxPay) || strings.Contains(err.Error(), "at most") && strings.Contains(err.Error(), "satoshis asked"):
		return ErrOverBudget
	case errors.Is(err, bwallet.ErrNoSpendable):
		return fmt.Errorf("%w: %v", ErrCannotPay, err)
	}
	return err
}

// Post sends a JSON body over BRC-104 to a front (a URL an advertisement
// or a statement named), paying a 402 up to maxPay.
func (p *Publisher) Post(ctx context.Context, url string, body []byte, maxPay uint64) (*Call, error) {
	return p.frontAuth.client(url).do(ctx, http.MethodPost, url, map[string]string{"content-type": "application/json"}, body, maxPay, storerec.MaxSignedResponse)
}

// Get sends a GET over BRC-104 and pays nothing.
func (p *Publisher) Get(ctx context.Context, url string) (*Call, error) {
	return p.frontAuth.client(url).do(ctx, http.MethodGet, url, nil, nil, 0, 1<<20)
}

// authPool is one authClient per server (scheme and host): a client makes
// its calls one at a time, and calls to different fronts (a race's
// candidates) go at once.
type authPool struct {
	p  *Publisher
	hc *http.Client
	mu sync.Mutex
	by map[string]*authClient
}

func newAuthPool(p *Publisher, hc *http.Client) *authPool {
	return &authPool{p: p, hc: hc, by: map[string]*authClient{}}
}

// maxAuthClients bounds the servers kept; past it the pool starts again.
const maxAuthClients = 256

func (ap *authPool) client(raw string) *authClient {
	key := raw
	if u, err := neturl.Parse(raw); err == nil {
		key = u.Scheme + "://" + u.Host
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	if c := ap.by[key]; c != nil {
		return c
	}
	if len(ap.by) >= maxAuthClients {
		ap.by = map[string]*authClient{}
	}
	c := newAuthClient(ap.p, ap.hc)
	ap.by[key] = c
	return c
}

// CollectChange asks the node for the proof of every transaction whose
// change the pool holds unproven (a payment this identity made), so that
// change becomes spendable once it has mined.
func (p *Publisher) CollectChange(ctx context.Context) {
	p.proveCoins(ctx)
	for _, txid := range p.wallet.Pool.UnprovenTxids() {
		mp, height, err := p.cfg.Chain.Proof(ctx, txid)
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
	// Small marks a fee coin taken from the payment coins, where it goes
	// back.
	Small bool `json:"small,omitempty"`
}

// paymentWait is how long a payment the node does not know is waited for
// before its coin, when the node shows it unspent, goes back to the pool.
const paymentWait = time.Hour

func (p *Publisher) notePayment(tx *transaction.Transaction, answered, small bool) {
	if len(tx.Inputs) == 0 || tx.Inputs[0].SourceTransaction == nil {
		return
	}
	in := tx.Inputs[0]
	coin := coinOf(mint.Input{Tx: in.SourceTransaction, Vout: in.SourceTxOutIndex})
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.Payments = append(p.st.Payments, PaymentRecord{Txid: tx.TxID().String(), Coin: coin, At: p.cfg.Now().Unix(), Answered: answered, Small: small})
	_ = p.saveLocked()
}

// ReconcilePayments settles every payment recorded: one mined has its
// change proven; one the node has not seen for paymentWait, whose coin the
// node shows unspent, gives its coin back to the pool and its change up;
// one whose coin something else spent is forgotten. It returns how many
// are still open.
func (p *Publisher) ReconcilePayments(ctx context.Context) int {
	p.reconcileIntents(ctx)
	p.mu.Lock()
	pays := slices.Clone(p.st.Payments)
	p.mu.Unlock()
	settled := map[string]bool{}
	for _, r := range pays {
		if mp, height, err := p.cfg.Chain.Proof(ctx, r.Txid); err == nil {
			_, _ = p.wallet.Pool.Prove(r.Txid, funding.BumpHex(mp), height)
			settled[r.Txid] = true
			continue
		}
		if p.cfg.Now().Sub(time.Unix(r.At, 0)) < paymentWait {
			continue
		}
		free, by, err := p.unspent(ctx, r.Coin.TxID, r.Coin.Vout)
		switch {
		case err != nil || (!free && by == r.Txid):
		case free:
			// The coin goes back before the record goes: a stop between
			// the two finds the record and returns the coin again, which
			// the pool takes once.
			p.dropChange(r.Txid)
			p.returnCoin(r.Coin, r.Small)
			settled[r.Txid] = true
			p.cfg.Logf("payment %s: never broadcast; its coin is back in the pool", short(r.Txid))
		default:
			p.dropChange(r.Txid)
			settled[r.Txid] = true
		}
	}
	// Only the records settled here go: one noted meanwhile stays.
	p.mu.Lock()
	defer p.mu.Unlock()
	keep := p.st.Payments[:0:0]
	for _, r := range p.st.Payments {
		if !settled[r.Txid] {
			keep = append(keep, r)
		}
	}
	p.st.Payments = keep
	_ = p.saveLocked()
	return len(keep)
}

// noteIntent records a payment coin taken for a call, before the call.
func (p *Publisher) noteIntent(c bwallet.Output) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.Intents = append(p.st.Intents, PaymentRecord{Coin: c, At: p.cfg.Now().Unix(), Small: true})
	_ = p.saveLocked()
}

// dropIntent forgets the record once the call has ended and any payment
// is recorded.
func (p *Publisher) dropIntent(c bwallet.Output) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.Intents = slices.DeleteFunc(p.st.Intents, func(r PaymentRecord) bool { return r.Coin.Outpoint() == c.Outpoint() })
	_ = p.saveLocked()
}

// reconcileIntents settles the coins of calls a stop cut short, an hour
// on: one the node shows unspent goes back to the payment coins; one spent
// is forgotten.
func (p *Publisher) reconcileIntents(ctx context.Context) {
	p.mu.Lock()
	ins := slices.Clone(p.st.Intents)
	p.mu.Unlock()
	done := map[string]bool{}
	for _, r := range ins {
		if p.cfg.Now().Sub(time.Unix(r.At, 0)) < paymentWait {
			continue
		}
		free, _, err := p.unspent(ctx, r.Coin.TxID, r.Coin.Vout)
		if err != nil {
			continue
		}
		if free {
			p.returnCoin(r.Coin, true)
		}
		done[r.Coin.Outpoint()] = true
	}
	if len(done) == 0 {
		return
	}
	p.mu.Lock()
	p.st.Intents = slices.DeleteFunc(p.st.Intents, func(r PaymentRecord) bool { return done[r.Coin.Outpoint()] })
	_ = p.saveLocked()
	p.mu.Unlock()
}

// ReleasePayment gives back the coin of a payment recorded as not
// answered, at once: its payee signed a note that it did not take it (row
// C30). The payment's change goes, and its fee coin returns to the pool.
// It reports whether a payment was released.
func (p *Publisher) ReleasePayment(txid string) bool {
	p.mu.Lock()
	var rec *PaymentRecord
	for i := range p.st.Payments {
		if p.st.Payments[i].Txid == txid {
			r := p.st.Payments[i]
			rec = &r
			break
		}
	}
	p.mu.Unlock()
	if rec == nil {
		return false
	}
	// The coin back first: a stop before the record goes returns it again,
	// and the pool takes an outpoint once.
	p.dropChange(txid)
	p.returnCoin(rec.Coin, rec.Small)
	p.mu.Lock()
	p.st.Payments = slices.DeleteFunc(p.st.Payments, func(r PaymentRecord) bool { return r.Txid == txid })
	err := p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		p.cfg.Logf("payment %s: %v", short(txid), err)
	}
	p.cfg.Logf("payment %s: its payee signed that it did not take it; its coin is back in the pool", short(txid))
	return true
}

// dropChange removes from the pool the outputs of txid it holds.
func (p *Publisher) dropChange(txid string) {
	for _, o := range p.wallet.Pool.Outputs() {
		if o.TxID == txid {
			_, _ = p.wallet.Pool.Remove(o)
		}
	}
}
