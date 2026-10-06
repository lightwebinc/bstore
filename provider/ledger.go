package provider

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/payee"
	"github.com/lightwebinc/bcommon/purse"
)

// ledger is the provider's payment ledger: payments.jsonl in the data
// directory, the shared payee ledger's lines (bcommon payee, version 2),
// one per payment accepted, flushed to disk before the request is
// answered. A txid is taken once, and so is every coin a payment spends:
// a payment is unbroadcast when it is offered, so two spending one coin
// both verify and at most one can settle.
type ledger struct {
	path   string
	now    func() time.Time
	mu     sync.Mutex
	f      *os.File
	claims *payee.Claims
	ps     []payee.Payment
	// forgotten are payments the network refused for good: their coins
	// claim nothing any more, so the transaction that did spend them (a
	// settlement a decoy remittance hid) can be taken.
	forgotten map[string]bool
}

// find is the ledger line of txid, or nil.
func (l *ledger) find(txid string) *payee.Payment {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range l.ps {
		if l.ps[i].Txid == txid {
			p := l.ps[i]
			return &p
		}
	}
	return nil
}

// forget lets a payment the network refused stop claiming its coins.
func (l *ledger) forget(txid string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.forgotten[txid] = true
	l.rebuild()
}

func (l *ledger) rebuild() {
	var keep []payee.Payment
	for _, p := range l.ps {
		if !l.forgotten[p.Txid] {
			keep = append(keep, p)
		}
	}
	cl := payee.ClaimsOf(keep)
	for t := range l.forgotten {
		cl.Take(t, nil)
	}
	l.claims = cl
}

func openLedger(dir string, now func() time.Time, forgotten []string) (*ledger, error) {
	p := filepath.Join(dir, payee.LedgerFile)
	ps, err := payee.ReadLedgers(nil, p)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	// A line cut short by a crash is ended, so the next starts on its own.
	if st, err := f.Stat(); err == nil && st.Size() > 0 {
		b := make([]byte, 1)
		if r, err := os.Open(p); err == nil {
			_, _ = r.ReadAt(b, st.Size()-1)
			r.Close()
			if b[0] != '\n' {
				_, _ = f.Write([]byte("\n"))
			}
		}
	}
	l := &ledger{path: p, now: now, f: f, ps: ps, forgotten: map[string]bool{}}
	for _, t := range forgotten {
		l.forgotten[t] = true
	}
	l.rebuild()
	return l, nil
}

// take records p unless its txid or a coin it spends was taken before.
func (l *ledger) take(p payee.Payment) (payee.Claim, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.claims.Refusal(p.Txid, p.Inputs); c != payee.Accepted {
		return c, nil
	}
	p.At = l.now().Unix()
	line, err := p.Line()
	if err != nil {
		return 0, err
	}
	if _, err := l.f.Write(line); err != nil {
		return 0, err
	}
	if err := l.f.Sync(); err != nil {
		return 0, err
	}
	l.claims.Take(p.Txid, p.Inputs)
	l.ps = append(l.ps, p)
	return payee.Accepted, nil
}

// refusal is what take would answer for a payment, without taking it.
func (l *ledger) refusal(txid string, inputs []string) payee.Claim {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.claims.Refusal(txid, inputs)
}

func (l *ledger) has(txid string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.claims.Refusal(txid, []string{"-"}) == payee.Replayed
}

func (l *ledger) payments() []payee.Payment {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]payee.Payment(nil), l.ps...)
}

func (l *ledger) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// spends are tx's inputs as "<txid>.<index>".
func spends(tx *transaction.Transaction) []string {
	out := make([]string, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		id := ""
		if in.SourceTXID != nil {
			id = in.SourceTXID.String()
		}
		out = append(out, id+"."+itoa(uint64(in.SourceTxOutIndex)))
	}
	return out
}

// incoming checks that output outputIndex of the BEEF beef pays this
// provider the key BRC-29 derives for prefix, suffix and sender, and that
// the transaction verifies against the provider's headers.
func (p *Provider) incoming(ctx context.Context, beef []byte, outputIndex uint32, prefix, suffix, sender string) (*purse.Incoming, error) {
	r, err := purse.Remittance(prefix, suffix, sender)
	if err != nil {
		return nil, err
	}
	return p.purse().Check(ctx, wallet.InternalizeActionArgs{Tx: beef, Outputs: []wallet.InternalizeOutput{{
		OutputIndex: outputIndex, Protocol: wallet.InternalizeProtocolWalletPayment, PaymentRemittance: r}}})
}

// purse is the payee leg over the provider's wallet.
func (p *Provider) purse() *purse.Purse {
	return &purse.Purse{Embedded: p.wallet, NewPayer: p.newPayer, Fees: p.fees(), Settler: p.settler, Asset: p.asset,
		Headers: p.hc, Wait: 30 * time.Second, Poll: time.Second}
}

// settlePayments takes into the pool every ledger payment not yet settled.
func (p *Provider) settlePayments(ctx context.Context) {
	ps := p.ledger.payments()
	todo := 0
	p.mu.Lock()
	for _, x := range ps {
		if !p.st.IsSettled(x.Txid) && !p.st.IsUnsettleable(x.Txid) {
			todo++
		}
	}
	p.mu.Unlock()
	if todo == 0 {
		return
	}
	s := &payee.Settler{App: "bstore", Payer: p.purse(), Record: lockedBook{p}, Pool: p.pool, InFlight: 4,
		Warn: logWriter{p}}
	rep, err := s.Settle(ctx, ps)
	if err != nil {
		p.logf("settling payments: %v", err)
		return
	}
	if rep.Settled > 0 || rep.Refused > 0 {
		p.logf("payments: %s", rep)
	}
	p.metrics.set("bstore_provider_pool_satoshis", float64(p.pool.Balance()))
}

// lockedBook is the state's payee book as a payee.Record, saved under the
// state's lock.
type lockedBook struct{ p *Provider }

func (b lockedBook) IsSettled(txid string) bool {
	b.p.mu.Lock()
	defer b.p.mu.Unlock()
	return b.p.st.IsSettled(txid)
}

func (b lockedBook) IsUnsettleable(txid string) bool {
	b.p.mu.Lock()
	defer b.p.mu.Unlock()
	return b.p.st.IsUnsettleable(txid)
}

func (b lockedBook) RecordSettled(txid string) error {
	b.p.mu.Lock()
	defer b.p.mu.Unlock()
	b.p.st.AddSettled(txid)
	return b.p.save()
}

func (b lockedBook) RecordUnsettleable(txid, why string) error {
	b.p.mu.Lock()
	defer b.p.mu.Unlock()
	b.p.st.AddUnsettleable(txid, why)
	b.p.ledger.forget(txid)
	return b.p.save()
}

type logWriter struct{ p *Provider }

func (w logWriter) Write(b []byte) (int, error) {
	w.p.logf("%s", string(trimNL(b)))
	return len(b), nil
}

func trimNL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// payment is a ledger line for an incoming payment.
func payment(in *purse.Incoming, beef []byte, outputIndex uint32, sats uint64, prefix, suffix, sender, class string) payee.Payment {
	return payee.Payment{Txid: in.Txid, Beef: base64.StdEncoding.EncodeToString(beef), OutputIndex: outputIndex, Satoshis: sats,
		DerivationPrefix: prefix, DerivationSuffix: suffix, SenderIdentityKey: sender, Class: class, Inputs: spends(in.Tx)}
}
