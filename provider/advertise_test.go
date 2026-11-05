package provider

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// countSettler counts the submissions of each transaction.
type countSettler struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *countSettler) Submit(_ context.Context, tx *transaction.Transaction) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[tx.TxID().String()]++
	return nil
}

func (c *countSettler) Name() string { return "count" }

// An unmined ancestor goes on the leg ahead of the first transaction that
// spends it, and not again for every later spender; after ancestorEvery
// it goes once more.
func TestUnminedAncestorSentOnce(t *testing.T) {
	now := time.Now()
	cs := &countSettler{}
	p := &Provider{settler: cs, metrics: newMetrics(), opts: Options{Now: func() time.Time { return now }}}
	tree := transaction.NewTransaction()
	tree.AddOutput(&transaction.TransactionOutput{Satoshis: 200, LockingScript: &script.Script{}})
	tree.AddOutput(&transaction.TransactionOutput{Satoshis: 200, LockingScript: &script.Script{}})
	spender := func(vout uint32) *transaction.Transaction {
		tx := transaction.NewTransaction()
		tx.AddInput(&transaction.TransactionInput{SourceTXID: tree.TxID(), SourceTxOutIndex: vout, SourceTransaction: tree})
		return tx
	}
	a, b := spender(0), spender(1)
	for _, tx := range []*transaction.Transaction{a, b} {
		if err := p.send(context.Background(), tx); err != nil {
			t.Fatal(err)
		}
	}
	if cs.n[tree.TxID().String()] != 1 || cs.n[a.TxID().String()] != 1 || cs.n[b.TxID().String()] != 1 {
		t.Fatalf("submissions %v", cs.n)
	}
	now = now.Add(ancestorEvery)
	if err := p.send(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if cs.n[tree.TxID().String()] != 2 {
		t.Fatalf("the ancestor was not sent again after %v: %v", ancestorEvery, cs.n)
	}
	if _, n := p.metrics.sum("bstore_provider_advertise_seconds", "stage=submit"); n != 3 {
		t.Fatalf("%v submits observed", n)
	}
}

// A histogram is served as Prometheus has it: cumulative buckets, +Inf,
// a sum and a count.
func TestHistogramServed(t *testing.T) {
	m := newMetrics()
	m.observe("x_seconds", 0.003, "stage=a")
	m.observe("x_seconds", 0.3, "stage=a")
	m.observe("x_seconds", 30, "stage=a")
	rec := httptest.NewRecorder()
	m.serve(rec, nil)
	body := rec.Body.String()
	for _, want := range []string{`x_seconds_bucket{stage="a",le="0.0025"} 0`, `x_seconds_bucket{stage="a",le="0.005"} 1`, `x_seconds_bucket{stage="a",le="0.5"} 2`,
		`x_seconds_bucket{stage="a",le="+Inf"} 3`, `x_seconds_sum{stage="a"} 30.303`, `x_seconds_count{stage="a"} 3`} {
		if !strings.Contains(body, want) {
			t.Fatalf("no %q in\n%s", want, body)
		}
	}
}
