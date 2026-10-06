package efleg

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
)

type capture struct{ got *transaction.Transaction }

func (c *capture) Submit(_ context.Context, tx *transaction.Transaction) error {
	c.got = tx
	return nil
}
func (c *capture) Name() string { return "capture" }

// A transaction rebuilt from its bytes reaches the leg with each input's
// source read from the node, so it encodes as Extended Format; the
// caller's transaction is left as it was.
func TestHydrate(t *testing.T) {
	grand := transaction.NewTransaction()
	grand.AddOutput(&transaction.TransactionOutput{Satoshis: 4000, LockingScript: &script.Script{0x51}})
	parent := transaction.NewTransaction()
	parent.AddInputFromTx(grand, 0, nil)
	parent.Inputs[0].UnlockingScript = &script.Script{0x51}
	parent.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &script.Script{0x51}})
	parent.AddOutput(&transaction.TransactionOutput{Satoshis: 2000, LockingScript: &script.Script{0x51}})
	child := transaction.NewTransaction()
	child.AddInputFromTx(parent, 1, nil)
	child.Inputs[0].UnlockingScript = &script.Script{0x51}
	child.AddOutput(&transaction.TransactionOutput{Satoshis: 1500, LockingScript: &script.Script{0x51}})
	rebuilt, err := transaction.NewTransactionFromBytes(child.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	asked := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked++
		if strings.Contains(r.URL.Path, parent.TxID().String()) {
			_, _ = w.Write(parent.Bytes())
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := &capture{}
	s := Wrap(c, &nodeapi.Asset{Base: srv.URL})
	if err := s.Submit(context.Background(), rebuilt); err != nil {
		t.Fatal(err)
	}
	if c.got == rebuilt || c.got.Inputs[0].SourceTransaction == nil {
		t.Fatal("the leg got the transaction without its input's source")
	}
	if rebuilt.Inputs[0].SourceTransaction != nil {
		t.Fatal("the caller's transaction was changed")
	}
	ef, err := c.got.EF()
	if err != nil || len(ef) == 0 {
		t.Fatalf("EF: %v", err)
	}
	if c.got.TxID().String() != child.TxID().String() {
		t.Fatal("the copy is another transaction")
	}
	// A transaction with every source already attached goes as it is.
	c.got = nil
	if err := s.Submit(context.Background(), child); err != nil || c.got != child {
		t.Fatal("a complete transaction was copied")
	}
	if asked == 0 {
		t.Fatal("the node was never asked")
	}
}
