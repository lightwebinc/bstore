package reader

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

type roots map[uint32]chainhash.Hash

func (h roots) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	r, ok := h[height]
	return ok && r == *root, nil
}
func (h roots) CurrentHeight(context.Context) (uint32, error) { return 0, nil }

type down struct{ err error }

func (d down) IsValidRootForHeight(context.Context, *chainhash.Hash, uint32) (bool, error) {
	return false, d.err
}
func (d down) CurrentHeight(context.Context) (uint32, error) { return 0, d.err }

// minedTx is a transaction proven at height 7, beside one sibling.
func minedTx(t *testing.T) (*transaction.Transaction, chainhash.Hash) {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: &script.Script{script.OpTRUE}})
	id := tx.TxID()
	yes := true
	sibling := chainhash.Hash{1}
	tx.MerklePath = transaction.NewMerklePath(7, [][]*transaction.PathElement{{{Offset: 0, Hash: id, Txid: &yes}, {Offset: 1, Hash: &sibling}}})
	root, err := tx.MerklePath.ComputeRoot(id)
	if err != nil {
		t.Fatal(err)
	}
	return tx, *root
}

// A lab web node under load ran out of local ports, and every read it
// could not check against its headers answered "its proof does not verify".
// The reader keeps the two apart: a proof that fails, and headers that did
// not answer.
func TestAHeaderOutageIsNotAFailedProof(t *testing.T) {
	tx, root := minedTx(t)
	ctx := context.Background()
	if err := (&Reader{Headers: roots{7: root}}).mined(ctx, tx); err != nil {
		t.Fatalf("proven: %v", err)
	}
	if err := (&Reader{Headers: roots{7: {}}}).mined(ctx, tx); !errors.Is(err, errNotMined) || errors.Is(err, ErrHeaders) {
		t.Fatalf("another root: %v, want not mined", err)
	}
	dial := errors.New("dial tcp: connect: cannot assign requested address")
	err := (&Reader{Headers: down{dial}}).mined(ctx, tx)
	if !errors.Is(err, ErrHeaders) || errors.Is(err, errNotMined) {
		t.Fatalf("headers down: %v, want ErrHeaders", err)
	}
}

// A read across every host that stopped at the headers ends unavailable
// and names the headers; a refusal still wins.
func TestUnavailableCarriesTheHeaders(t *testing.T) {
	h := fmt.Errorf("entry: funding tree: %w: dial", ErrHeaders)
	if err := unavailable(nil, h); !errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrHeaders) {
		t.Fatalf("got %v, want both ErrUnavailable and ErrHeaders", err)
	}
	if err := unavailable(nil, nil); err != ErrUnavailable {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	ref := fmt.Errorf("%w: writer", ErrRefused)
	if err := unavailable(ref, h); err != ref {
		t.Fatalf("got %v, want the refusal", err)
	}
}
