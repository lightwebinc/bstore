// Package fundimport takes coin the user already owns into a wallet's pool:
// the outputs of a mined transaction, paid from the user's own wallet to the
// fund address a command printed, imported by txid. It is the funding path
// on mainnet and testnet, where nobody running a command has coinbase.
package fundimport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/nodeapi"
)

// Result is what an import took.
type Result struct {
	// TxID is the transaction imported.
	TxID string
	// Added is how many outputs entered the pool, Satoshis their sum.
	Added    int
	Satoshis uint64
	// Paying is how many outputs pay the fund address; Spent how many of
	// them the node shows spent already (left out); Held how many the pool
	// already held (left out).
	Paying int
	Spent  int
	Held   int
	// Height is the block the transaction is mined in.
	Height uint32
}

// ErrNotMined is a transaction the node has not placed in a block yet.
var ErrNotMined = errors.New("not mined yet; import it once it has a block")

// Import fetches txid and its proof from the node's asset API, checks the
// proof against the header source, and adds every output that pays the
// signer's fund address, that the node shows unspent and that the pool does
// not hold, with the transaction and its proof so a later spend can carry
// its parent. An output whose spend the node cannot vouch for is an error and
// nothing is added: a coin added on no evidence could be one already spent.
func Import(ctx context.Context, s *bwallet.Signer, pool *bwallet.Pool, asset *nodeapi.Asset, headers chaintracker.ChainTracker, txid string) (Result, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	want, err := chainhash.NewHashFromHex(txid)
	if err != nil || len(txid) != 64 {
		return Result{}, fmt.Errorf("not a transaction id: %q", txid)
	}
	raw, err := asset.TxRaw(ctx, txid)
	if err != nil {
		return Result{}, fmt.Errorf("node: %s: %w", txid, err)
	}
	tx, err := guard.ParseTransaction(raw, guard.DefaultBound)
	if err != nil {
		return Result{}, fmt.Errorf("node: %s: %w", txid, err)
	}
	if !tx.TxID().IsEqual(want) {
		return Result{}, fmt.Errorf("node: asked for %s, answered %s", txid, tx.TxID())
	}
	mp, height, err := asset.Proof(ctx, txid)
	if errors.Is(err, nodeapi.ErrNotMined) {
		return Result{}, fmt.Errorf("%s: %w", txid, ErrNotMined)
	}
	if err != nil {
		return Result{}, fmt.Errorf("node: %s: its proof: %w", txid, err)
	}
	ok, err := mp.Verify(ctx, want, headers)
	if err != nil {
		return Result{}, fmt.Errorf("%s: checking its proof against the header source: %w", txid, err)
	}
	if !ok {
		return Result{}, fmt.Errorf("%s: its proof is not in the header source at height %d", txid, height)
	}
	tx.MerklePath = mp
	fund, err := s.FundScript()
	if err != nil {
		return Result{}, err
	}
	res := Result{TxID: txid, Height: height}
	held := map[string]bool{}
	for _, o := range pool.Outputs() {
		held[o.Outpoint()] = true
	}
	var outs []bwallet.Output
	for i, o := range tx.Outputs {
		if o.LockingScript == nil || !bytes.Equal(*o.LockingScript, *fund) || o.Satoshis == 0 {
			continue
		}
		res.Paying++
		vout := uint32(i) //nolint:gosec // an output index
		out := bwallet.Output{
			TxID: txid, Vout: vout, Satoshis: o.Satoshis, LockingScript: o.LockingScript.String(),
			Height: height, Coinbase: tx.IsCoinbase(), Raw: tx.Hex(), Bump: funding.BumpHex(mp),
		}
		if held[out.Outpoint()] {
			res.Held++
			continue
		}
		by, err := asset.Spender(ctx, txid, vout)
		if err != nil {
			return Result{}, fmt.Errorf("node: %w", err)
		}
		if by != "" {
			res.Spent++
			continue
		}
		outs = append(outs, out)
		res.Satoshis += o.Satoshis
	}
	if res.Paying == 0 {
		return res, fmt.Errorf("%s pays nothing to this wallet's fund address", txid)
	}
	n, err := pool.Add(outs...)
	res.Added = n
	return res, err
}
