package chaincfg

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/bwallet"
)

// Funded is what an import of a payment took into a pool.
type Funded struct {
	// Txid is the payment; Mined whether it has a block, Height which.
	Txid   string
	Mined  bool
	Height uint32
	// Paying is how many outputs pay the fund address; Held how many of
	// them the pool held already and Spent how many the chain view shows
	// spent (both left out); Added how many entered the pool, Satoshis
	// their sum.
	Paying   int
	Held     int
	Spent    int
	Added    int
	Satoshis uint64
}

// ErrUnmined is a payment imported by txid that has no block yet.
var ErrUnmined = bwallet.ErrUnmined

// FundTxid imports a mined payment to the signer's fund address by txid:
// fetched with its proof from the chain view, the proof checked against
// the headers (bwallet.ImportTxid). A payment not mined yet is ErrUnmined.
func (v *View) FundTxid(ctx context.Context, s *bwallet.Signer, pool *bwallet.Pool, txid string) (Funded, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if _, err := chainhash.NewHashFromHex(txid); err != nil || len(txid) != 64 {
		return Funded{}, fmt.Errorf("not a transaction id: %q", txid)
	}
	fund, err := s.FundScript()
	if err != nil {
		return Funded{}, err
	}
	im, err := bwallet.ImportTxid(ctx, txid, fund, v.Chain, v.Headers)
	if err != nil {
		return Funded{}, err
	}
	return v.take(ctx, pool, im)
}

// FundBEEF imports a payment to the signer's fund address handed over as
// BEEF, as a wallet answers a payment (bwallet.ImportBEEF): no lookup. A
// mined payment's proof is checked against the headers. An unmined one is
// taken when every parent it spends is proven, unless refuseUnmined; it is
// handed to the settlement leg (in case the wallet did not broadcast it),
// and its outputs wait in the pool until its proof is collected.
func (v *View) FundBEEF(ctx context.Context, s *bwallet.Signer, pool *bwallet.Pool, beef []byte, refuseUnmined bool) (Funded, error) {
	fund, err := s.FundScript()
	if err != nil {
		return Funded{}, err
	}
	im, err := bwallet.ImportBEEF(ctx, beef, fund, v.Headers, bwallet.ImportOptions{RefuseUnmined: refuseUnmined})
	if err != nil {
		return Funded{}, err
	}
	if !im.Mined && v.Settler != nil {
		if err := v.Settler.Submit(ctx, im.Tx); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
			return Funded{}, fmt.Errorf("payment %s: the settlement leg did not take it: %w", im.Txid, err)
		}
	}
	return v.take(ctx, pool, im)
}

// take adds the payment's outputs the pool does not hold. A mined
// output the chain view shows spent is left out, and one it cannot vouch
// for is an error with nothing added: a coin added on no evidence could be
// one already spent. An unmined payment's outputs are new.
func (v *View) take(ctx context.Context, pool *bwallet.Pool, im *bwallet.Import) (Funded, error) {
	res := Funded{Txid: im.Txid, Mined: im.Mined, Height: im.Height, Paying: len(im.Outputs)}
	held := map[string]bool{}
	for _, o := range pool.Outputs() {
		held[o.Outpoint()] = true
	}
	var outs []bwallet.Output
	for _, o := range im.Outputs {
		if held[o.Outpoint()] {
			res.Held++
			continue
		}
		if im.Mined {
			by, err := v.Chain.Spender(ctx, o.TxID, o.Vout)
			if err != nil {
				return Funded{}, fmt.Errorf("output %s: %w", o.Outpoint(), err)
			}
			if by != "" {
				res.Spent++
				continue
			}
		}
		outs = append(outs, o)
		res.Satoshis += o.Satoshis
	}
	n, err := pool.Add(outs...)
	res.Added = n
	return res, err
}

// maxBEEF bounds a funding payment's BEEF read from a file or standard
// input (hex is twice its bytes).
const maxBEEF = 64 << 20

// ReadBEEF reads a payment's BEEF from a file, or stdin for "-", as bytes
// or as hex.
func ReadBEEF(path string, stdin io.Reader) ([]byte, error) {
	var r io.Reader
	if path == "-" {
		r = stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	raw, err := io.ReadAll(io.LimitReader(r, 2*maxBEEF+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 2*maxBEEF {
		return nil, errors.New("the BEEF is larger than 64 MiB")
	}
	if t := bytes.TrimSpace(raw); len(t) > 0 && len(t)%2 == 0 {
		if b, err := hex.DecodeString(string(t)); err == nil {
			return b, nil
		}
	}
	if len(raw) == 0 {
		return nil, errors.New("no BEEF read")
	}
	return raw, nil
}
