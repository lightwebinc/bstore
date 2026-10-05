package reader

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/lookup"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Ask asks one host one free question of ls_bstore and returns its
// output-list, unverified: the caller verifies every output it uses.
func (r *Reader) Ask(ctx context.Context, host string, q map[string]string) (*lookup.Answer, error) {
	return r.ask(ctx, host, q)
}

// Tx is the token's transaction, with its proof.
func (t *Token) Tx() *transaction.Transaction { return t.tx }

// Blobs are the closure's blob references in content order.
func (o *Object) Blobs() []chirp.Child { return append([]chirp.Child(nil), o.blobs...) }

// ErrNoChain is a chain no host answers a token of that verifies.
var ErrNoChain = errors.New("reader: no host answers a token of the chain that verifies")

// Tokens rebuilds a chain from the hosts, as the owner's anchoring tooling
// must (spec section 3.4): the highest token any host answers to `bucket`
// that verifies against the reader's headers, walked back by prev through
// `bucket-seq` to seq 0, each token verified, every link checked and the
// first token's id held to the outpoint it spends (section 15.1 and row
// C15). Nothing a reader remembers is used: the held token is neither read
// nor moved. The tokens come back in seq order from 0.
//
// When several hosts answer different latest tokens, the highest seq that
// verifies is taken; with a spend source, a candidate whose output is spent
// is passed over (a later token spent it), and when every candidate at the
// highest seq is spent, or the source cannot say, the answer is an error:
// the tooling mints only on a token it holds evidence is the latest.
func (r *Reader) Tokens(ctx context.Context, c Chain) ([]*Token, error) {
	var cands []*Token
	var hostOf []string
	for _, h := range r.Hosts {
		a, err := r.ask(ctx, h, c.query())
		if err != nil {
			r.logf("%v", &hostError{h, fmt.Errorf("bucket: %w", err)})
			continue
		}
		for _, o := range a.Outputs {
			t, err := r.verifyToken(ctx, c, o)
			if err != nil {
				r.logf("%v", &hostError{h, err})
				continue
			}
			cands = append(cands, t)
			hostOf = append(hostOf, h)
		}
	}
	if len(cands) == 0 {
		return nil, ErrNoChain
	}
	// Highest seq first, then lowest txid, each once.
	order := make([]int, len(cands))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(x, y int) bool {
		a, b := cands[order[x]], cands[order[y]]
		if a.Bucket.Seq != b.Bucket.Seq {
			return a.Bucket.Seq > b.Bucket.Seq
		}
		return displayLess(a.Txid, b.Txid)
	})
	var lastErr error
	seen := map[[32]byte]bool{}
	for _, i := range order {
		t := cands[i]
		if seen[t.Txid] {
			continue
		}
		seen[t.Txid] = true
		if r.Spender != nil {
			by, err := r.Spender(ctx, t.TxidHex(), t.Vout)
			if err != nil {
				lastErr = fmt.Errorf("%w: the spend source cannot say whether the token at seq %d is spent: %v", storerec.ErrBucket, t.Bucket.Seq, err)
				continue
			}
			if by != "" {
				lastErr = fmt.Errorf("%w: the token at seq %d that %s answered is spent by %s: a later token exists, or the bucket is closed",
					storerec.ErrBucket, t.Bucket.Seq, hostOf[i], short(by))
				continue
			}
		}
		walk := []storerec.Link{{Txid: t.Txid, Bucket: t.Bucket}}
		toks := []*Token{t}
		cur := t
		var err error
		for cur.Bucket.Seq > 0 {
			var prev *Token
			if prev, err = r.tokenAt(ctx, hostOf[i], c, cur.Bucket.Seq-1, cur.Bucket.Prev); err != nil {
				break
			}
			walk = append(walk, storerec.Link{Txid: prev.Txid, Bucket: prev.Bucket})
			toks = append(toks, prev)
			cur = prev
		}
		if err == nil {
			walk[len(walk)-1].Spends = spends(cur)
			err = storerec.CheckWalk(nil, walk)
		}
		if err != nil {
			lastErr = err
			continue
		}
		for a, b := 0, len(toks)-1; a < b; a, b = a+1, b-1 {
			toks[a], toks[b] = toks[b], toks[a]
		}
		return toks, nil
	}
	return nil, lastErr
}

// displayLess orders two txids (hash byte order) by display order.
func displayLess(a, b [32]byte) bool {
	for i := 31; i >= 0; i-- {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// TokenTxid parses a display-order txid.
func TokenTxid(s string) ([32]byte, error) {
	h, err := chainhash.NewHashFromHex(s)
	if err != nil {
		return [32]byte{}, err
	}
	return [32]byte(*h), nil
}
