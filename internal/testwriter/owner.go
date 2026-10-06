package testwriter

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/mint"

	"github.com/lightwebinc/bstore/storerec"
)

// Chain is a bucket chain this writer owns, for tests only: the owner's
// anchoring tooling at its least (spec sections 3.4 and 3.6). It mints the
// first token from a chain tree and each later token from the entries a
// host answers by `unanchored`, resolved as spec section 3.4 has it.
type Chain struct {
	W       *Writer
	Bucket  string
	ID      [32]byte
	Writers [][]byte
	Map     storerec.Map
	Named   map[[32]byte]bool
	Token   *transaction.Transaction
	Record  *storerec.Bucket
}

// NewChain mints a chain's first token for bucket naming writers, and
// returns the chain and the token's BEEF (with its parent, the chain
// tree).
func (w *Writer) NewChain(ctx context.Context, bucket string, writers [][]byte) (*Chain, []byte, error) {
	tree, err := w.Tree(ctx, storerec.ChainDerivation, 1)
	if err != nil {
		return nil, nil, err
	}
	ws := append([][]byte(nil), writers...)
	sort.Slice(ws, func(i, j int) bool { return bytes.Compare(ws[i], ws[j]) < 0 })
	b := &storerec.Bucket{Bucket: bucket, Owner: w.Identity(), ID: storerec.BucketID([32]byte(*tree.TxID()), 0), Seq: 0,
		Writers: ws, Changes: []storerec.Change{}, Count: 0, MapRoot: storerec.EmptyHash,
		Created: uint64(time.Now().UnixMilli()), Consumed: [][32]byte{}}
	c := &Chain{W: w, Bucket: bucket, ID: b.ID, Writers: ws, Map: storerec.Map{}, Named: map[[32]byte]bool{}}
	beef, err := c.mint(ctx, b, tree, 0)
	if err != nil {
		return nil, nil, err
	}
	return c, beef, nil
}

func (c *Chain) mint(ctx context.Context, b *storerec.Bucket, prev *transaction.Transaction, vout uint32) ([]byte, error) {
	w := c.W
	rec, err := b.Encode()
	if err != nil {
		return nil, err
	}
	lock, err := storerec.BucketDerivation.Lock(ctx, w.W, w.W.Originator, [][]byte{storerec.TagToken, rec}, true)
	if err != nil {
		return nil, err
	}
	unlock := storerec.ChainDerivation.Unlocker(ctx, w.W, w.W.Originator)
	if b.Seq > 0 {
		unlock = storerec.BucketDerivation.Unlocker(ctx, w.W, w.W.Originator)
	}
	pay, err := w.payer(ctx)
	if err != nil {
		return nil, err
	}
	fee, err := pay.TakeAtLeast(ctx, 1+w.Fees.Floor*4)
	if err != nil {
		return nil, err
	}
	change, _ := w.Signer.FundScript()
	tx, err := mint.Transition(lock, 1, &mint.Input{Tx: prev, Vout: vout, Unlocker: unlock}, fee, change, w.Fees)
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	if tx, err = w.mine(ctx, tx, pay); err != nil {
		return nil, err
	}
	c.Token, c.Record = tx, b
	return storerec.TokenBEEF(tx, prev)
}

// Anchor mints the next token applying batch (spec section 3.4) and returns
// its BEEF.
func (c *Chain) Anchor(ctx context.Context, batch []storerec.Applicable) ([]byte, error) {
	changes, consumed := storerec.ResolveBatch(c.Map, c.Named, batch)
	next := c.Map.Clone()
	for _, ch := range changes {
		if ch.Removal {
			delete(next, ch.Key)
		} else {
			next[ch.Key] = ch.C
		}
		c.Named[ch.C] = true
	}
	for _, x := range consumed {
		c.Named[x] = true
	}
	if changes == nil {
		changes = []storerec.Change{}
	}
	if consumed == nil {
		consumed = [][32]byte{}
	}
	b := &storerec.Bucket{Bucket: c.Bucket, Owner: c.W.Identity(), ID: c.ID, Seq: c.Record.Seq + 1, Prev: [32]byte(*c.Token.TxID()),
		Writers: c.Writers, Changes: changes, Count: uint64(len(next)), MapRoot: next.Root(),
		Created: uint64(time.Now().UnixMilli()), Consumed: consumed}
	beef, err := c.mint(ctx, b, c.Token, 0)
	if err != nil {
		return nil, err
	}
	c.Map = next
	return beef, nil
}

// Unanchored asks host for writer's entries no token names (spec section
// 11.1, `unanchored`), every page, held to the carrier rules.
func (c *Chain) Unanchored(ctx context.Context, host string, writer []byte) ([]storerec.Applicable, error) {
	var out []storerec.Applicable
	after := ""
	for page := 0; page < 1000; page++ {
		q := map[string]any{"service": storerec.LookupService, "query": map[string]string{"bucket": c.Bucket,
			"owner": hex.EncodeToString(c.W.Identity()), "id": hex.EncodeToString(c.ID[:]), "writer": hex.EncodeToString(writer), "after": after}}
		body, _ := json.Marshal(q)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(host, "/")+"/lookup", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := c.W.HTTP.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<20))
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unanchored: %d %s", res.StatusCode, raw)
		}
		var ans struct {
			Outputs []struct {
				Beef []int `json:"beef"`
			} `json:"outputs"`
		}
		if err := json.Unmarshal(raw, &ans); err != nil {
			return nil, err
		}
		for _, o := range ans.Outputs {
			b := make([]byte, len(o.Beef))
			for i, v := range o.Beef {
				b[i] = byte(v)
			}
			a, err := storerec.Admit(ctx, b, nil, storerec.Host{Bucket: c.Bucket, Headers: c.W.Headers})
			if err != nil || a.Carrier == nil || a.Carrier.Entry == nil {
				continue
			}
			e := a.Carrier.Entry
			if e.Bucket != c.Bucket || e.ID != c.ID || !bytes.Equal(e.Writer, writer) {
				continue
			}
			out = append(out, storerec.Applicable{C: a.Carrier.Commitment, Key: e.Key, Kind: e.Kind, Created: e.Created})
			after = displayHex(a.Carrier.Commitment)
		}
		if len(ans.Outputs) < 64 {
			break
		}
	}
	return out, nil
}

func displayHex(h [32]byte) string {
	var r [32]byte
	for i := range h {
		r[i] = h[31-i]
	}
	return hex.EncodeToString(r[:])
}

// Entries mints a put entry by the owner for each key, all naming the
// closure b, on one entry tree, and returns their BEEFs.
func (c *Chain) Entries(ctx context.Context, keys []string, b *storerec.Built) ([][]byte, error) {
	tree, err := c.W.Tree(ctx, storerec.EntryDerivation, len(keys))
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for i, k := range keys {
		e := &storerec.Entry{Bucket: c.Bucket, ID: c.ID, Writer: c.W.Identity(), Key: k, Kind: storerec.KindPut,
			Created: uint64(time.Now().UnixMilli()), Size: b.Size, Root: b.RootHash, ContentHash: b.ContentHash,
			ProofRoot: b.ProofRoot, ProofProfile: 1, ContentType: "application/octet-stream"}
		rec, err := e.Encode()
		if err != nil {
			return nil, err
		}
		beef, _, err := c.W.Carrier(ctx, storerec.EntryDerivation, rec, tree, uint32(i))
		if err != nil {
			return nil, err
		}
		out = append(out, beef)
	}
	return out, nil
}
