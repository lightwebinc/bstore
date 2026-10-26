package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/nodeapi"
)

// linker parses stored transactions with their inputs' sources attached,
// which the extended format needs.
type linker struct {
	st      *txStore
	funding *transaction.Transaction
	mu      sync.Mutex
	cache   map[string]*transaction.Transaction
}

func (l *linker) parent(id string) (*transaction.Transaction, error) {
	if id == l.funding.TxID().String() {
		return l.funding, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.cache[id]; ok {
		return t, nil
	}
	t, err := l.st.tx(id)
	if err != nil {
		return nil, err
	}
	if len(l.cache) > 4096 {
		l.cache = map[string]*transaction.Transaction{}
	}
	l.cache[id] = t
	return t, nil
}

func (l *linker) linked(id string) (*transaction.Transaction, error) {
	tx, err := l.st.tx(id)
	if err != nil {
		return nil, err
	}
	for _, in := range tx.Inputs {
		p, err := l.parent(in.SourceTXID.String())
		if err != nil {
			return nil, fmt.Errorf("%s input: %w", id, err)
		}
		in.SourceTransaction = p
	}
	return tx, nil
}

func cmdSend(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	prop := fs.String("propagation", "http://10.10.11.120:20833", "Teranode propagation base URL (POST /txs, extended format)")
	batch := fs.Int("batch", 500, "transactions per POST")
	rate := fs.Int("rate", 2000, "transactions a second at most")
	from := fs.Int("from-level", 0, "start at this level (a resumed send)")
	_ = fs.Parse(args)
	var p planFile
	if err := readJSON(filepath.Join(*root, "chain", "plan.json"), &p); err != nil {
		return err
	}
	st, err := openStore(*root)
	if err != nil {
		return err
	}
	_, ftx, err := loadFunding(*root)
	if err != nil {
		return err
	}
	l := &linker{st: st, funding: ftx, cache: map[string]*transaction.Transaction{}}
	client := &http.Client{Timeout: 120 * time.Second}
	url := strings.TrimRight(*prop, "/") + "/txs"
	var sent int
	start := time.Now()
	for lv := *from; lv < len(p.Levels); lv++ {
		ids := p.Levels[lv]
		for i := 0; i < len(ids); i += *batch {
			chunk := ids[i:min(i+*batch, len(ids))]
			var body bytes.Buffer
			for _, id := range chunk {
				if st.proven(id) {
					continue
				}
				tx, err := l.linked(id)
				if err != nil {
					return err
				}
				ef, err := tx.EF()
				if err != nil {
					return fmt.Errorf("%s: %w", id, err)
				}
				body.Write(ef)
			}
			if body.Len() == 0 {
				continue
			}
			for attempt := 0; ; attempt++ {
				status, msg, err := post(ctx, client, url, body.Bytes())
				if err == nil && status == http.StatusOK {
					break
				}
				if attempt >= 8 || ctx.Err() != nil {
					return fmt.Errorf("level %d batch at %d: status %d %s %v", lv, i, status, msg, err)
				}
				time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			}
			sent += len(chunk)
			if want := time.Duration(float64(sent) / float64(*rate) * float64(time.Second)); time.Since(start) < want {
				time.Sleep(want - time.Since(start))
			}
		}
		if len(ids) > 0 {
			fmt.Printf("level %d: %d sent (%d total, %s)\n", lv, len(ids), sent, time.Since(start).Round(time.Second))
		}
	}
	return nil
}

func post(ctx context.Context, c *http.Client, url string, body []byte) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	res, err := c.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	return res.StatusCode, strings.TrimSpace(string(b)), nil
}

func cmdProve(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("prove", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	asset := fs.String("asset", "http://10.10.11.120:20090", "Teranode asset API")
	par := fs.Int("par", 48, "concurrent proof requests")
	wait := fs.Duration("wait", 30*time.Minute, "give up after this long without progress")
	_ = fs.Parse(args)
	var p planFile
	if err := readJSON(filepath.Join(*root, "chain", "plan.json"), &p); err != nil {
		return err
	}
	st, err := openStore(*root)
	if err != nil {
		return err
	}
	a := &nodeapi.Asset{Base: *asset, Client: &http.Client{Timeout: 30 * time.Second}}
	var todo []string
	for _, lv := range p.Levels {
		for _, id := range lv {
			if !st.proven(id) {
				todo = append(todo, id)
			}
		}
	}
	fmt.Printf("prove: %d to prove\n", len(todo))
	last := time.Now()
	for len(todo) > 0 {
		var mu sync.Mutex
		got := map[string]*transaction.MerklePath{}
		var left []string
		var bad atomic.Int64
		jobs := make(chan string)
		var wg sync.WaitGroup
		for range *par {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for id := range jobs {
					mp, err := a.MerkleProof(ctx, id)
					if err == nil {
						h, _ := chainhash.NewHashFromHex(id)
						mp, err = minimalPath(mp, h)
						if err == nil && !chaintoken.MinimalPath(mp, *h) {
							err = errors.New("not minimal")
						}
					}
					mu.Lock()
					if err != nil {
						if !errors.Is(err, nodeapi.ErrNotMined) {
							bad.Add(1)
						}
						left = append(left, id)
					} else {
						got[id] = mp
					}
					mu.Unlock()
				}
			}()
		}
		for _, id := range todo {
			jobs <- id
		}
		close(jobs)
		wg.Wait()
		if len(got) > 0 {
			if err := st.addProofs(got); err != nil {
				return err
			}
			last = time.Now()
		}
		fmt.Printf("prove: %d proven this pass, %d left (%d errors other than not mined)\n", len(got), len(left), bad.Load())
		todo = left
		if len(todo) == 0 {
			break
		}
		if time.Since(last) > *wait || ctx.Err() != nil {
			return fmt.Errorf("prove: %d unproven after %s without progress", len(todo), *wait)
		}
		time.Sleep(10 * time.Second)
	}
	return nil
}

// minimalPath cuts a proof to the path of txid alone (bstore's provider
// does the same before it publishes a mined advertisement).
func minimalPath(mp *transaction.MerklePath, txid *chainhash.Hash) (*transaction.MerklePath, error) {
	if mp == nil || len(mp.Path) == 0 {
		return nil, errors.New("no proof")
	}
	var at *transaction.PathElement
	for _, e := range mp.Path[0] {
		if e.Hash != nil && *e.Hash == *txid {
			at = e
		}
	}
	if at == nil {
		return nil, fmt.Errorf("the proof does not hold %s", txid)
	}
	index := make([]map[uint64]*transaction.PathElement, len(mp.Path))
	for l, leaves := range mp.Path {
		index[l] = map[uint64]*transaction.PathElement{}
		for _, e := range leaves {
			index[l][e.Offset] = e
		}
	}
	isTxid := true
	path := make([][]*transaction.PathElement, len(mp.Path))
	path[0] = []*transaction.PathElement{{Offset: at.Offset, Hash: txid, Txid: &isTxid}}
	for l := range mp.Path {
		off := at.Offset>>uint(l) ^ 1
		e := index[l][off]
		if e == nil {
			return mp, nil
		}
		leaf := &transaction.PathElement{Offset: off}
		if e.Duplicate != nil && *e.Duplicate {
			dup := true
			leaf.Duplicate = &dup
		} else if e.Hash != nil {
			h := *e.Hash
			leaf.Hash = &h
		}
		path[l] = append(path[l], leaf)
	}
	slices.SortFunc(path[0], func(a, b *transaction.PathElement) int { return int(a.Offset) - int(b.Offset) })
	out := transaction.NewMerklePath(mp.BlockHeight, path)
	want, err := mp.ComputeRoot(txid)
	if err != nil {
		return nil, err
	}
	got, err := out.ComputeRoot(txid)
	if err != nil || *got != *want {
		return mp, nil
	}
	return out, nil
}

// ---- prove-local: the paths computed from each block's subtrees.

type blockJSON struct {
	Header struct {
		MerkleRoot string `json:"hash_merkle_root"`
	} `json:"header"`
	Coinbase struct {
		TxID string `json:"txid"`
	} `json:"coinbase_tx"`
	Count    int      `json:"transaction_count"`
	Subtrees []string `json:"subtrees"`
	Height   uint32   `json:"height"`
}

func getBytes(ctx context.Context, c *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %d %s", url, res.StatusCode, b[:min(len(b), 200)])
	}
	return b, nil
}

// blockPaths returns the minimal path of every wanted txid in the block at
// height, computed from its subtrees and held to its header's root.
func blockPaths(ctx context.Context, a *nodeapi.Asset, c *http.Client, height uint32, want map[chainhash.Hash]bool) (map[string]*transaction.MerklePath, int, error) {
	hash, err := a.HashAtHeight(ctx, height)
	if err != nil {
		return nil, 0, err
	}
	raw, err := getBytes(ctx, c, a.Base+"/api/v1/block/"+hash+"/json")
	if err != nil {
		return nil, 0, err
	}
	var b blockJSON
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, 0, err
	}
	var leaves []chainhash.Hash
	for _, st := range b.Subtrees {
		hs, err := getBytes(ctx, c, a.Base+"/api/v1/subtree/"+st)
		if err != nil {
			return nil, 0, err
		}
		if len(hs)%32 != 0 {
			return nil, 0, fmt.Errorf("subtree %s: %d bytes", st, len(hs))
		}
		for i := 0; i < len(hs); i += 32 {
			leaves = append(leaves, chainhash.Hash(hs[i:i+32]))
		}
	}
	cb, err := chainhash.NewHashFromHex(b.Coinbase.TxID)
	if err != nil {
		return nil, 0, err
	}
	if len(leaves) == 0 {
		return nil, 0, errors.New("no leaves")
	}
	leaves[0] = *cb
	if b.Count > 0 && len(leaves) > b.Count {
		leaves = leaves[:b.Count]
	}
	levels := [][]chainhash.Hash{leaves}
	for cur := leaves; len(cur) > 1; {
		next := make([]chainhash.Hash, (len(cur)+1)/2)
		for i := range next {
			l := cur[2*i]
			r := l
			if 2*i+1 < len(cur) {
				r = cur[2*i+1]
			}
			next[i] = chainhash.DoubleHashH(append(append(make([]byte, 0, 64), l[:]...), r[:]...))
		}
		levels = append(levels, next)
		cur = next
	}
	root, err := chainhash.NewHashFromHex(b.Header.MerkleRoot)
	if err != nil {
		return nil, 0, err
	}
	if levels[len(levels)-1][0] != *root {
		return nil, 0, fmt.Errorf("block %d: the subtrees' root %s is not the header's %s", height, levels[len(levels)-1][0], root)
	}
	out := map[string]*transaction.MerklePath{}
	isTxid := true
	for idx, h := range leaves {
		if !want[h] {
			continue
		}
		path := make([][]*transaction.PathElement, len(levels)-1)
		self := h
		path[0] = []*transaction.PathElement{{Offset: uint64(idx), Hash: &self, Txid: &isTxid}}
		for l := 0; l < len(levels)-1; l++ {
			off := (idx >> l) ^ 1
			e := &transaction.PathElement{Offset: uint64(off)}
			if off < len(levels[l]) {
				x := levels[l][off]
				e.Hash = &x
			} else {
				dup := true
				e.Duplicate = &dup
			}
			path[l] = append(path[l], e)
		}
		slices.SortFunc(path[0], func(a, b *transaction.PathElement) int { return int(a.Offset) - int(b.Offset) })
		mp := transaction.NewMerklePath(height, path)
		got, err := mp.ComputeRoot(&self)
		if err != nil || *got != *root {
			return nil, 0, fmt.Errorf("path of %s does not compute the root", self)
		}
		if !chaintoken.MinimalPath(mp, self) {
			return nil, 0, fmt.Errorf("path of %s is not minimal", self)
		}
		out[self.String()] = mp
	}
	return out, len(leaves), nil
}

func cmdProveLocal(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("prove-local", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	asset := fs.String("asset", "http://10.10.11.120:20090", "Teranode asset API")
	from := fs.Uint("from", 0, "first block height to read (default: the funding's)")
	to := fs.Uint("to", 0, "last block height (default: the tip)")
	_ = fs.Parse(args)
	var p planFile
	if err := readJSON(filepath.Join(*root, "chain", "plan.json"), &p); err != nil {
		return err
	}
	st, err := openStore(*root)
	if err != nil {
		return err
	}
	fnd, _, err := loadFunding(*root)
	if err != nil {
		return err
	}
	want := map[chainhash.Hash]bool{}
	for _, lv := range p.Levels {
		for _, id := range lv {
			if !st.proven(id) {
				want[*must(chainhash.NewHashFromHex(id))] = true
			}
		}
	}
	c := &http.Client{Timeout: 120 * time.Second}
	a := &nodeapi.Asset{Base: *asset, Client: c}
	lo := uint32(*from)
	if lo == 0 {
		lo = fnd.Height + 1
	}
	hi := uint32(*to)
	if hi == 0 {
		tip, err := a.BestHeader(ctx)
		if err != nil {
			return err
		}
		hi = tip.Height
	}
	fmt.Printf("prove-local: %d unproven, blocks %d..%d\n", len(want), lo, hi)
	for h := lo; h <= hi && len(want) > 0; h++ {
		got, n, err := blockPaths(ctx, a, c, h, want)
		if err != nil {
			return err
		}
		if err := st.addProofs(got); err != nil {
			return err
		}
		for id := range got {
			delete(want, *must(chainhash.NewHashFromHex(id)))
		}
		fmt.Printf("block %d: %d transactions, %d of ours proven, %d left\n", h, n, len(got), len(want))
	}
	if len(want) > 0 {
		return fmt.Errorf("%d not found in blocks %d..%d", len(want), lo, hi)
	}
	return nil
}
