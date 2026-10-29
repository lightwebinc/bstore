package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	sdkspv "github.com/bsv-blockchain/go-sdk/spv"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/storerec"
)

// The shapes a set may hold.
var shapeDoc = map[string]string{
	"put4k":   "the writer's put of a 4 KiB object (a log line batch): no hold, application/json, a fresh closure each",
	"put1m":   "the writer's put of a 1 MiB object with hold terms (t 3, K 3), an ETag and meta, image/jpeg",
	"put256m": "the writer's put of a 256 MiB object with hold terms, video/mp4",
	"delete":  "the writer's delete entry",
	"outcome": "the buyer's outcome record of a passed challenge (C 30) on a 4 KiB object, the provider's signature checked at admission",
	"front":   "the buyer's front statement for a provider",
	"advert":  "a provider's mined advertisement of a 1 MiB root (alone, its own minimal path)",
	"hold":    "the writer's mined hold output (alone, its own minimal path)",
	"mix":     "a put cycle in order: entry, its hold output, one advertisement per provider; the owner's anchor token per 100 puts of a bucket",
}

// manifest is txmint's corpus manifest, which `txmint replay` reads.
type manifest struct {
	Name     string                  `json:"name"`
	Built    string                  `json:"built"`
	Builder  string                  `json:"builder"`
	Funding  string                  `json:"funding_txid"`
	Height   uint32                  `json:"funding_height"`
	Encoding string                  `json:"encoding"`
	Params   map[string]any          `json:"params"`
	Shapes   map[string]shapeSummary `json:"shapes"`
}

type shapeSummary struct {
	File        string   `json:"file"`
	Count       int      `json:"count"`
	Topics      []string `json:"topics"`
	FirstSerial uint64   `json:"first_serial"`
	LastSerial  uint64   `json:"last_serial"`
	ObjectBytes int      `json:"object_bytes_mean"`
	Work        work     `json:"work"`
	Doc         string   `json:"doc"`
}

type work struct{ Txs, Inputs, Scripts, Proofs, Depth int }

// record is a BRC-149 submission record (txmint beef.RecordTopics).
func record(topic string, obj []byte) []byte {
	out := make([]byte, 0, 8+1+len(topic)+len(obj))
	out = binary.BigEndian.AppendUint16(out, 0xBEEF)
	out = append(out, 0x01, 1, byte(len(topic)))
	out = append(out, topic...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(obj)))
	return append(out, obj...)
}

type builder struct {
	ctx    context.Context
	root   string
	id     *ids
	p      *planFile
	st     *txStore
	entry  []*transaction.Transaction
	outT   []*transaction.Transaction
	front  []*transaction.Transaction
	bid    [][32]byte
	topic  []string
	now    uint64
	height uint64 // the funding's block height
}

func (b *builder) trees(ids []string) ([]*transaction.Transaction, error) {
	var out []*transaction.Transaction
	for _, id := range ids {
		t, err := b.st.tx(id)
		if err != nil {
			return nil, err
		}
		if t.MerklePath == nil {
			return nil, fmt.Errorf("tree %s is not proven yet (run prove)", id)
		}
		out = append(out, t)
	}
	return out, nil
}

func challengePriv(p *ident) *ec.PrivateKey {
	return must(wallet.NewKeyDeriver(p.Priv).DerivePrivateKey(storerec.Protocol, storerec.KeyChallenge, pushdrop.Anyone()))
}

func cmdBuild(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	name := fs.String("name", "", "set name (under <root>/sets)")
	shapes := fs.String("shapes", "put4k,put1m,put256m,delete,outcome,front,advert,hold,mix", "shapes")
	count := fs.Int("count", 90000, "objects per offline shape; advert, hold and mix hold what the plan minted")
	_ = fs.Parse(args)
	if *root == "" || *name == "" {
		return errors.New("build: -root and -name")
	}
	b := &builder{ctx: ctx, root: *root, now: uint64(time.Now().UnixMilli())}
	var err error
	if b.id, err = loadIDs(*root); err != nil {
		return err
	}
	b.p = &planFile{}
	if err := readJSON(filepath.Join(*root, "chain", "plan.json"), b.p); err != nil {
		return err
	}
	if b.st, err = openStore(*root); err != nil {
		return err
	}
	if b.entry, err = b.trees(b.p.Trees.Entry); err != nil {
		return err
	}
	if b.outT, err = b.trees(b.p.Trees.Outcome); err != nil {
		return err
	}
	if b.front, err = b.trees(b.p.Trees.Front); err != nil {
		return err
	}
	for _, pb := range b.p.Buckets {
		b.bid = append(b.bid, h32(pb.ID))
		b.topic = append(b.topic, pb.Topic)
	}
	fnd, _, err := loadFunding(*root)
	if err != nil {
		return err
	}
	b.height = uint64(fnd.Height)
	dir := filepath.Join(*root, "sets", *name)
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return fmt.Errorf("build: %w (a set is built once)", err)
	}
	m := &manifest{Name: *name, Built: time.Now().UTC().Format(time.RFC3339), Builder: "bstore perfcorpus",
		Funding: fnd.TxID, Height: fnd.Height, Encoding: "mixed",
		Params: map[string]any{"buckets": b.id.Buckets, "plan": b.p.Params}, Shapes: map[string]shapeSummary{}}
	for _, s := range strings.Split(*shapes, ",") {
		s = strings.TrimSpace(s)
		if _, ok := shapeDoc[s]; !ok {
			return fmt.Errorf("unknown shape %q", s)
		}
		start := time.Now()
		sum, err := b.shape(dir, s, *count)
		if err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		sum.Doc = shapeDoc[s]
		m.Shapes[s] = *sum
		fmt.Printf("%-8s %7d objects, %6d B mean, serials %d..%d, %s\n", s, sum.Count, sum.ObjectBytes, sum.FirstSerial, sum.LastSerial, time.Since(start).Round(time.Second))
		if err := writeJSON(filepath.Join(dir, "manifest.json"), m); err != nil {
			return err
		}
	}
	fmt.Printf("set %s\n", dir)
	return nil
}

// shape writes one shape file of n objects (or what the plan minted).
func (b *builder) shape(dir, s string, n int) (*shapeSummary, error) {
	var gen func(i int, serial uint64) (topic string, obj []byte, err error)
	var w work
	switch s {
	case "advert", "hold":
		list := b.p.Adverts
		if s == "hold" {
			list = b.p.Holds
		}
		n = len(list)
		w = work{1, 1, 0, 1, 0}
		gen = func(i int, _ uint64) (string, []byte, error) {
			obj, err := b.alone(list[i])
			return b.topic[i%len(b.topic)], obj, err
		}
	case "mix":
		n = len(b.p.Mix)
		w = work{2, 2, 1, 1, 1}
		gen = func(i int, _ uint64) (string, []byte, error) {
			it := b.p.Mix[i]
			var obj []byte
			var err error
			switch it.Kind {
			case "entry":
				obj, err = b.storedCarrier(it.Txid)
			case "token":
				obj, err = b.tokenBEEF(it.Bucket, it.Txid)
			default:
				obj, err = b.alone(it.Txid)
			}
			return b.topic[it.Bucket], obj, err
		}
	default:
		var err error
		if gen, w, err = b.offline(s); err != nil {
			return nil, err
		}
	}
	first, err := serials(b.root, n)
	if err != nil {
		return nil, err
	}
	// Generate in parallel, write in order.
	type res struct {
		rec []byte
		err error
	}
	out := make([]res, n)
	var wg sync.WaitGroup
	workers := runtime.NumCPU()
	for k := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := k; i < n; i += workers {
				topic, obj, err := gen(i, first+uint64(i))
				if err == nil {
					out[i] = res{rec: record(topic, obj)}
				} else {
					out[i] = res{err: err}
				}
			}
		}()
	}
	wg.Wait()
	f, err := os.OpenFile(filepath.Join(dir, s+".bin"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var bytes int
	topics := map[string]bool{}
	for i, r := range out {
		if r.err != nil {
			f.Close()
			return nil, fmt.Errorf("object %d: %w", i, r.err)
		}
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(len(r.rec)))
		bw.Write(hdr[:])
		bw.Write(r.rec)
		bytes += len(r.rec)
		topics[string(r.rec[5:5+int(r.rec[4])])] = true
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	sum := &shapeSummary{File: s + ".bin", Count: n, FirstSerial: first, LastSerial: first + uint64(n) - 1, Work: w}
	for t := range topics {
		sum.Topics = append(sum.Topics, t)
	}
	if n > 0 {
		sum.ObjectBytes = bytes / n
	}
	return sum, nil
}

// alone is a mined transaction as a host admits it: its own minimal path.
func (b *builder) alone(id string) ([]byte, error) {
	tx, err := b.st.tx(id)
	if err != nil {
		return nil, err
	}
	if tx.MerklePath == nil {
		return nil, fmt.Errorf("%s is not proven (run prove)", id)
	}
	return tx.BEEF()
}

// storedCarrier is a planned carrier with its funding tree, as Atomic BEEF.
func (b *builder) storedCarrier(id string) ([]byte, error) {
	tx, err := b.st.tx(id)
	if err != nil {
		return nil, err
	}
	tree, err := b.st.tx(tx.Inputs[0].SourceTXID.String())
	if err != nil {
		return nil, err
	}
	if tree.MerklePath == nil {
		return nil, fmt.Errorf("tree of %s is not proven", id)
	}
	tx.Inputs[0].SourceTransaction = tree
	return tx.AtomicBEEF(false)
}

// tokenBEEF is a token and its parent (the previous token, or the chain
// tree at seq 0), as a host admits a token.
func (b *builder) tokenBEEF(_ int, id string) ([]byte, error) {
	tx, err := b.st.tx(id)
	if err != nil {
		return nil, err
	}
	tok, err := storerec.ReadToken(tx)
	if err != nil {
		return nil, err
	}
	parentID := chainhash.Hash(tok.Bucket.Prev).String()
	if tok.Bucket.Seq == 0 {
		parentID = tx.Inputs[0].SourceTXID.String()
	}
	parent, err := b.st.tx(parentID)
	if err != nil {
		return nil, err
	}
	if tx.MerklePath == nil || parent.MerklePath == nil {
		return nil, fmt.Errorf("token %s or its parent is not proven", id)
	}
	return storerec.TokenBEEF(tx, parent)
}

// offline returns the generator of a carrier shape, built here and signed
// against the proven funding trees.
func (b *builder) offline(s string) (func(int, uint64) (string, []byte, error), work, error) {
	id := b.id
	nb := len(b.topic)
	cw := work{2, 2, 1, 1, 1}
	entry := func(i int, e *storerec.Entry) ([]byte, error) {
		rec, err := e.Encode()
		if err != nil {
			return nil, err
		}
		t := b.entry[i%len(b.entry)]
		vout := uint32((i / len(b.entry)) % b.p.Params.TreeSize)
		c, err := carrier.Mint(b.ctx, id.Writer.W, "", carrierParams(storerec.EntryDerivation), rec, t, vout)
		if err != nil {
			return nil, err
		}
		return c.AtomicBEEF(false)
	}
	base := func(i int, serial uint64) *storerec.Entry {
		bk := i % nb
		return &storerec.Entry{Bucket: id.Buckets[bk], ID: b.bid[bk], Writer: id.Writer.Pub(), Created: b.now + serial}
	}
	holdOf := func(i int) *storerec.Hold {
		txid := must(chainhash.NewHashFromHex(b.p.Holds[i%len(b.p.Holds)]))
		return &storerec.Hold{Txid: [32]byte(*txid), Vout: 0, Satoshis: b.p.Params.HoldSats, T: 3, K: 3, Retention: 7 * 24 * 3600,
			Deadline: b.height + 5000, CheckC: storerec.DefaultC}
	}
	switch s {
	case "put4k":
		return func(i int, serial uint64) (string, []byte, error) {
			bt, err := storerec.Build(content("4k", 4096, int(serial)))
			if err != nil {
				return "", nil, err
			}
			e := base(i, serial)
			e.Key, e.Kind = fmt.Sprintf("logs/2026/10/23/%010d.json", serial), storerec.KindPut
			e.Size, e.Root, e.ContentHash, e.ProofRoot, e.ProofProfile, e.ContentType = bt.Size, bt.RootHash, bt.ContentHash, bt.ProofRoot, 1, "application/json"
			obj, err := entry(i, e)
			return b.topic[i%nb], obj, err
		}, cw, nil
	case "put1m", "put256m":
		var refs []closureRef
		var err error
		ctype, key := "image/jpeg", "photos/2026/%010d.jpg"
		if s == "put1m" {
			refs = b.p.Closures
		} else {
			ctype, key = "video/mp4", "video/2026/%010d.mp4"
			if refs, err = closures(b.root, "256m", 256<<20, 4); err != nil {
				return nil, cw, err
			}
		}
		return func(i int, serial uint64) (string, []byte, error) {
			ref := refs[i%len(refs)]
			e := base(i, serial)
			e.Key, e.Kind = fmt.Sprintf(key, serial), storerec.KindPut
			e.Size, e.Root, e.ContentHash, e.ProofRoot, e.ProofProfile, e.ContentType = ref.Size, ref.root(), ref.contentHash(), ref.proofRoot(), 1, ctype
			ch := ref.contentHash()
			etag := hex.EncodeToString(ch[:16])
			e.ETag = &etag
			e.Meta = []storerec.MetaPair{{Key: "album", Value: "perf"}, {Key: "source", Value: "perfcorpus"}}
			e.Hold = holdOf(i)
			obj, err := entry(i, e)
			return b.topic[i%nb], obj, err
		}, cw, nil
	case "delete":
		return func(i int, serial uint64) (string, []byte, error) {
			e := base(i, serial)
			e.Key, e.Kind = fmt.Sprintf("logs/2026/10/23/%010d.json", serial), storerec.KindDelete
			obj, err := entry(i, e)
			return b.topic[i%nb], obj, err
		}, cw, nil
	case "outcome":
		type obj4k struct {
			c  []byte
			bt *storerec.Built
			t  *storerec.ProofTree
		}
		objs := make([]obj4k, 16)
		for k := range objs {
			c := content("outcome4k", 4096, k)
			bt, err := storerec.Build(c)
			if err != nil {
				return nil, cw, err
			}
			objs[k] = obj4k{c, bt, storerec.NewProofTree(c)}
		}
		privs := make([]*ec.PrivateKey, len(id.Providers))
		for k, p := range id.Providers {
			privs[k] = challengePriv(p)
		}
		seed := sha256.Sum256([]byte("perfcorpus seed"))
		return func(i int, serial uint64) (string, []byte, error) {
			o := objs[i%len(objs)]
			pv := i % len(id.Providers)
			var nonce [16]byte
			binary.BigEndian.PutUint64(nonce[8:], serial)
			q := &storerec.Request{Root: o.bt.RootHash, ProofRoot: o.bt.ProofRoot, Size: o.bt.Size, Provider: id.Providers[pv].Pub(),
				Answerer: id.Providers[pv].Pub(), SeedHeight: b.height, SeedHash: seed, Nonce: nonce, C: storerec.DefaultC}
			reqRaw, err := q.Encode()
			if err != nil {
				return "", nil, err
			}
			body, err := storerec.Answer(q, reqRaw, o.c, o.t, true).Encode()
			if err != nil {
				return "", nil, err
			}
			signed, err := storerec.SignResponse(privs[pv], reqRaw, body)
			if err != nil {
				return "", nil, err
			}
			_, sig, _ := storerec.SplitSigned(signed)
			h := sha256.Sum256(body)
			oc := &storerec.Outcome{Bucket: id.Buckets[i%nb], Buyer: id.Buyer.Pub(), Created: b.now + serial, Request: reqRaw,
				Outcome: storerec.OutcomePass, Response: &h, Signature: sig}
			rec, err := oc.Encode()
			if err != nil {
				return "", nil, err
			}
			t := b.outT[i%len(b.outT)]
			c, err := carrier.Mint(b.ctx, id.Buyer.W, "", carrierParams(storerec.OutcomeDerivation), rec, t, uint32((i/len(b.outT))%b.p.Params.TreeSize))
			if err != nil {
				return "", nil, err
			}
			ob, err := c.AtomicBEEF(false)
			return b.topic[i%nb], ob, err
		}, cw, nil
	case "front":
		return func(i int, serial uint64) (string, []byte, error) {
			pv := i % len(id.Providers)
			f := &storerec.Front{Bucket: id.Buckets[i%nb], Front: id.Buyer.Pub(), Advertiser: id.Providers[pv].Pub(),
				URL: fmt.Sprintf("https://front-%d.perf.invalid/bstore/v1", pv+1), Created: b.now + serial}
			rec, err := f.Encode()
			if err != nil {
				return "", nil, err
			}
			t := b.front[i%len(b.front)]
			c, err := carrier.Mint(b.ctx, id.Buyer.W, "", carrierParams(storerec.FrontDerivation), rec, t, uint32((i/len(b.front))%b.p.Params.TreeSize))
			if err != nil {
				return "", nil, err
			}
			ob, err := c.AtomicBEEF(false)
			return b.topic[i%nb], ob, err
		}, cw, nil
	}
	return nil, cw, fmt.Errorf("no offline builder for %s", s)
}

// ---- check: admit objects locally, as a host would.

// assetHeaders is a chain tracker over the node's asset API.
type assetHeaders struct {
	a     *nodeapi.Asset
	mu    sync.Mutex
	roots map[uint32]chainhash.Hash
}

func (h *assetHeaders) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	h.mu.Lock()
	r, ok := h.roots[height]
	h.mu.Unlock()
	if !ok {
		hash, err := h.a.HashAtHeight(ctx, height)
		if err != nil {
			return false, err
		}
		hd, err := h.a.Header(ctx, hash)
		if err != nil {
			return false, err
		}
		x, err := chainhash.NewHashFromHex(hd.MerkleRoot)
		if err != nil {
			return false, err
		}
		r = *x
		h.mu.Lock()
		h.roots[height] = r
		h.mu.Unlock()
	}
	return root != nil && *root == r, nil
}

func (h *assetHeaders) CurrentHeight(ctx context.Context) (uint32, error) {
	b, err := h.a.BestHeader(ctx)
	if err != nil {
		return 0, err
	}
	return b.Height, nil
}

func cmdCheck(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	set := fs.String("set", "", "set directory")
	asset := fs.String("asset", "", "Teranode asset API (headers); required")
	n := fs.Int("n", 200, "objects checked per shape, spread across the file")
	_ = fs.Parse(args)
	if *asset == "" {
		return errors.New("check: -asset is required")
	}
	var m manifest
	if err := readJSON(filepath.Join(*set, "manifest.json"), &m); err != nil {
		return err
	}
	hd := &assetHeaders{a: &nodeapi.Asset{Base: *asset, Client: &http.Client{Timeout: 30 * time.Second}}, roots: map[uint32]chainhash.Hash{}}
	return checkSet(ctx, *set, &m, *n, hd)
}

// checkSet admits about n objects of each shape with storerec.Admit and
// runs SPV over each, against headers.
func checkSet(ctx context.Context, set string, m *manifest, n int, hd chaintracker.ChainTracker) error {
	bad := 0
	for s, sum := range m.Shapes {
		step := max(1, sum.Count/n)
		f, err := os.Open(filepath.Join(set, sum.File))
		if err != nil {
			return err
		}
		r := bufio.NewReaderSize(f, 1<<20)
		kinds := map[string]int{}
		checked := 0
		for i := 0; ; i++ {
			var hdr [4]byte
			if _, err := io.ReadFull(r, hdr[:]); err != nil {
				break
			}
			rec := make([]byte, binary.BigEndian.Uint32(hdr[:]))
			if _, err := io.ReadFull(r, rec); err != nil {
				f.Close()
				return err
			}
			if i%step != 0 {
				continue
			}
			tl := int(rec[4])
			topic := string(rec[5 : 5+tl])
			obj := rec[5+tl+4:]
			a, err := storerec.Admit(ctx, obj, nil, storerec.Host{Bucket: strings.TrimPrefix(topic, storerec.RecordsPrefix), Headers: hd})
			if err == nil {
				err = spvCheck(ctx, obj, hd)
			}
			if err != nil {
				bad++
				fmt.Printf("%s object %d: %v\n", s, i, err)
				continue
			}
			kinds[string(a.Kind)]++
			checked++
		}
		f.Close()
		fmt.Printf("%-8s %d checked, admitted as %v\n", s, checked, kinds)
	}
	if bad > 0 {
		return fmt.Errorf("%d objects refused", bad)
	}
	return nil
}

// spvCheck is the bridge's SPV: every unproven transaction's scripts, the
// proven ones' paths against the headers.
func spvCheck(ctx context.Context, obj []byte, hd chaintracker.ChainTracker) error {
	tx, err := transaction.NewTransactionFromBEEF(obj)
	if err != nil {
		return err
	}
	ok, err := sdkspv.Verify(ctx, tx, hd, nil)
	if err != nil {
		return fmt.Errorf("spv: %w", err)
	}
	if !ok {
		return errors.New("spv: does not verify")
	}
	return nil
}

func cmdInfo(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	_ = fs.Parse(args)
	var p planFile
	if err := readJSON(filepath.Join(*root, "chain", "plan.json"), &p); err != nil {
		return err
	}
	st, err := openStore(*root)
	if err != nil {
		return err
	}
	total, proven := 0, 0
	for _, lv := range p.Levels {
		for _, id := range lv {
			total++
			if st.proven(id) {
				proven++
			}
		}
	}
	fmt.Printf("plan %s: %d to broadcast in %d levels, %d proven; trees %d/%d/%d; adverts %d, holds %d, mix %d objects\n",
		p.Created, total, len(p.Levels), proven, len(p.Trees.Entry), len(p.Trees.Outcome), len(p.Trees.Front), len(p.Adverts), len(p.Holds), len(p.Mix))
	for _, b := range p.Buckets {
		fmt.Printf("bucket %s topic %s id %s tokens %d\n", b.Name, b.Topic, b.ID, len(b.Tokens))
	}
	return nil
}
