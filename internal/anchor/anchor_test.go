package anchor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/lookup"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/producer"
	bpub "github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

const tBucket = "example_photos_hmqzvtkwrx"

// fakeHost is an overlay host in memory: it admits what it is sent by the
// carrier and token rules, answers bucket, bucket-seq, entry and
// unanchored, and can be told to answer unanchored some other way.
type fakeHost struct {
	t       *testing.T
	headers *testchain.Chain
	mu      sync.Mutex
	tokens  map[uint64][][]byte // seq -> token BEEFs
	entries []fakeEntry
	// unanchored, when set, answers the question in place of the honest
	// answer.
	unanchored func(writer, after string, honest []fakeEntry) []lookup.Output
	srv        *httptest.Server
}

type fakeEntry struct {
	txid   string
	writer string
	id     [32]byte
	beef   []byte
}

func newFakeHost(t *testing.T, chain *testchain.Chain) *fakeHost {
	h := &fakeHost{t: t, headers: chain, tokens: map[uint64][][]byte{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/submit", h.submit)
	mux.HandleFunc("/lookup", h.lookup)
	h.srv = httptest.NewServer(mux)
	t.Cleanup(h.srv.Close)
	return h
}

func (h *fakeHost) submit(w http.ResponseWriter, r *http.Request) {
	beef, _ := io.ReadAll(r.Body)
	topic := r.Header.Get("x-topics")
	a, err := storerec.Admit(r.Context(), beef, nil, storerec.Host{Bucket: tBucket, Headers: h.headers})
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{topic: map[string]any{"outputsToAdmit": []int{}, "refused": err.Error()}})
		return
	}
	h.mu.Lock()
	switch {
	case a.Token != nil:
		h.tokens[a.Token.Bucket.Seq] = append(h.tokens[a.Token.Bucket.Seq], beef)
	case a.Carrier != nil && a.Carrier.Entry != nil:
		e := a.Carrier.Entry
		h.entries = append(h.entries, fakeEntry{txid: displayHex(a.Carrier.Commitment), writer: hex.EncodeToString(e.Writer), id: e.ID, beef: beef})
	}
	h.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{topic: map[string]any{"outputsToAdmit": a.Outputs, "coinsToRetain": []int{}}})
}

func displayHex(c [32]byte) string {
	var r [32]byte
	for i := range c {
		r[i] = c[31-i]
	}
	return hex.EncodeToString(r[:])
}

// named is every commitment the tokens held name.
func (h *fakeHost) named() map[string]bool {
	out := map[string]bool{}
	for _, bs := range h.tokens {
		for _, b := range bs {
			tx, _ := transaction.NewTransactionFromBEEF(b)
			tk, err := storerec.ReadToken(tx)
			if err != nil {
				continue
			}
			for _, c := range tk.Bucket.Changes {
				out[displayHex(c.C)] = true
			}
			for _, c := range tk.Bucket.Consumed {
				out[displayHex(c)] = true
			}
		}
	}
	return out
}

func tokenOut(b []byte) lookup.Output {
	tx, _ := transaction.NewTransactionFromBEEF(b)
	tk, _ := storerec.ReadToken(tx)
	return lookup.Output{Beef: b, OutputIndex: tk.Vout}
}

func (h *fakeHost) lookup(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Query map[string]string `json:"query"`
	}
	_ = json.NewDecoder(r.Body).Decode(&q)
	h.mu.Lock()
	defer h.mu.Unlock()
	var outs []lookup.Output
	switch {
	case q.Query["writer"] != "":
		named := h.named()
		var honest []fakeEntry
		for _, e := range h.entries {
			if e.writer == q.Query["writer"] && !named[e.txid] && e.txid > q.Query["after"] {
				honest = append(honest, e)
			}
		}
		sort.Slice(honest, func(i, j int) bool { return honest[i].txid < honest[j].txid })
		if len(honest) > 64 {
			honest = honest[:64]
		}
		if h.unanchored != nil {
			outs = h.unanchored(q.Query["writer"], q.Query["after"], honest)
			break
		}
		for _, e := range honest {
			outs = append(outs, lookup.Output{Beef: e.beef})
		}
	case hasKey(q.Query, "prefix"):
		top := uint64(0)
		for s := range h.tokens {
			top = max(top, s)
		}
		var recs []*storerec.Bucket
		for s := uint64(0); s <= top; s++ {
			tx, _ := transaction.NewTransactionFromBEEF(h.tokens[s][0])
			tk, _ := storerec.ReadToken(tx)
			recs = append(recs, tk.Bucket)
		}
		f, _ := storerec.FoldChain(recs)
		byC := map[[32]byte][]byte{}
		for _, e := range h.entries {
			byC[cOfHex(e.txid)] = e.beef
		}
		pes, leaves := f.Map.ProvePage(q.Query["prefix"], q.Query["after"], 256, func(c [32]byte) bool { return byC[c] != nil })
		tc, _ := storerec.EncodeTokenContext(leaves)
		o := tokenOut(h.tokens[top][0])
		o.Context = tc
		outs = append(outs, o)
		for _, pe := range pes {
			ec, _ := pe.Ctx.Encode()
			outs = append(outs, lookup.Output{Beef: byC[pe.C], Context: ec})
		}
	case q.Query["seq"] != "":
		s, _ := strconv.ParseUint(q.Query["seq"], 10, 64)
		for _, b := range h.tokens[s] {
			outs = append(outs, tokenOut(b))
		}
	case q.Query["entry"] != "":
		for _, e := range h.entries {
			if e.txid == q.Query["entry"] {
				outs = append(outs, lookup.Output{Beef: e.beef})
			}
		}
	default:
		top := uint64(0)
		for s := range h.tokens {
			top = max(top, s)
		}
		for _, b := range h.tokens[top] {
			outs = append(outs, tokenOut(b))
		}
	}
	if outs == nil {
		outs = []lookup.Output{}
	}
	type out struct {
		Beef        []byte `json:"beef"`
		OutputIndex uint32 `json:"outputIndex"`
		Context     []byte `json:"context,omitempty"`
	}
	ans := struct {
		Type    string `json:"type"`
		Outputs []out  `json:"outputs"`
	}{Type: lookup.TypeOutputList}
	for _, o := range outs {
		ans.Outputs = append(ans.Outputs, out{o.Beef, o.OutputIndex, o.Context})
	}
	_ = json.NewEncoder(w).Encode(ans)
}

// rig is an owner (a publisher and its tooling) and a writer on a test
// chain, over the fake host.
type rig struct {
	t      *testing.T
	ctx    context.Context
	chain  *testchain.Chain
	cs     *httptest.Server
	asset  *nodeapi.Asset
	host   *fakeHost
	tool   *Tool
	writer *testwriter.Writer
	c      *Chain
	// failSpend names a txid whose spend the node will not say.
	failSpend atomic.Value
}

func newRig(t *testing.T) *rig {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	r := &rig{t: t, ctx: ctx, chain: testchain.New(700)}
	r.failSpend.Store("")
	r.cs = httptest.NewServer(r.chain)
	t.Cleanup(r.cs.Close)
	// The node as the tooling sees it: the chain, but a spend question
	// about failSpend gets no answer.
	target, _ := url.Parse(r.cs.URL)
	rp := httputil.NewSingleHostReverseProxy(target)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if f, _ := r.failSpend.Load().(string); f != "" && strings.Contains(q.URL.Path, "/utxos/"+f) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		rp.ServeHTTP(w, q)
	}))
	t.Cleanup(node.Close)
	r.asset = &nodeapi.Asset{Base: node.URL}
	r.host = newFakeHost(t, r.chain)
	dir := t.TempDir()
	rpc := &nodeapi.RPC{URL: r.cs.URL + "/rpc", ID: "t"}
	ownerDir := filepath.Join(dir, "owner")
	if _, err := publish.Init(ownerDir); err != nil {
		t.Fatal(err)
	}
	w, err := bwallet.Open(publish.WalletDir(ownerDir), publish.Profile)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, rpc, &nodeapi.Asset{Base: r.cs.URL}, 104, 50); err != nil {
		t.Fatal(err)
	}
	store, err := publish.OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	rd := &reader.Reader{Hosts: []string{r.host.srv.URL}, Headers: r.chain, Spender: r.asset.Spender, Logf: t.Logf}
	pub, err := publish.Open(ctx, publish.Config{Dir: ownerDir, Asset: r.asset, Settler: &bpub.RPCSettler{RPC: rpc}, Fees: mint.DefaultFees,
		Store: store, Delivery: &publish.Delivery{Hosts: []string{r.host.srv.URL}}, Reader: rd, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	r.tool = &Tool{Pub: pub, Reader: rd, Store: OpenStore(ownerDir), Logf: t.Logf}
	if r.writer, err = testwriter.New(ctx, filepath.Join(dir, "writer"), tBucket, rpc, &nodeapi.Asset{Base: r.cs.URL}, r.chain, http.DefaultClient); err != nil {
		t.Fatal(err)
	}
	if r.c, err = r.tool.New(ctx, tBucket, []string{hex.EncodeToString(r.writer.Identity())}); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *rig) id() [32]byte {
	b, _ := hex.DecodeString(r.c.ID)
	return [32]byte(b)
}

// entries mints the writer's entries, one tree, each for this chain unless
// id is given, and returns their trees and txids.
type spec struct {
	key     string
	kind    uint64
	created uint64
	id      *[32]byte
}

func (r *rig) mint(specs []spec, submit bool) (*transaction.Transaction, []string) {
	tree, ids, _ := r.mintBEEF(specs, submit)
	return tree, ids
}

func (r *rig) mintBEEF(specs []spec, submit bool) (*transaction.Transaction, []string, [][]byte) {
	r.t.Helper()
	tree, err := r.writer.Tree(r.ctx, storerec.EntryDerivation, len(specs))
	if err != nil {
		r.t.Fatal(err)
	}
	b, _ := storerec.Build([]byte("x"))
	var ids []string
	var beefs [][]byte
	for i, s := range specs {
		e := &storerec.Entry{Bucket: tBucket, ID: r.id(), Writer: r.writer.Identity(), Key: s.key, Kind: s.kind, Created: s.created}
		if s.id != nil {
			e.ID = *s.id
		}
		if s.kind == storerec.KindPut {
			e.Size, e.Root, e.ContentHash, e.ProofRoot, e.ProofProfile, e.ContentType = b.Size, b.RootHash, b.ContentHash, b.ProofRoot, 1, "text/plain"
		}
		rec, err := e.Encode()
		if err != nil {
			r.t.Fatal(err)
		}
		beef, txid, err := r.writer.Carrier(r.ctx, storerec.EntryDerivation, rec, tree, uint32(i))
		if err != nil {
			r.t.Fatal(err)
		}
		if submit {
			if _, err := r.writer.Submit(r.ctx, r.host.srv.URL, storerec.RecordsTopic(tBucket), beef); err != nil {
				r.t.Fatal(err)
			}
		}
		ids = append(ids, txid)
		beefs = append(beefs, beef)
	}
	return tree, ids, beefs
}

// latest is the map of the chain's latest token at the host, folded.
func (r *rig) fold() (*storerec.Fold, []*storerec.Bucket) {
	r.t.Helper()
	toks, err := r.tool.Reader.Tokens(r.ctx, reader.Chain{Bucket: tBucket, Owner: r.tool.Pub.Identity(), ID: r.id()})
	if err != nil {
		r.t.Fatal(err)
	}
	var recs []*storerec.Bucket
	for _, t := range toks {
		recs = append(recs, t.Bucket)
	}
	f, err := storerec.FoldChain(recs)
	if err != nil {
		r.t.Fatal(err)
	}
	for i, s := range f.Statuses {
		if s != storerec.StatusConsistent {
			r.t.Fatalf("token %d: %s %s", i, s, f.Reasons[i])
		}
	}
	return f, recs
}

func cOf(t *testing.T, txid string) [32]byte {
	b, err := hex.DecodeString(txid)
	if err != nil {
		t.Fatal(err)
	}
	var c [32]byte
	for i := range c {
		c[i] = b[31-i]
	}
	return c
}

// The tooling folds a batch exactly as frozen, and a host that answers the
// same entries again after a token named them never gets one applied
// twice: the named set is rebuilt from the chain each run.
func TestAnchorFoldsOnceAndNeverTwice(t *testing.T) {
	r := newRig(t)
	_, ids := r.mint([]spec{
		{"k1", storerec.KindPut, 1, nil}, {"k1", storerec.KindPut, 2, nil}, {"gone", storerec.KindDelete, 3, nil}, {"k2", storerec.KindPut, 4, nil},
	}, true)
	rep, err := r.tool.Anchor(r.ctx, r.c, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Tokens) != 1 || rep.Applied != 2 || rep.Consumed != 2 {
		t.Fatalf("report %+v", rep)
	}
	f, recs := r.fold()
	tk := recs[len(recs)-1]
	if f.Map["k1"] != cOf(t, ids[1]) || f.Map["k2"] != cOf(t, ids[3]) || len(f.Map) != 2 {
		t.Fatal("the map after the token")
	}
	cons := map[[32]byte]bool{}
	for _, c := range tk.Consumed {
		cons[c] = true
	}
	if !cons[cOf(t, ids[0])] || !cons[cOf(t, ids[2])] {
		t.Fatal("the superseded put and the delete of an absent key are not consumed")
	}
	// A stale host answers every entry as unanchored again.
	r.host.unanchored = func(writer, after string, _ []fakeEntry) []lookup.Output {
		if after != "" {
			return nil
		}
		var out []lookup.Output
		es := append([]fakeEntry(nil), r.host.entries...)
		sort.Slice(es, func(i, j int) bool { return es[i].txid < es[j].txid })
		for _, e := range es {
			if e.writer == writer {
				out = append(out, lookup.Output{Beef: e.beef})
			}
		}
		return out
	}
	rep, err = r.tool.Anchor(r.ctx, r.c, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Tokens) != 0 || rep.Named != 4 {
		t.Fatalf("a named entry was applied again: %+v", rep)
	}
	// A later put of k1 still applies, once.
	r.host.unanchored = nil
	_, ids2 := r.mint([]spec{{"k1", storerec.KindPut, 5, nil}}, true)
	if rep, err = r.tool.Anchor(r.ctx, r.c, Options{}); err != nil || len(rep.Tokens) != 1 {
		t.Fatalf("%v %+v", err, rep)
	}
	if f, _ := r.fold(); f.Map["k1"] != cOf(t, ids2[0]) {
		t.Fatal("the later put")
	}
}

// A hostile unanchored answer: another writer's entry, an entry for
// another chain, bytes that are no transaction and a page out of order.
// The host is named, nothing of it is applied, and the honest entries
// still are.
func TestAnchorHostileUnanchored(t *testing.T) {
	r := newRig(t)
	_, good := r.mint([]spec{{"ok", storerec.KindPut, 1, nil}}, true)
	// The writer's own entry, for another chain.
	other := [32]byte{9}
	_, _, foreign := r.mintBEEF([]spec{{"foreign", storerec.KindPut, 2, &other}}, false)
	// A stranger's entry for this chain, slipped in under the writer's
	// question.
	stranger, err := testwriter.New(r.ctx, filepath.Join(t.TempDir(), "s"), tBucket, &nodeapi.RPC{URL: r.cs.URL + "/rpc", ID: "s"},
		&nodeapi.Asset{Base: r.cs.URL}, r.chain, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := stranger.Tree(r.ctx, storerec.EntryDerivation, 1)
	b, _ := storerec.Build([]byte("x"))
	se := &storerec.Entry{Bucket: tBucket, ID: r.id(), Writer: stranger.Identity(), Key: "stranger", Kind: storerec.KindPut, Created: 3,
		Size: b.Size, Root: b.RootHash, ContentHash: b.ContentHash, ProofRoot: b.ProofRoot, ProofProfile: 1, ContentType: "text/plain"}
	rec, _ := se.Encode()
	sBEEF, _, err := stranger.Carrier(r.ctx, storerec.EntryDerivation, rec, st, 0)
	if err != nil {
		t.Fatal(err)
	}
	r.host.unanchored = func(writer, after string, honest []fakeEntry) []lookup.Output {
		if after != "" {
			return nil
		}
		out := []lookup.Output{{Beef: sBEEF}, {Beef: []byte("not a transaction at all")}}
		for _, e := range honest {
			out = append(out, lookup.Output{Beef: e.beef})
		}
		return out
	}
	rep, err := r.tool.Anchor(r.ctx, r.c, Options{})
	if !errors.Is(err, ErrIncomplete) || len(rep.Hosts) == 0 {
		t.Fatalf("a hostile host was not named: %v %+v", err, rep)
	}
	f, _ := r.fold()
	if _, ok := f.Map["stranger"]; ok {
		t.Fatal("a stranger's entry was applied")
	}
	// The garbage stopped that page: the honest entry behind it waits for
	// an answer that verifies. With the host honest again, it applies.
	r.host.unanchored = func(writer, after string, honest []fakeEntry) []lookup.Output {
		out := []lookup.Output{}
		if after == "" {
			// Ascending by txid, as the class answers.
			for _, b := range [][]byte{sBEEF, foreign[0]} {
				out = append(out, lookup.Output{Beef: b})
			}
			for _, e := range honest {
				out = append(out, lookup.Output{Beef: e.beef})
			}
			sort.Slice(out, func(i, j int) bool { return txidOf(out[i].Beef) < txidOf(out[j].Beef) })
			return out
		}
		for _, e := range honest {
			out = append(out, lookup.Output{Beef: e.beef})
		}
		return out
	}
	rep, err = r.tool.Anchor(r.ctx, r.c, Options{})
	if !errors.Is(err, ErrIncomplete) || rep.Strangers < 2 {
		t.Fatalf("a stranger's entry was not refused: %v %+v", err, rep)
	}
	f, _ = r.fold()
	if f.Map["ok"] != cOf(t, good[0]) {
		t.Fatal("the honest entry beside a stranger's was not applied")
	}
	if _, ok := f.Map["stranger"]; ok {
		t.Fatal("a stranger's entry was applied")
	}
	if _, ok := f.Map["foreign"]; ok {
		t.Fatal("another chain's entry was applied")
	}
}

func txidOf(beef []byte) string {
	tx, err := transaction.NewTransactionFromBEEF(beef)
	if err != nil {
		return ""
	}
	return tx.TxID().String()
}

// An entry whose funding output the writer swept is never applied, though
// a host that never saw the sweep still answers it; one whose funding
// output the node cannot vouch for waits.
func TestAnchorSpendChecks(t *testing.T) {
	r := newRig(t)
	tree, ids := r.mint([]spec{{"swept", storerec.KindPut, 1, nil}, {"kept", storerec.KindPut, 2, nil}}, true)
	// Sweep output 0 of the writer's tree.
	pay := &producer.Payer{Pool: r.writer.W.Pool, Keys: map[string]*bwallet.Signer{r.writer.Signer.IdentityHex(): r.writer.Signer},
		Settler: r.writer.Settler, Asset: r.writer.Asset, Fees: mint.DefaultFees}
	if h, err := r.writer.Asset.BestHeader(r.ctx); err == nil {
		pay.Tip = h.Height
	}
	fee, err := pay.TakeAtLeast(r.ctx, 2000)
	if err != nil {
		t.Fatal(err)
	}
	change, _ := r.writer.Signer.FundScript()
	sw, err := carrier.Sweep(r.ctx, r.writer.W, r.writer.W.Originator, carrier.Params{Derivation: storerec.EntryDerivation, FundingTag: storerec.TagFunding,
		ValidatePayload: func([]byte) error { return nil }, ErrIdentity: storerec.ErrIdentity}, tree, []uint32{0}, fee.Tx, fee.Vout, fee.Unlocker, change, 1, 250)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pay.SettleAndWait(r.ctx, "sweep", sw); err != nil {
		t.Fatal(err)
	}
	// The node cannot say whether the tree's outputs are spent: nothing
	// applies, and the run is incomplete.
	r.failSpend.Store(tree.TxID().String())
	rep, err := r.tool.Anchor(r.ctx, r.c, Options{})
	if !errors.Is(err, ErrIncomplete) || len(rep.Unvouched) != 2 || len(rep.Tokens) != 0 {
		t.Fatalf("no evidence: %v %+v", err, rep)
	}
	r.failSpend.Store("")
	rep, err = r.tool.Anchor(r.ctx, r.c, Options{})
	if err != nil || len(rep.Swept) != 1 || len(rep.Tokens) != 1 {
		t.Fatalf("%v %+v", err, rep)
	}
	f, _ := r.fold()
	if _, ok := f.Map["swept"]; ok {
		t.Fatal("a swept entry was applied")
	}
	if f.Map["kept"] != cOf(t, ids[1]) {
		t.Fatal("the entry beside it was not applied")
	}
}

// A writer is removed only when the map names none of its entries.
func TestAnchorRemoveWriter(t *testing.T) {
	r := newRig(t)
	r.mint([]spec{{"w", storerec.KindPut, 1, nil}}, true)
	if _, err := r.tool.Anchor(r.ctx, r.c, Options{}); err != nil {
		t.Fatal(err)
	}
	w := hex.EncodeToString(r.writer.Identity())
	if _, err := r.tool.Anchor(r.ctx, r.c, Options{RemoveWriters: []string{w}}); err == nil || !strings.Contains(err.Error(), "names 1") {
		t.Fatalf("a writer whose entry the map names was removed: %v", err)
	}
}

func hasKey(m map[string]string, k string) bool {
	_, ok := m[k]
	return ok
}

func cOfHex(txid string) [32]byte {
	b, _ := hex.DecodeString(txid)
	var c [32]byte
	for i := range c {
		c[i] = b[31-i]
	}
	return c
}
