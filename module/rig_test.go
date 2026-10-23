package module

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/guard"
	bmodule "github.com/lightwebinc/bcommon/module"
	"github.com/lightwebinc/bcommon/pushdrop"
	"github.com/lightwebinc/bcommon/termsroute"

	"github.com/lightwebinc/bstore/storerec"
)

// The module and an engine together, for tests: a stand-in for the Go
// engine's storage and admission path with the module's managers and
// lookup service mounted, the configuration the tests use, and submit and
// answer helpers. The twin of the TypeScript module's testhost.ts and
// testworld.ts.

const (
	bucket   = "photos_qzvmkwerty"
	topic    = "tm_bstore_" + bucket
	vecDir   = "../testdata/vectors"
	dayMs    = int64(24 * time.Hour / time.Millisecond)
	hourMs   = int64(time.Hour / time.Millisecond)
	dayDur   = 24 * time.Hour
	hourDur  = time.Hour
	someDays = 400 * dayDur
)

// fakeHost records what the module counts and logs.
type fakeHost struct {
	mu       sync.Mutex
	counters map[string]float64
	gauges   map[string]func() float64
	lines    []logLine
}

type logLine struct {
	msg   string
	extra map[string]any
}

func newFakeHost() *fakeHost {
	return &fakeHost{counters: map[string]float64{}, gauges: map[string]func() float64{}}
}

func mkey(name string, labels map[string]string) string {
	ks := make([]string, 0, len(labels))
	for k := range labels {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	parts := make([]string, 0, len(ks))
	for _, k := range ks {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	if len(parts) == 0 {
		return name
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (h *fakeHost) Log(msg string, extra map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, logLine{msg, extra})
}
func (h *fakeHost) Metrics() bmodule.Metrics { return h }
func (h *fakeHost) Inc(name string, labels map[string]string, by float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counters[mkey(name, labels)] += by
}
func (h *fakeHost) Preset(name string, labels map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.counters[mkey(name, labels)]; !ok {
		h.counters[mkey(name, labels)] = 0
	}
}
func (h *fakeHost) Gauge(name string, fn func() float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gauges[name] = fn
}
func (h *fakeHost) count(name string, labels map[string]string) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counters[mkey(name, labels)]
}
func (h *fakeHost) logged(msg string, f func(map[string]any) bool) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, l := range h.lines {
		if l.msg == msg && (f == nil || f(l.extra)) {
			return true
		}
	}
	return false
}

// memStorage is the engine's storage in memory, as the Go engine's store
// behaves where the module depends on it: FindOutput matches spent and
// unspent outputs, the unspent rows are what a restore is handed,
// DeleteOutput deletes one output and leaves the applied transactions,
// which DeleteAppliedTransaction removes.
type memStorage struct {
	mu      sync.Mutex
	outputs map[string]*bmodule.Output
	applied map[string]bool
	// slow, when set, is waited on by every FindOutput.
	slow chan struct{}
}

func newMemStorage() *memStorage {
	return &memStorage{outputs: map[string]*bmodule.Output{}, applied: map[string]bool{}}
}

func skey(topic string, op *transaction.Outpoint) string { return topic + "|" + op.String() }

func (m *memStorage) FindOutput(_ context.Context, op *transaction.Outpoint, topic string) (*bmodule.Output, error) {
	if m.slow != nil {
		<-m.slow
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.outputs))
	for k := range m.outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		o := m.outputs[k]
		if o.Outpoint == *op && (topic == "" || o.Topic == topic) {
			c := *o
			return &c, nil
		}
	}
	return nil, nil
}

func (m *memStorage) DeleteOutput(_ context.Context, op *transaction.Outpoint, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.outputs, skey(topic, op))
	return nil
}

func (m *memStorage) DeleteAppliedTransaction(_ context.Context, txid *chainhash.Hash, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.applied, topic+"|"+txid.String())
	return nil
}

func (m *memStorage) DoesAppliedTransactionExist(_ context.Context, txid *chainhash.Hash, topic string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied[topic+"|"+txid.String()], nil
}

func (m *memStorage) isApplied(txid, topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied[topic+"|"+txid]
}

func (m *memStorage) held(txid string, vout uint32, topic string) bool {
	h, _ := chainhash.NewHashFromHex(txid)
	o, _ := m.FindOutput(context.Background(), &transaction.Outpoint{Txid: *h, Index: vout}, topic)
	return o != nil
}

func (m *memStorage) markSpent(txid string, vout uint32, topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, _ := chainhash.NewHashFromHex(txid)
	if o := m.outputs[skey(topic, &transaction.Outpoint{Txid: *h, Index: vout})]; o != nil {
		o.Spent = true
	}
}

// utxos are the unspent rows of topic, in the order they were stored.
func (m *memStorage) utxos(topic string) []bmodule.Output {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []bmodule.Output
	for _, o := range m.outputs {
		if o.Topic == topic && !o.Spent {
			out = append(out, *o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score < out[j].Score })
	return out
}

// clock is a clock the tests move.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock(ms uint64) *clock { return &clock{t: time.UnixMilli(int64(ms))} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *clock) ms() int64 { return c.now().UnixMilli() }

// baseConfig is the configuration the tests use.
func baseConfig() *Config {
	return &Config{Buckets: []string{bucket}, StateDir: "/nonexistent", HeadersURL: "http://127.0.0.1:1", KeepDays: 31, PendingDays: 7, OtherHours: 24,
		MaxBEEF: storerec.DefaultMaxBEEF, MaxSweepInputs: storerec.DefaultMaxSweepInputs, Prices: map[string]uint64{},
		Sessions: termsroute.SessionBound{Max: 100, TTL: 600 * time.Second}, Budget: termsroute.DefaultBudget, Response: termsroute.DefaultResponseBudget}
}

// rig is a module and its engine, with storage handed over as the host
// does.
type rig struct {
	t             *testing.T
	host          *fakeHost
	m             *Mounted
	ls            *Service
	storage       *memStorage
	journal       *MemoryJournal
	chain         *testChain
	config        *Config
	clock         *clock
	overlayTopics []string
	// naive returns every refusal as empty instructions, as a manager that
	// does not raise would; calls counts the managers' calls.
	naive bool
	calls int
}

type setupOpts struct {
	naive         bool
	journal       *MemoryJournal
	chain         *testChain
	storage       *memStorage
	config        func(*Config)
	clock         *clock
	overlayTopics []string
}

func setup(t *testing.T, o setupOpts) *rig {
	t.Helper()
	host := newFakeHost()
	if o.chain == nil {
		o.chain = newTestChain(70000, nil)
	}
	if o.journal == nil {
		o.journal = &MemoryJournal{}
	}
	if o.clock == nil {
		o.clock = newClock(uint64(time.Now().UnixMilli()))
	}
	c := baseConfig()
	if o.config != nil {
		o.config(c)
	}
	topics := o.overlayTopics
	if topics == nil {
		for _, b := range c.Buckets {
			topics = append(topics, storerec.RecordsPrefix+b)
		}
	}
	m, err := Mount(host, c, topics, o.journal, o.chain, o.clock.now)
	if err != nil {
		t.Fatal(err)
	}
	if o.storage == nil {
		o.storage = newMemStorage()
	}
	var rows []bmodule.Output
	for name := range m.Module.Topics {
		rows = append(rows, o.storage.utxos(name)...)
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Score < rows[j].Score })
	if _, err := m.LS.Restore(context.Background(), rows, o.storage); err != nil {
		t.Fatal(err)
	}
	return &rig{t: t, host: host, m: m, ls: m.LS, storage: o.storage, journal: o.journal, chain: o.chain, config: c, clock: o.clock, overlayTopics: o.overlayTopics, naive: o.naive}
}

// restart is a new module and lookup service over the same storage and
// journal.
func (r *rig) restart() *rig {
	c := r.config
	return setup(r.t, setupOpts{chain: r.chain, storage: r.storage, journal: r.journal, clock: r.clock, overlayTopics: r.overlayTopics,
		config: func(x *Config) { *x = *c }})
}

// admit is the engine's admission path without the drops carried out:
// the outputs admitted, or an error when the engine or the manager
// refused. A transaction the topic applied is a duplicate the manager is
// never shown. The order is the Go engine's: outputs stored, the lookup
// services told, then the applied transaction recorded.
func (r *rig) admit(raw []byte, t string) ([]uint32, error) { return r.admitWith(raw, t, nil) }

// admitWith is admit with told run after the lookup services are told and
// before the applied transaction is recorded.
func (r *rig) admitWith(raw []byte, t string, told func()) ([]uint32, error) {
	ctx := bmodule.WithSubmission(context.Background(), raw)
	beef, tx, txid, err := guard.ParseBEEF(raw, guard.DefaultBound)
	if err != nil || tx == nil {
		return nil, fmt.Errorf("the engine refused the BEEF: %v", err)
	}
	st := r.storage
	if st.isApplied(txid.String(), t) {
		return []uint32{}, nil
	}
	tm := r.m.Module.Topics[t]
	if tm == nil {
		return nil, fmt.Errorf("no manager for %s", t)
	}
	st.mu.Lock()
	var coins []uint32
	held := map[uint32]*bmodule.Output{}
	for i, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		if o := st.outputs[skey(t, &transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex})]; o != nil {
			coins = append(coins, uint32(i)) //nolint:gosec // test
			held[uint32(i)] = o              //nolint:gosec // test
		}
	}
	st.mu.Unlock()
	r.calls++
	ai, err := tm.IdentifyAdmissibleOutputs(ctx, beef, txid, coins)
	if err != nil {
		if _, refused := err.(*Refused); !refused || !r.naive {
			return nil, err
		}
		ai.OutputsToAdmit, ai.CoinsToRetain = nil, coins
	}
	atomic, err := beef.AtomicBytes(txid)
	if err != nil {
		return nil, err
	}
	st.mu.Lock()
	retain := map[uint32]bool{}
	for _, c := range ai.CoinsToRetain {
		retain[c] = true
	}
	for vin, o := range held {
		if retain[vin] {
			o.Spent = true
			o.ConsumedBy = append(o.ConsumedBy, &transaction.Outpoint{Txid: *txid, Index: 0})
		} else {
			delete(st.outputs, skey(t, &o.Outpoint))
		}
	}
	for _, vout := range ai.OutputsToAdmit {
		op := transaction.Outpoint{Txid: *txid, Index: vout}
		st.outputs[skey(t, &op)] = &bmodule.Output{Outpoint: op, Topic: t, Beef: atomic, Score: float64(r.clock.now().UnixMicro()) + float64(len(st.outputs)),
			OutputScript: tx.Outputs[vout].LockingScript, Satoshis: tx.Outputs[vout].Satoshis}
	}
	st.mu.Unlock()
	for _, vout := range ai.OutputsToAdmit {
		if err := r.ls.OutputAdmittedByTopic(ctx, &bmodule.OutputAdmittedByTopic{Mode: bmodule.AdmitWholeTx, Outpoint: transaction.Outpoint{Txid: *txid, Index: vout}, Topic: t, AtomicBEEF: atomic}); err != nil {
			return nil, err
		}
	}
	if told != nil {
		told()
	}
	st.mu.Lock()
	st.applied[t+"|"+txid.String()] = true
	st.mu.Unlock()
	if ai.OutputsToAdmit == nil {
		return []uint32{}, nil
	}
	return ai.OutputsToAdmit, nil
}

// submit submits to topic t (the records topic when ""): the outputs
// admitted, or nil when the engine or a manager refused. The drops the
// index decided are then carried out.
func (r *rig) submit(raw []byte, t string) []uint32 {
	if t == "" {
		t = topic
	}
	got, err := r.admit(raw, t)
	r.ls.Flush(context.Background())
	if err != nil {
		return nil
	}
	return got
}

// submitTx submits a transaction alone (Atomic BEEF), as a carrier or a
// mined object is published.
func (r *rig) submitTx(tx *transaction.Transaction, t string) []uint32 {
	return r.submit(atomicOf(r.t, tx), t)
}

type answered struct {
	tx      *transaction.Transaction
	txid    string
	vout    uint32
	beef    []byte
	context []byte
}

func question(q map[string]string) *lookup.LookupQuestion {
	b, _ := json.Marshal(q)
	return &lookup.LookupQuestion{Service: storerec.LookupService, Query: b}
}

// outputs is an engine answer, hydrated: each output's transaction, and
// its context when it has one.
func (r *rig) outputs(q map[string]string) []answered {
	r.t.Helper()
	a, err := r.ls.Lookup(context.Background(), question(q))
	if err != nil {
		r.t.Fatalf("%v: %v", q, err)
	}
	outs, _ := a.Result.([]bmodule.HydratedOutput)
	var res []answered
	for _, o := range outs {
		tx, err := parse(o.Beef)
		if err != nil {
			r.t.Fatal(err)
		}
		res = append(res, answered{tx: tx, txid: tx.TxID().String(), vout: o.OutputIndex, beef: o.Beef, context: o.Context})
	}
	return res
}

// answer is an engine answer: each output's txid.
func (r *rig) answer(q map[string]string) []string {
	r.t.Helper()
	out := []string{}
	for _, a := range r.outputs(q) {
		out = append(out, a.txid)
	}
	return out
}

// refused is a question the service refuses: its error.
func (r *rig) refused(q map[string]string) string {
	r.t.Helper()
	_, err := r.ls.Lookup(context.Background(), question(q))
	if err == nil {
		r.t.Fatalf("answered: %v", q)
	}
	return err.Error()
}

func (r *rig) status(txid string) string {
	s, _ := r.ls.TokenStatus(txid)
	return s
}

func (r *rig) prune(d time.Duration) int {
	r.clock.advance(d)
	return r.ls.Prune(context.Background())
}

// world is a bucket world: a served chain's owner, a writer the owner
// names, the owner writing under its own key, a stranger, two providers, a
// buyer and a front, over one local chain.
type world struct {
	r      *rig
	chain  *testChain
	clock  *clock
	owner  *ownerSim
	alice  *author
	owned  *author
	bob    *author
	prov   *provider
	prov2  *provider
	buyer  *author
	front  *author
	chainQ map[string]string
}

type worldOpts struct {
	unserved bool
	seed     string
	chain    *testChain
	clock    *clock
	config   func(*Config)
}

func newWorld(t *testing.T, o worldOpts) *world {
	t.Helper()
	seed := o.seed
	if seed == "" {
		seed = "w"
	}
	ch := o.chain
	if ch == nil {
		ch = newTestChain(80000, nil)
	}
	cl := o.clock
	if cl == nil {
		cl = newClock(T0 + uint64(dayMs))
	}
	owner := newOwner(bucket, seed+" owner", ch)
	r := setup(t, setupOpts{chain: ch, clock: cl, config: func(c *Config) {
		if o.config != nil {
			o.config(c)
		}
		if o.unserved {
			c.Served = nil
		} else {
			c.Served = []string{owner.served()}
		}
	}})
	return &world{r: r, chain: ch, clock: cl, owner: owner, alice: newAuthor(bucket, seed+" alice", ch), owned: newAuthor(bucket, seed+" owner", ch),
		bob: newAuthor(bucket, seed+" bob", ch), prov: newProvider(seed+" provider", ch), prov2: newProvider(seed+" other provider", ch),
		buyer: newAuthor(bucket, seed+" buyer", ch), front: newAuthor(bucket, seed+" front", ch),
		chainQ: map[string]string{"bucket": bucket, "owner": owner.hex(), "id": owner.idHex()}}
}

func with(q map[string]string, kv ...string) map[string]string {
	out := map[string]string{}
	for k, v := range q {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

// tokenOf is the bucket record of a token output.
func tokenOf(t *testing.T, a answered) *storerec.Bucket {
	t.Helper()
	tok, err := storerec.ReadToken(a.tx)
	if err != nil || tok.Vout != a.vout {
		t.Fatalf("%s.%d is not a token: %v", a.txid, a.vout, err)
	}
	return tok.Bucket
}

func entryOf(t *testing.T, a answered) *storerec.Entry {
	t.Helper()
	p, _ := pushdrop.FirstPush(*a.tx.Outputs[0].LockingScript)
	e, err := storerec.DecodeEntry(p)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

type objectRead struct {
	result storerec.ObjectResult
	entry  string
	token  string
}

// readObject is an object answer, checked as a reader checks it (spec
// section 15.2, steps 2 to 5): what it proves, and the entry's txid when
// found.
func readObject(t *testing.T, outs []answered, key string) (objectRead, error) {
	t.Helper()
	tok := tokenOf(t, outs[0])
	leaves, err := storerec.DecodeTokenContext(outs[0].context)
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) < 2 {
		res, err := storerec.CheckHead(tok, key, nil, nil, nil, leaves)
		return objectRead{result: res, token: outs[0].txid}, err
	}
	e := entryOf(t, outs[1])
	ectx, err := storerec.DecodeEntryContext(outs[1].context)
	if err != nil {
		t.Fatal(err)
	}
	c := commitment(outs[1].tx)
	res, err := storerec.CheckHead(tok, key, &c, e, &ectx, leaves)
	return objectRead{result: res, entry: outs[1].txid, token: outs[0].txid}, err
}

type page struct {
	keys   []string
	leaves []string
	txids  []string
}

// readPage is a list or export page, checked as a reader checks it (spec
// section 11.3): its keys, and the leaves' roles and keys.
func readPage(t *testing.T, outs []answered, p, a string, exported bool) page {
	t.Helper()
	tok := tokenOf(t, outs[0])
	leaves, err := storerec.DecodeTokenContext(outs[0].context)
	if err != nil {
		t.Fatal(err)
	}
	var entries []storerec.PageEntry
	res := page{keys: []string{}, leaves: []string{}}
	for _, o := range outs[1:] {
		e := entryOf(t, o)
		ectx, err := storerec.DecodeEntryContext(o.context)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, storerec.PageEntry{Key: e.Key, C: commitment(o.tx), Ctx: ectx})
		res.keys = append(res.keys, e.Key)
		res.txids = append(res.txids, o.txid)
	}
	max := storerec.PageList
	if exported {
		max = storerec.PageExport
	}
	if err := storerec.CheckPage(tok.Count, tok.MapRoot, p, a, max, entries, leaves); err != nil {
		t.Fatalf("page %q after %q: %v", p, a, err)
	}
	for _, l := range leaves {
		res.leaves = append(res.leaves, fmt.Sprintf("%d %s", l.Role, l.Key))
	}
	return res
}

func (w *world) object(t *testing.T, key string) (*objectRead, error) {
	t.Helper()
	outs := w.r.outputs(with(w.chainQ, "key", key))
	if len(outs) == 0 {
		return nil, nil
	}
	o, err := readObject(t, outs, key)
	return &o, err
}

func (w *world) list(t *testing.T, prefix, after string) page {
	t.Helper()
	return readPage(t, w.r.outputs(with(w.chainQ, "prefix", prefix, "after", after)), prefix, after, false)
}

func equalStrings(t *testing.T, got, want []string, what string) {
	t.Helper()
	if got == nil {
		got = []string{}
	}
	if want == nil {
		want = []string{}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s:\n got %v\nwant %v", what, got, want)
	}
}

func equalU32(t *testing.T, got, want []uint32, what string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) && !(len(got) == 0 && len(want) == 0) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func txids(ps ...*posted) []string {
	out := []string{}
	for _, p := range ps {
		out = append(out, p.txid)
	}
	return out
}

func sorted(a []string) []string {
	out := append([]string(nil), a...)
	sort.Strings(out)
	return out
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ---- the golden transaction vectors

type txVector struct {
	Name string `json:"name"`
	Txid string `json:"txid"`
	BEEF string `json:"beef"`
	Fill *struct {
		At   int  `json:"at"`
		Byte byte `json:"byte"`
		Len  int  `json:"len"`
	} `json:"beefFill"`
	PreviousCoins []int  `json:"previousCoins"`
	Verdict       string `json:"verdict"`
	Reason        string `json:"reason"`
	Admits        *struct {
		Kind           string   `json:"kind"`
		OutputsToAdmit []uint32 `json:"outputsToAdmit"`
		CoinsToRetain  []uint32 `json:"coinsToRetain"`
	} `json:"admits"`
}

type txVectors struct {
	Bucket  string `json:"bucket"`
	Topic   string `json:"topic"`
	Headers []struct {
		Height     uint32 `json:"height"`
		MerkleRoot string `json:"merkleRoot"`
	} `json:"headers"`
	Cases []txVector `json:"cases"`
}

func readTxVectors(t *testing.T) (*txVectors, map[uint32]chainhash.Hash) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(vecDir, "transaction-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v txVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	h := map[uint32]chainhash.Hash{}
	for _, x := range v.Headers {
		r, err := chainhash.NewHashFromHex(x.MerkleRoot)
		if err != nil {
			t.Fatal(err)
		}
		h[x.Height] = *r
	}
	return &v, h
}

func (v *txVectors) byName(t *testing.T, name string) txVector {
	t.Helper()
	for _, c := range v.Cases {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no transaction vector %s", name)
	return txVector{}
}

// beefOf is a vector's BEEF, its filler expanded.
func beefOf(t *testing.T, c txVector) []byte {
	t.Helper()
	b := unhex(t, c.BEEF)
	if c.Fill == nil {
		return b
	}
	out := make([]byte, 0, len(b)+c.Fill.Len)
	out = append(out, b[:c.Fill.At]...)
	for range c.Fill.Len {
		out = append(out, c.Fill.Byte)
	}
	return append(out, b[c.Fill.At:]...)
}

func txOf(t *testing.T, c txVector) *transaction.Transaction {
	t.Helper()
	tx, err := parse(beefOf(t, c))
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// headersOf is a chain tracker over fixed roots.
type headersOf map[uint32]chainhash.Hash

func (h headersOf) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	r, ok := h[height]
	return ok && root != nil && r == *root, nil
}

func (h headersOf) CurrentHeight(context.Context) (uint32, error) {
	var top uint32
	for k := range h {
		top = max(top, k)
	}
	return top, nil
}

var _ chaintracker.ChainTracker = headersOf{}
