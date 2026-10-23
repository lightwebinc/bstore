package module

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	clients "github.com/bsv-blockchain/go-sdk/auth/clients/authhttp"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/termsroute"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

// The terms route over real HTTP (paid.test.ts): the terms document, free
// questions with and without BRC-104, each priceable class answered 402
// and then answered once paid by go-sdk's own client, the export page with
// its contexts, and the payee ledger line byte for byte with the
// TypeScript host's golden file. The route's own hardening (sessions,
// budgets, replay, payment refusals, acceptance) is bcommon termsroute's
// and is tested there.

const allPrices = "history=5,chain=10,export=20,outcome-history=3,outcome-history-of=4"

var payeeKey = func() *ec.PrivateKey { k, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x11}, 32)); return k }()

// both are the module's chain and the payment chain as one header source.
type both struct {
	a, b interface {
		IsValidRootForHeight(context.Context, *chainhash.Hash, uint32) (bool, error)
		CurrentHeight(context.Context) (uint32, error)
	}
}

func (x both) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, h uint32) (bool, error) {
	if ok, _ := x.a.IsValidRootForHeight(ctx, root, h); ok {
		return true, nil
	}
	return x.b.IsValidRootForHeight(ctx, root, h)
}

func (x both) CurrentHeight(ctx context.Context) (uint32, error) { return x.b.CurrentHeight(ctx) }

// payer is a paying client: a key's wallet, and CreateAction paying from a
// mature coinbase of the payment chain.
type payer struct {
	*wallet.CompletedProtoWallet
	t     *testing.T
	chain *testchain.Chain
	key   *ec.PrivateKey
	mu    sync.Mutex
	spent map[string]bool
}

func newPayer(t *testing.T, chain *testchain.Chain, label string) *payer {
	p := newParty(label)
	w, err := wallet.NewCompletedProtoWallet(p.key)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := script.NewAddressFromPublicKey(p.key.PubKey(), true)
	if _, err := chain.Generate(testchain.Maturity+8, addr.AddressString); err != nil {
		t.Fatal(err)
	}
	return &payer{CompletedProtoWallet: w, t: t, chain: chain, key: p.key, spent: map[string]bool{}}
}

func (c *payer) coin() *transaction.Transaction {
	c.mu.Lock()
	defer c.mu.Unlock()
	addr, _ := script.NewAddressFromPublicKey(c.key.PubKey(), true)
	lock, _ := p2pkh.Lock(addr)
	tip := c.chain.Height()
	for _, id := range c.chain.Txids() {
		if c.spent[id] {
			continue
		}
		tx := c.chain.Tx(id)
		if tx == nil || len(tx.Inputs) != 1 || tx.Inputs[0].SourceTXID == nil || *tx.Inputs[0].SourceTXID != (chainhash.Hash{}) {
			continue
		}
		mp, h, ok := c.chain.Proof(id)
		if !ok || tip+1-h < testchain.Maturity || !bytes.Equal(*tx.Outputs[0].LockingScript, *lock) {
			continue
		}
		tx.MerklePath = mp
		c.spent[id] = true
		return tx
	}
	c.t.Fatal("no mature coin")
	return nil
}

func (c *payer) CreateAction(_ context.Context, args wallet.CreateActionArgs, _ string) (*wallet.CreateActionResult, error) {
	src := c.coin()
	tx := transaction.NewTransaction()
	unlock, _ := p2pkh.Unlock(c.key, nil)
	if err := tx.AddInputFrom(src.TxID().String(), 0, src.Outputs[0].LockingScript.String(), src.Outputs[0].Satoshis, unlock); err != nil {
		return nil, err
	}
	tx.Inputs[0].SourceTransaction = src
	ls := script.Script(args.Outputs[0].LockingScript)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: args.Outputs[0].Satoshis, LockingScript: &ls})
	addr, _ := script.NewAddressFromPublicKey(c.key.PubKey(), true)
	change, _ := p2pkh.Lock(addr)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: src.Outputs[0].Satoshis - args.Outputs[0].Satoshis - 500, LockingScript: change})
	if err := tx.Sign(); err != nil {
		return nil, err
	}
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return nil, err
	}
	return &wallet.CreateActionResult{Txid: *tx.TxID(), Tx: beef}, nil
}

type paidRig struct {
	*rig
	srv      *httptest.Server
	owner    *ownerSim
	prov     *provider
	tokens   []string
	entries  []string
	outcomes []string
	client   *payer
	payChain *testchain.Chain
}

// newPaidRig is a module serving one chain of three tokens, the second
// anchoring three entries, with two outcomes, its terms route on a test
// server, and a client that pays from the payment chain.
func newPaidRig(t *testing.T, prices string) *paidRig {
	ch := newTestChain(95000, nil)
	owner := newOwner(bucket, "paid owner", ch)
	payChain := testchain.New(700)
	node := httptest.NewServer(payChain)
	t.Cleanup(node.Close)
	p, err := termsroute.ParsePrices(prices, Classes, "BSTORE_PRICES")
	if err != nil {
		t.Fatal(err)
	}
	host := newFakeHost()
	cl := newClock(T0 + uint64(dayMs))
	c := baseConfig()
	c.Served, c.Prices, c.Listen, c.PayeeKey, c.StateDir = []string{owner.served()}, p, "127.0.0.1:0", payeeKey, t.TempDir()
	if c.Accept, err = termsroute.ParseAcceptConfig("BSTORE", env(map[string]string{"BSTORE_ARCADE_URL": node.URL + "/arcade", "BSTORE_ASSET_URL": node.URL})); err != nil {
		t.Fatal(err)
	}
	c.Accept.Policy.Wait, c.Accept.Policy.Poll = 2*time.Second, 50*time.Millisecond
	m, err := Mount(host, c, []string{topic}, &MemoryJournal{}, both{ch, payChain}, cl.now)
	if err != nil {
		t.Fatal(err)
	}
	st := newMemStorage()
	if _, err := m.LS.Restore(context.Background(), nil, st); err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, host: host, m: m, ls: m.LS, storage: st, journal: &MemoryJournal{}, chain: ch, config: c, clock: cl}
	alice := newAuthor(bucket, "paid writer", ch)
	es := []*posted{alice.put(owner.id, "k/1", putOpts{}), alice.put(owner.id, "k/2", putOpts{}), alice.put(owner.id, "k/3", putOpts{})}
	for _, e := range es {
		r.submitTx(e.tx, "")
	}
	ts := []*minted{owner.token(tokenOpts{}), owner.token(tokenOpts{writers: [][]byte{alice.id()}, anchor: es}), owner.token(tokenOpts{})}
	for _, x := range ts {
		r.submit(x.beef, "")
	}
	prov := newProvider("paid provider", ch)
	buyer := newAuthor(bucket, "paid buyer", ch)
	os := []*posted{buyer.outcome(outcomeOpts{root: es[0].e.Root, provider: prov.party, created: T0}), buyer.outcome(outcomeOpts{root: es[0].e.Root, provider: prov.party, created: T0 + 1})}
	for _, o := range os {
		r.submitTx(o.tx, "")
	}
	srv := httptest.NewServer(m.Front.Front)
	t.Cleanup(srv.Close)
	pr := &paidRig{rig: r, srv: srv, owner: owner, prov: prov, entries: txids(es...), outcomes: []string{os[1].txid, os[0].txid}, payChain: payChain}
	for _, x := range ts {
		pr.tokens = append(pr.tokens, x.txid)
	}
	pr.client = newPayer(t, payChain, "client")
	return pr
}

func (r *paidRig) free() map[string]string {
	return map[string]string{"bucket": bucket, "owner": r.owner.hex(), "id": r.owner.idHex()}
}
func (r *paidRig) chainQ() map[string]string {
	return map[string]string{"bucket": bucket, "owner": r.owner.hex(), "chain": r.owner.idHex(), "after": "0"}
}
func (r *paidRig) historyQ() map[string]string {
	return with(r.free(), "history", "k/2", "before", "9")
}
func (r *paidRig) exportQ() map[string]string {
	return map[string]string{"bucket": bucket, "owner": r.owner.hex(), "export": r.owner.idHex(), "after": ""}
}
func (r *paidRig) outcomeQ(member string) map[string]string {
	return map[string]string{"bucket": bucket, member: r.prov.hex(), "before": "1798761600009:" + strings.Repeat("f", 64)}
}

func ask(q map[string]string) []byte {
	b, _ := json.Marshal(map[string]any{"service": "ls_bstore", "query": q})
	return b
}

func post(t *testing.T, url string, body []byte) (int, []byte) {
	t.Helper()
	res, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, b
}

// answerTxids are the txids of an output-list answer's outputs.
func answerTxids(t *testing.T, body []byte) []string {
	t.Helper()
	var a struct {
		Type    string `json:"type"`
		Outputs []struct {
			Beef        []int  `json:"beef"`
			OutputIndex uint32 `json:"outputIndex"`
			Context     []int  `json:"context"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(body, &a); err != nil || a.Type != "output-list" {
		t.Fatalf("not an output list: %s", body)
	}
	out := []string{}
	for _, o := range a.Outputs {
		b := make([]byte, len(o.Beef))
		for i, x := range o.Beef {
			b[i] = byte(x)
		}
		tx, err := parse(b)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, tx.TxID().String())
	}
	return out
}

func TestPricesAndTermsDocument(t *testing.T) {
	p, err := termsroute.ParsePrices("export=7, chain=20", Classes, "BSTORE_PRICES")
	if err != nil || p["export"] != 7 || p["chain"] != 20 || len(p) != 2 {
		t.Fatal(p, err)
	}
	if p, err := termsroute.ParsePrices("", Classes, "BSTORE_PRICES"); err != nil || len(p) != 0 {
		t.Fatal(p, err)
	}
	for _, bad := range []string{"bucket=1", "object=1", "list=1", "outcomes=1", "sweep=1", "history", "history=-1", "history=01", "history=1.5", "history=1,history=2", "nosuch=1", "chain=9007199254740992"} {
		if _, err := termsroute.ParsePrices(bad, Classes, "BSTORE_PRICES"); err == nil {
			t.Errorf("%q", bad)
		}
	}
	p, _ = termsroute.ParsePrices("outcome-history-of=7,chain=20,history=0", Classes, "BSTORE_PRICES")
	b, _ := json.Marshal(termsroute.TermsDocument(storerec.LookupService, Classes, p))
	if string(b) != `{"service":"ls_bstore","terms":1,"classes":[{"class":"history","satoshis":0},{"class":"chain","satoshis":20},{"class":"outcome-history-of","satoshis":7}]}` {
		t.Fatal(string(b))
	}
}

func TestTermsAndFreeQuestions(t *testing.T) {
	r := newPaidRig(t, allPrices)
	res, err := http.Get(r.srv.URL + "/ls_bstore/terms")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	p, _ := termsroute.ParsePrices(allPrices, Classes, "BSTORE_PRICES")
	want, _ := json.Marshal(termsroute.TermsDocument(storerec.LookupService, Classes, p))
	if res.StatusCode != 200 || !bytes.Equal(bytes.TrimSpace(body), want) {
		t.Fatalf("%d %s", res.StatusCode, body)
	}
	code, body := post(t, r.srv.URL+"/lookup", ask(r.free()))
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	equalStrings(t, answerTxids(t, body), r.tokens[2:], "free answer")
	for _, q := range []map[string]string{with(r.free(), "bucket", "other_qzvmkwerty"), with(r.free(), "extra", "x"), {"bucket": bucket, "owner": r.owner.hex()}} {
		if code, body := post(t, r.srv.URL+"/lookup", ask(q)); code != 400 {
			t.Fatalf("%v: %d %s", q, code, body)
		}
	}
	if code, _ := post(t, r.srv.URL+"/lookup", []byte("nope")); code != 400 {
		t.Fatal(code)
	}
	if res, _ := http.Get(r.srv.URL + "/nosuch"); res.StatusCode != 404 {
		t.Fatal(res.StatusCode)
	}
	for _, q := range []map[string]string{r.chainQ(), r.historyQ(), r.exportQ(), r.outcomeQ("outcome-history"), r.outcomeQ("outcome-history-of")} {
		if code, body := post(t, r.srv.URL+"/lookup", ask(q)); code != 401 {
			t.Fatalf("a priced class is asked over BRC-104: %d %s", code, body)
		}
	}
	// No spelling turns a free question into a priced one.
	if code, _ := post(t, r.srv.URL+"/lookup", ask(with(r.chainQ(), "id", r.owner.idHex()))); code != 400 {
		t.Fatal(code)
	}
	// The engine's own route refuses a priced class with an error that says where to ask.
	if _, err := r.ls.Lookup(context.Background(), question(r.chainQ())); err == nil || !strings.Contains(err.Error(), "priced on this host") {
		t.Fatal(err)
	}
	none := newPaidRig(t, "")
	if res, _ := http.Get(none.srv.URL + "/ls_bstore/terms"); res.StatusCode != 404 {
		t.Fatal("a host that prices nothing serves terms")
	}
	code, body = post(t, none.srv.URL+"/lookup", ask(none.chainQ()))
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	equalStrings(t, answerTxids(t, body), none.tokens[1:], "unpriced chain")
}

func TestEachPricedClassPaid(t *testing.T) {
	r := newPaidRig(t, allPrices)
	af := clients.New(r.client, clients.WithoutLogging())
	cases := []struct {
		class string
		q     map[string]string
		price string
		want  []string
	}{
		{"history", r.historyQ(), "5", []string{r.tokens[1], r.entries[1]}},
		{"chain", r.chainQ(), "10", r.tokens[1:]},
		{"export", r.exportQ(), "20", append([]string{r.tokens[2]}, r.entries...)},
		{"outcome-history", r.outcomeQ("outcome-history"), "3", r.outcomes},
		{"outcome-history-of", r.outcomeQ("outcome-history-of"), "4", r.outcomes},
	}
	for _, c := range cases {
		res, err := af.Fetch(context.Background(), r.srv.URL+"/lookup", &clients.SimplifiedFetchRequestOptions{Method: http.MethodPost, Headers: map[string]string{"content-type": "application/json"}, Body: ask(c.q)})
		if err != nil {
			t.Fatalf("%s: %v", c.class, err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || res.Header.Get("x-bsv-payment-satoshis-paid") != c.price {
			t.Fatalf("%s: %d %s %v", c.class, res.StatusCode, body, res.Header)
		}
		equalStrings(t, answerTxids(t, body), c.want, c.class)
		if res.Header.Get("x-bsv-auth-identity-key") != payeeKey.PubKey().ToDERHex() {
			t.Fatalf("%s: not signed by the payee", c.class)
		}
	}
	if n := r.host.count("bstore_payments_total", map[string]string{"decision": "requested", "reason": "no-payment"}); n != 5 {
		t.Fatalf("requested %v", n)
	}
	if n := r.host.count("bstore_payments_total", map[string]string{"decision": "fast", "reason": "at-or-below-threshold"}); n != 5 {
		t.Fatalf("fast %v", n)
	}
	// Each payment is a ledger line, in the payee layout.
	text, err := os.ReadFile(r.config.StateDir + "/" + termsroute.LedgerFile)
	if err != nil || bytes.Count(text, []byte("\n")) != 5 {
		t.Fatalf("ledger: %v\n%s", err, text)
	}
	for i, line := range bytes.Split(bytes.TrimSpace(text), []byte("\n")) {
		var l map[string]any
		if err := json.Unmarshal(line, &l); err != nil || l["class"] != cases[i].class || l["decision"] != "fast" {
			t.Fatalf("line %d: %s", i, line)
		}
		if !bytes.Contains(line, []byte(`"inputs":[`)) || bytes.Index(line, []byte(`"at":`)) > bytes.Index(line, []byte(`"decision":`)) {
			t.Fatalf("line %d is not the payee layout: %s", i, line)
		}
	}
}

func TestRouteAnswersAsTheEngine(t *testing.T) {
	r := newPaidRig(t, "")
	code, body := post(t, r.srv.URL+"/lookup", ask(r.exportQ()))
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	var a struct {
		Outputs []struct {
			Beef        []int  `json:"beef"`
			OutputIndex uint32 `json:"outputIndex"`
			Context     []int  `json:"context"`
		} `json:"outputs"`
	}
	_ = json.Unmarshal(body, &a)
	var outs []answered
	for _, o := range a.Outputs {
		b, c := make([]byte, len(o.Beef)), make([]byte, len(o.Context))
		for i, x := range o.Beef {
			b[i] = byte(x)
		}
		for i, x := range o.Context {
			c[i] = byte(x)
		}
		tx, _ := parse(b)
		outs = append(outs, answered{tx: tx, txid: tx.TxID().String(), vout: o.OutputIndex, beef: b, context: c})
	}
	pg := readPage(t, outs, "", "", true)
	equalStrings(t, pg.keys, []string{"k/1", "k/2", "k/3"}, "export keys")
	// The same bytes as the engine's answer.
	eng := r.outputs(r.exportQ())
	for i := range eng {
		if !bytes.Equal(eng[i].beef, outs[i].beef) || !bytes.Equal(eng[i].context, outs[i].context) {
			t.Fatalf("output %d differs from the engine's", i)
		}
	}
}

// TestLedgerLineGolden holds the module's payment ledger line to the
// TypeScript host's golden file (testdata/host/payments-v2.jsonl): the
// payee layout, byte for byte.
func TestLedgerLineGolden(t *testing.T) {
	raw, err := os.ReadFile("../testdata/host/payments-v2.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var l struct {
		Txid              string   `json:"txid"`
		Beef              string   `json:"beef"`
		OutputIndex       uint32   `json:"outputIndex"`
		Satoshis          uint64   `json:"satoshis"`
		DerivationPrefix  string   `json:"derivationPrefix"`
		DerivationSuffix  string   `json:"derivationSuffix"`
		SenderIdentityKey string   `json:"senderIdentityKey"`
		Class             string   `json:"class"`
		Inputs            []string `json:"inputs"`
		At                int64    `json:"at"`
	}
	if err := json.Unmarshal(raw, &l); err != nil {
		t.Fatal(err)
	}
	beef, _ := base64.StdEncoding.DecodeString(l.Beef)
	p := &termsroute.Received{Txid: l.Txid, Beef: beef, OutputIndex: l.OutputIndex, Satoshis: l.Satoshis, DerivationPrefix: l.DerivationPrefix,
		DerivationSuffix: l.DerivationSuffix, SenderIdentityKey: l.SenderIdentityKey, Class: l.Class, Inputs: l.Inputs}
	line, err := termsroute.LedgerLine(p, l.At, termsroute.LayoutPayee)
	if err != nil || !bytes.Equal(line, raw) {
		t.Fatalf("the module writes\n%s\nthe golden file holds\n%s", line, raw)
	}
	// And the ledger file the module opens writes it so.
	dir := t.TempDir()
	ledger, err := termsroute.OpenLedger(dir, termsroute.LayoutPayee, func() time.Time { return time.Unix(l.At, 0) })
	if err != nil {
		t.Fatal(err)
	}
	if ledger.Claim(p) != termsroute.Accepted {
		t.Fatal("not accepted")
	}
	_ = ledger.Close()
	if got, _ := os.ReadFile(dir + "/" + termsroute.LedgerFile); !bytes.Equal(got, raw) {
		t.Fatalf("the file:\n%s", got)
	}
}
