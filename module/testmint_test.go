package module

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/storerec"
)

// The test minter: fresh bstore objects the golden vectors do not hold
// (funding trees, bucket tokens, advertisements, hold outputs and sweeps
// mined on a local chain; entry, outcome and front carriers), the twin of
// the TypeScript module's testmint.ts. ownerSim keeps a bucket chain and
// its map and anchors entries the way spec section 3.4 says the owner's
// tooling does, or, on request, the way it says not to. Every key is drawn
// from a fixed label, so a party is the same on every run.

func sha(s string) [32]byte { return sha256.Sum256([]byte(s)) }

// T0 is the Unix milliseconds the tests write as created by default.
const T0 uint64 = 1_798_761_600_000

// testChain is a local chain: blocks of two, each a minted transaction
// beside a fixed sibling.
type testChain struct {
	mu     sync.Mutex
	roots  map[uint32]chainhash.Hash
	height uint32
}

func newTestChain(first uint32, extra map[uint32]chainhash.Hash) *testChain {
	c := &testChain{roots: map[uint32]chainhash.Hash{}, height: first}
	for h, r := range extra {
		c.roots[h] = r
	}
	return c
}

var sibling = func() *chainhash.Hash { h := chainhash.Hash(bytes.Repeat([]byte{0x55}, 32)); return &h }()

func (c *testChain) mine(tx *transaction.Transaction) *transaction.Transaction {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.height
	c.height++
	t := true
	tx.MerklePath = transaction.NewMerklePath(h, [][]*transaction.PathElement{{
		{Offset: 0, Hash: sibling},
		{Offset: 1, Hash: tx.TxID(), Txid: &t},
	}})
	root, err := tx.MerklePath.ComputeRoot(tx.TxID())
	if err != nil {
		panic(err)
	}
	c.roots[h] = *root
	return tx
}

// orphan takes the block at height out of the best chain.
func (c *testChain) orphan(h uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.roots, h)
}

func (c *testChain) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, h uint32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.roots[h]
	return ok && root != nil && r == *root, nil
}

func (c *testChain) CurrentHeight(context.Context) (uint32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var top uint32
	for h := range c.roots {
		top = max(top, h)
	}
	return top, nil
}

// party is one identity: its key, its wallet and its compressed public key.
type party struct {
	label  string
	key    *ec.PrivateKey
	id     []byte
	hex    string
	wallet wallet.Interface
}

func newParty(label string) *party {
	k := sha("bstore/test/party/" + label)
	priv, _ := ec.PrivateKeyFromBytes(k[:])
	w, err := wallet.NewCompletedProtoWallet(priv)
	if err != nil {
		panic(err)
	}
	id := priv.PubKey().Compressed()
	return &party{label: label, key: priv, id: id, hex: hex.EncodeToString(id), wallet: w}
}

func (p *party) derived(d pushdrop.Derivation) *ec.PublicKey {
	k, err := storerec.LockingKey(d, p.id)
	if err != nil {
		panic(err)
	}
	return k
}

func (p *party) lock(d pushdrop.Derivation, fields ...[]byte) *script.Script {
	s, err := d.Lock(context.Background(), p.wallet, "", fields, true)
	if err != nil {
		panic(err)
	}
	return s
}

func (p *party) unlocker(d pushdrop.Derivation) transaction.UnlockingScriptTemplate {
	return d.Unlocker(context.Background(), p.wallet, "")
}

func (p *party) derivedPriv(d pushdrop.Derivation) *ec.PrivateKey {
	k, err := wallet.NewKeyDeriver(p.key).DerivePrivateKey(d.Protocol, d.KeyID, pushdrop.Anyone())
	if err != nil {
		panic(err)
	}
	return k
}

func scr(b []byte) *script.Script { s := script.Script(b); return &s }

func (p *party) p2pkh() *script.Script {
	addr, _ := script.NewAddressFromPublicKey(p.key.PubKey(), true)
	s, _ := p2pkh.Lock(addr)
	return s
}

var nowhere atomic.Int64

// fromNowhere is a transaction from nowhere: one unsigned input of an
// outpoint no one holds.
func fromNowhere(outs ...*transaction.TransactionOutput) *transaction.Transaction {
	src := sha(fmt.Sprintf("bstore/test/nowhere/%d", nowhere.Add(1)))
	h := chainhash.Hash(src)
	tx := transaction.NewTransaction()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &h, SourceTxOutIndex: 0, UnlockingScript: &script.Script{}, SequenceNumber: 0xffffffff})
	for _, o := range outs {
		tx.AddOutput(o)
	}
	return tx
}

func out(sats uint64, s *script.Script) *transaction.TransactionOutput {
	return &transaction.TransactionOutput{Satoshis: sats, LockingScript: s}
}

func signed(tx *transaction.Transaction) *transaction.Transaction {
	if err := tx.Sign(); err != nil {
		panic(err)
	}
	return tx
}

// fundingTree is a mined tree of n outputs of 1000 satoshis, locked to
// derive(owner, d).
func fundingTree(owner *party, d pushdrop.Derivation, n int, c *testChain) *transaction.Transaction {
	lock := scr(storerec.FundingScript(owner.derived(d)))
	outs := make([]*transaction.TransactionOutput, n)
	for i := range outs {
		outs[i] = out(1000, lock)
	}
	return c.mine(fromNowhere(outs...))
}

// mintCarrier is a carrier of record spending output vout of owner's tree,
// locked and signed under d.
func mintCarrier(tree *transaction.Transaction, vout uint32, owner *party, d pushdrop.Derivation, record []byte) *transaction.Transaction {
	tx := transaction.NewTransaction()
	tx.LockTime = carrier.LockTime
	tx.AddInputFromTx(tree, vout, owner.unlocker(d))
	tx.Inputs[0].SequenceNumber = carrier.Sequence
	tx.AddOutput(out(tree.Outputs[vout].Satoshis, owner.lock(d, record)))
	return signed(tx)
}

// mintSweep is a mined sweep of outputs vouts of owner's tree: its output 0
// funding-shaped under d.
func mintSweep(tree *transaction.Transaction, vouts []uint32, owner *party, d pushdrop.Derivation, c *testChain) *transaction.Transaction {
	tx := transaction.NewTransaction()
	var value uint64
	for _, v := range vouts {
		tx.AddInputFromTx(tree, v, owner.unlocker(d))
		value += tree.Outputs[v].Satoshis
	}
	tx.AddOutput(out(value, scr(storerec.FundingScript(owner.derived(d)))))
	return c.mine(signed(tx))
}

func commitment(tx *transaction.Transaction) [32]byte { return [32]byte(*tx.TxID()) }
func cHex(tx *transaction.Transaction) string         { c := commitment(tx); return hex.EncodeToString(c[:]) }

// posted is a carrier a party made: the record, its carrier and its funding
// outpoint.
type posted struct {
	tx     *transaction.Transaction
	txid   string
	c      string
	record []byte
	vout   uint32
	tree   *transaction.Transaction
	op     string
	e      *storerec.Entry
}

type tree struct {
	tx   *transaction.Transaction
	next uint32
}

// author is a party with funding trees of its own, one per derivation.
type author struct {
	bucket string
	party  *party
	chain  *testChain
	trees  map[string]*tree
	size   int
}

func newAuthor(bucket, label string, c *testChain) *author {
	return &author{bucket: bucket, party: newParty(label), chain: c, trees: map[string]*tree{}, size: 512}
}

func (a *author) hex() string { return a.party.hex }
func (a *author) id() []byte  { return a.party.id }

func (a *author) tree(d pushdrop.Derivation) *tree {
	t := a.trees[d.KeyID]
	if t == nil {
		t = &tree{tx: fundingTree(a.party, d, a.size, a.chain)}
		a.trees[d.KeyID] = t
	}
	return t
}

// post is a carrier of record under d, on the next unused output of the
// tree, or on output vout when it is at or above zero.
func (a *author) post(d pushdrop.Derivation, record []byte, vout int) *posted {
	t := a.tree(d)
	at := t.next
	if vout >= 0 {
		at = uint32(vout)
	} else {
		t.next++
	}
	tx := mintCarrier(t.tx, at, a.party, d, record)
	return &posted{tx: tx, txid: tx.TxID().String(), c: cHex(tx), record: record, vout: at, tree: t.tx, op: outpoint(t.tx.TxID().String(), at)}
}

type objectID struct {
	root, contentHash, proofRoot [32]byte
	size                         uint64
}

func object(label string) objectID {
	return objectID{root: sha("root/" + label), contentHash: sha("content/" + label), proofRoot: sha("proof/" + label), size: 1000}
}

type putOpts struct {
	label   string
	created uint64
	hold    *storerec.Hold
	// vout is the funding output to spend; the next unused when nil.
	vout *uint32
}

func (a *author) put(id [32]byte, key string, o putOpts) *posted {
	label := o.label
	if label == "" {
		label = key
	}
	created := o.created
	if created == 0 {
		created = T0
	}
	obj := object(label)
	e := &storerec.Entry{Bucket: a.bucket, ID: id, Writer: a.id(), Key: key, Kind: storerec.KindPut, Created: created, Size: obj.size, Root: obj.root,
		ContentHash: obj.contentHash, ProofRoot: obj.proofRoot, ProofProfile: 1, ContentType: "application/octet-stream", Hold: o.hold}
	rec, err := e.Encode()
	if err != nil {
		panic(err)
	}
	vout := -1
	if o.vout != nil {
		vout = int(*o.vout)
	}
	p := a.post(storerec.EntryDerivation, rec, vout)
	p.e = e
	return p
}

func ptr[T any](v T) *T { return &v }

func (a *author) del(id [32]byte, key string, created uint64) *posted {
	if created == 0 {
		created = T0
	}
	e := &storerec.Entry{Bucket: a.bucket, ID: id, Writer: a.id(), Key: key, Kind: storerec.KindDelete, Created: created}
	rec, err := e.Encode()
	if err != nil {
		panic(err)
	}
	p := a.post(storerec.EntryDerivation, rec, -1)
	p.e = e
	return p
}

type held struct {
	tx    *transaction.Transaction
	terms *storerec.Hold
	op    string
}

// hold is a mined hold output of sats, and the terms an entry names it by.
func (a *author) hold(sats uint64) held {
	tx := a.chain.mine(fromNowhere(out(sats, scr(storerec.HoldScript(a.party.derived(storerec.HoldDerivation)))), out(1000, a.party.p2pkh())))
	terms := &storerec.Hold{Txid: commitment(tx), Vout: 0, Satoshis: sats, T: 1, K: 2, Retention: 86400, Deadline: 1_000_000, CheckC: 30}
	return held{tx: tx, terms: terms, op: outpoint(tx.TxID().String(), 0)}
}

type outcomeOpts struct {
	root     [32]byte
	provider *party
	answerer *party
	outcome  uint64
	created  uint64
}

// outcome is a buyer's outcome record of a challenge of provider, answered
// by answerer.
func (a *author) outcome(o outcomeOpts) *posted {
	answerer := o.answerer
	if answerer == nil {
		answerer = o.provider
	}
	var nonce [16]byte
	n := sha("nonce")
	copy(nonce[:], n[:16])
	q := &storerec.Request{Root: o.root, ProofRoot: sha("proof"), Size: 1000, Provider: o.provider.id, Answerer: answerer.id, SeedHeight: 100, SeedHash: sha("seed"), Nonce: nonce, C: 30}
	request, err := q.Encode()
	if err != nil {
		panic(err)
	}
	outcome := o.outcome
	if outcome == 0 {
		outcome = 3
	}
	created := o.created
	if created == 0 {
		created = T0
	}
	rec := &storerec.Outcome{Bucket: a.bucket, Buyer: a.id(), Created: created, Request: request, Req: q, Outcome: outcome}
	if outcome == storerec.OutcomePass || outcome == 2 {
		resp := sha("response")
		rec.Response = &resp
		d := storerec.SignedDigest(sha256.Sum256(request), resp)
		sig, err := answerer.derivedPriv(storerec.ChallengeDerivation).Sign(d[:])
		if err != nil {
			panic(err)
		}
		rec.Signature = sig.Serialize()
	}
	raw, err := rec.Encode()
	if err != nil {
		panic(err)
	}
	return a.post(storerec.OutcomeDerivation, raw, -1)
}

// statement is a front's statement that it answers for advertiser.
func (a *author) statement(advertiser []byte, created uint64) *posted {
	if created == 0 {
		created = T0
	}
	f := &storerec.Front{Bucket: a.bucket, Front: a.id(), Advertiser: advertiser, URL: "https://front.example/bstore/v1", Created: created}
	raw, err := f.Encode()
	if err != nil {
		panic(err)
	}
	return a.post(storerec.FrontDerivation, raw, -1)
}

// provider makes BRC-26 advertisements of roots, mined, and their renewals.
type provider struct {
	party *party
	chain *testChain
}

func newProvider(label string, c *testChain) *provider {
	return &provider{party: newParty(label), chain: c}
}

func (p *provider) hex() string { return p.party.hex }

func (p *provider) adScript(root [32]byte, expiry uint64) *script.Script {
	host := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' {
			return r
		}
		return -1
	}, p.party.label)
	key := p.party.derivedPriv(storerec.AdDerivation)
	f, err := storerec.AdvertisementFields(p.party.id, key, root, "https://"+host+".example"+storerec.CHIRPURLSuffix(root), expiry, 100)
	if err != nil {
		panic(err)
	}
	return scr(storerec.AdvertisementScript(key.PubKey(), f))
}

// advertise is a mined transaction of advertisements of roots, expiring at
// expiry (Unix seconds).
func (p *provider) advertise(roots [][32]byte, expiry uint64) *transaction.Transaction {
	var outs []*transaction.TransactionOutput
	for _, r := range roots {
		outs = append(outs, out(1, p.adScript(r, expiry)))
	}
	outs = append(outs, out(1000, p.party.p2pkh()))
	return p.chain.mine(fromNowhere(outs...))
}

type adUnlocker struct{ key *ec.PrivateKey }

func (u adUnlocker) Sign(tx *transaction.Transaction, i uint32) (*script.Script, error) {
	d, err := tx.CalcInputSignatureHash(i, 0x41)
	if err != nil {
		return nil, err
	}
	sig, err := u.key.Sign(d)
	if err != nil {
		return nil, err
	}
	s := &script.Script{}
	if err := s.AppendPushData(append(sig.Serialize(), 0x41)); err != nil {
		return nil, err
	}
	return s, nil
}

func (u adUnlocker) EstimateLength(*transaction.Transaction, uint32) uint32 { return 74 }

// renew spends advertisement vout of ad into a new one of the same root
// with a later expiry.
func (p *provider) renew(ad *transaction.Transaction, vout uint32, root [32]byte, expiry uint64) *transaction.Transaction {
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(ad, vout, adUnlocker{p.party.derivedPriv(storerec.AdDerivation)})
	tx.AddOutput(out(1, p.adScript(root, expiry)))
	return p.chain.mine(signed(tx))
}

// minted is a mined token and the BEEF it is published in.
type minted struct {
	tx     *transaction.Transaction
	txid   string
	beef   []byte
	rec    *storerec.Bucket
	parent *transaction.Transaction
}

// ownerSim is a bucket's owner tooling over a local chain: its chain tree,
// one chain, and the map it anchors.
type ownerSim struct {
	bucket    string
	party     *party
	chain     *testChain
	chainTree *transaction.Transaction
	chainVout uint32
	id        [32]byte
	tip       *minted
	history   []*minted
	m         storerec.Map
	named     map[[32]byte]bool
	writers   [][]byte
}

func newOwner(bucket, label string, c *testChain) *ownerSim {
	p := newParty(label)
	t := fundingTree(p, storerec.ChainDerivation, 8, c)
	return &ownerSim{bucket: bucket, party: p, chain: c, chainTree: t, id: storerec.BucketID(commitment(t), 0), m: storerec.Map{}, named: map[[32]byte]bool{}}
}

func (o *ownerSim) hex() string   { return o.party.hex }
func (o *ownerSim) idHex() string { return hex.EncodeToString(o.id[:]) }

// served is this chain's triple as BSTORE_SERVED holds it after parsing.
func (o *ownerSim) served() string { return o.bucket + "|" + o.hex() + "|" + o.idHex() }

// fork is the same chain at the same tip, to build a competing token on.
func (o *ownerSim) fork() *ownerSim {
	f := *o
	f.history = append([]*minted(nil), o.history...)
	f.m = o.m.Clone()
	f.named = map[[32]byte]bool{}
	for k, v := range o.named {
		f.named[k] = v
	}
	f.writers = append([][]byte(nil), o.writers...)
	return &f
}

type tokenOpts struct {
	anchor  []*posted
	writers [][]byte
	mutate  func(*storerec.Bucket)
	unmined bool
	created uint64
}

// token is the next token, mined unless unmined: it anchors entries as the
// owner's tooling does (each named once, a batch resolved by created then
// commitment, no-ops consumed), names writers, and mutate may change the
// record before it is encoded.
func (o *ownerSim) token(opts tokenOpts) *minted {
	var seq uint64
	if o.tip != nil {
		seq = o.tip.rec.Seq + 1
	}
	if opts.writers != nil {
		w := append([][]byte(nil), opts.writers...)
		sort.Slice(w, func(i, j int) bool { return bytes.Compare(w[i], w[j]) < 0 })
		o.writers = w
	}
	changes := []storerec.Change{}
	consumed := [][32]byte{}
	next := o.m.Clone()
	named := map[[32]byte]bool{}
	for k, v := range o.named {
		named[k] = v
	}
	if seq > 0 {
		var batch []storerec.Applicable
		for _, p := range opts.anchor {
			batch = append(batch, storerec.Applicable{C: commitment(p.tx), Key: p.e.Key, Kind: p.e.Kind, Created: p.e.Created})
		}
		ch, cs := storerec.ResolveBatch(o.m, o.named, batch)
		if ch != nil {
			changes = ch
		}
		if cs != nil {
			consumed = cs
		}
		for _, c := range changes {
			named[c.C] = true
			if c.Removal {
				delete(next, c.Key)
			} else {
				next[c.Key] = c.C
			}
		}
		for _, c := range consumed {
			named[c] = true
		}
	}
	created := opts.created
	if created == 0 {
		created = T0 + seq*600_000
	}
	rec := &storerec.Bucket{Bucket: o.bucket, Owner: o.party.id, ID: o.id, Seq: seq, Writers: [][]byte{}, Changes: changes, Count: uint64(len(next)),
		MapRoot: next.Root(), Created: created, Consumed: consumed}
	if o.tip != nil {
		rec.Prev = commitment(o.tip.tx)
	}
	if seq > 0 && o.writers != nil {
		rec.Writers = o.writers
	}
	if opts.mutate != nil {
		opts.mutate(rec)
	}
	raw, err := rec.Encode()
	if err != nil {
		panic(err)
	}
	src, vout, d := o.chainTree, o.chainVout, storerec.ChainDerivation
	if o.tip != nil {
		src, vout, d = o.tip.tx, 0, storerec.BucketDerivation
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(src, vout, o.party.unlocker(d))
	tx.AddOutput(out(1, o.party.lock(storerec.BucketDerivation, storerec.TagToken, raw)))
	if total := src.Outputs[vout].Satoshis; total > 1 {
		tx.AddOutput(out(total-1, o.party.p2pkh()))
	}
	signed(tx)
	m := &minted{tx: tx, txid: tx.TxID().String(), rec: rec, parent: src}
	if !opts.unmined {
		o.chain.mine(tx)
		if m.beef, err = storerec.TokenBEEF(tx, src); err != nil {
			panic(err)
		}
	}
	o.tip = m
	o.history = append(o.history, m)
	if opts.mutate == nil {
		o.m, o.named = next, named
	}
	return m
}

func atomicOf(t *testing.T, tx *transaction.Transaction) []byte {
	t.Helper()
	b, err := tx.AtomicBEEF(false)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
