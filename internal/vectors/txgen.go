package vectors

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// txEnv is everything the transaction vectors are built from: real funding
// trees, carriers, tokens, advertisements, hold outputs and sweeps, so that
// every commitment a record names is a transaction's.
type txEnv struct {
	ctx           context.Context
	o, a, c, b, d *actor // owner, alice (writer), carol (buyer, front), bob (provider), dave (stranger)
	ch            *chain
	coins         map[string]*transaction.Transaction
	next          map[string]uint32

	entryTree, pieceTree, outcomeTree, frontTree, chainTree, misc *transaction.Transaction

	put, del, outcome, front, piece *transaction.Transaction // carriers
	putRec                          []byte
	tokens                          []*transaction.Transaction
	buckets                         []*storerec.Bucket
	ad, hold, renewal, sweep        *transaction.Transaction
	held                            map[transaction.Outpoint]bool
}

func (e *txEnv) fee(a *actor) mint.Input {
	v := e.next[a.p.Name]
	e.next[a.p.Name]++
	coin := e.coins[a.p.Name]
	if int(v) >= len(coin.Outputs) {
		panic("the coin has no output left")
	}
	return mint.Input{Tx: coin, Vout: v, Unlocker: a.feeUnlocker()}
}

func newTxEnv() (*txEnv, error) {
	e := &txEnv{ctx: context.Background(), ch: &chain{height: 800, headers: Headers{}},
		coins: map[string]*transaction.Transaction{}, next: map[string]uint32{}}
	var err error
	for _, x := range []struct {
		dst **actor
		p   Party
	}{{&e.o, Owner}, {&e.a, Alice}, {&e.c, Carol}, {&e.b, Bob}, {&e.d, Dave}} {
		if *x.dst, err = newActor(x.p); err != nil {
			return nil, err
		}
	}
	return e, e.build()
}

// coin is a transaction from nowhere paying an actor's fee key, mined.
func (e *txEnv) coin(a *actor, seed byte) {
	c := transaction.NewTransaction()
	c.AddInput(&transaction.TransactionInput{SourceTXID: fixedHash(seed), UnlockingScript: &script.Script{}, SequenceNumber: transaction.MaxTxInSequenceNum})
	for range 400 {
		c.AddOutput(out(2000000, a.feeLock))
	}
	e.ch.mine(c)
	e.coins[a.p.Name] = c
}

func (e *txEnv) fundingTree(a *actor, d pushdrop.Derivation, n int) (*transaction.Transaction, error) {
	lock, err := carrier.FundingLock(e.ctx, a.w, "", params(d))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(*lock, storerec.FundingScript(a.key(d))) {
		return nil, fmt.Errorf("bcommon's funding lock is not the funding script")
	}
	t, err := mint.FundingTree(lock, n, 1000, e.fee(a), a.feeLock, mint.DefaultFees)
	if err != nil {
		return nil, err
	}
	return e.ch.mine(t), nil
}

func (e *txEnv) mint(a *actor, d pushdrop.Derivation, rec []byte, tree *transaction.Transaction, vout uint32) (*transaction.Transaction, error) {
	return carrier.Mint(e.ctx, a.w, "", params(d), rec, tree, vout)
}

// token builds a bucket token: the lock under bucket of [tag, record],
// signed, spending prev:vout and a fee coin.
func (e *txEnv) token(rec []byte, prev *transaction.Transaction, vout uint32) (*transaction.Transaction, error) {
	return e.mintRaw(e.o.lock(storerec.BucketDerivation, true, storerec.TagToken, rec), 1, prev, vout)
}

func (e *txEnv) mintRaw(lock *script.Script, sats uint64, prev *transaction.Transaction, vout uint32) (*transaction.Transaction, error) {
	var in *mint.Input
	if prev != nil {
		in = &mint.Input{Tx: prev, Vout: vout, Unlocker: e.prevUnlocker(prev, vout)}
	}
	return mint.Transition(lock, sats, in, e.fee(e.o), e.o.feeLock, mint.DefaultFees)
}

func (e *txEnv) prevUnlocker(prev *transaction.Transaction, vout uint32) transaction.UnlockingScriptTemplate {
	s := *prev.Outputs[vout].LockingScript
	if storerec.Classify(s) == storerec.ClaimToken {
		return e.o.unlocker(storerec.BucketDerivation)
	}
	for _, d := range []struct {
		a *actor
		d pushdrop.Derivation
	}{{e.o, storerec.ChainDerivation}, {e.o, storerec.EntryDerivation}, {e.a, storerec.EntryDerivation}} {
		if bytes.Equal(s, storerec.FundingScript(d.a.key(d.d))) {
			return d.a.unlocker(d.d)
		}
	}
	return e.o.feeUnlocker()
}

// adTx is a mined transaction of advertisements and hold outputs, funded by
// a fee coin of a.
func (e *txEnv) adTx(a *actor, outs ...*transaction.TransactionOutput) *transaction.Transaction {
	t := transaction.NewTransaction()
	f := e.fee(a)
	t.AddInputFromTx(f.Tx, f.Vout, a.feeUnlocker())
	for _, o := range outs {
		t.AddOutput(o)
	}
	t.AddOutput(out(1000000, a.feeLock))
	return signAll(t)
}

func (e *txEnv) bucketRecord(seq uint64, prev [32]byte, id [32]byte, m storerec.Map, changes []storerec.Change, consumed [][32]byte) *storerec.Bucket {
	if consumed == nil {
		consumed = [][32]byte{}
	}
	if changes == nil {
		changes = []storerec.Change{}
	}
	w := [][]byte{Alice.Pub()}
	return &storerec.Bucket{Bucket: Bucket, Owner: Owner.Pub(), ID: id, Seq: seq, Prev: prev, Writers: w, Changes: changes,
		Count: uint64(len(m)), MapRoot: m.Root(), Created: Created + seq, Consumed: consumed}
}

func (e *txEnv) build() error {
	for i, a := range []*actor{e.o, e.a, e.c, e.b, e.d} {
		e.coin(a, byte(0x11+i))
	}
	var err error
	if e.entryTree, err = e.fundingTree(e.a, storerec.EntryDerivation, 16); err != nil {
		return err
	}
	if e.pieceTree, err = e.fundingTree(e.a, storerec.PieceDerivation, 4); err != nil {
		return err
	}
	if e.outcomeTree, err = e.fundingTree(e.c, storerec.OutcomeDerivation, 4); err != nil {
		return err
	}
	if e.frontTree, err = e.fundingTree(e.c, storerec.FrontDerivation, 4); err != nil {
		return err
	}
	if e.chainTree, err = e.fundingTree(e.o, storerec.ChainDerivation, 6); err != nil {
		return err
	}
	// misc: outputs an entry carrier must not be funded from: a bare
	// pay-to-public-key under alice's entry key, the funding tag under it
	// with a field signature, another tag under it, funding outputs under
	// her outcome, piece and chain keys.
	e.misc = transaction.NewTransaction()
	f := e.fee(e.a)
	e.misc.AddInputFromTx(f.Tx, f.Vout, e.a.feeUnlocker())
	ek := e.a.key(storerec.EntryDerivation)
	bare := script.Script(append(append([]byte{33}, ek.Compressed()...), script.OpCHECKSIG))
	e.misc.AddOutput(out(1000, &bare))
	e.misc.AddOutput(out(1000, e.a.lock(storerec.EntryDerivation, true, storerec.TagFunding)))
	e.misc.AddOutput(out(1000, e.a.lock(storerec.EntryDerivation, false, storerec.TagHold)))
	e.misc.AddOutput(out(1000, scr(storerec.FundingScript(e.a.key(storerec.OutcomeDerivation)))))
	e.misc.AddOutput(out(1000, scr(storerec.FundingScript(e.a.key(storerec.PieceDerivation)))))
	e.misc.AddOutput(out(1000, scr(storerec.FundingScript(e.a.key(storerec.ChainDerivation)))))
	e.misc.AddOutput(out(1990000, e.a.feeLock))
	e.ch.mine(signAll(e.misc))

	// The carriers.
	e.putRec = must(samplePut().Encode())
	if e.put, err = e.mint(e.a, storerec.EntryDerivation, e.putRec, e.entryTree, 0); err != nil {
		return err
	}
	if e.del, err = e.mint(e.a, storerec.EntryDerivation, must(sampleDelete().Encode()), e.entryTree, 1); err != nil {
		return err
	}
	if e.outcome, err = e.mint(e.c, storerec.OutcomeDerivation, must(sampleOutcome(storerec.OutcomePass).Encode()), e.outcomeTree, 0); err != nil {
		return err
	}
	if e.front, err = e.mint(e.c, storerec.FrontDerivation, must(sampleFront().Encode()), e.frontTree, 0); err != nil {
		return err
	}
	if e.piece, err = e.mint(e.a, storerec.PieceDerivation, must(samplePiece().Encode()), e.pieceTree, 0); err != nil {
		return err
	}

	// The bucket chain: its id is the hash of chain-tree output 0.
	id := storerec.BucketID([32]byte(*e.chainTree.TxID()), 0)
	b0 := e.bucketRecord(0, [32]byte{}, id, storerec.Map{}, nil, nil)
	b0.Writers = [][]byte{}
	t0, err := e.token(must(b0.Encode()), e.chainTree, 0)
	if err != nil {
		return err
	}
	e.ch.mine(t0)
	key := samplePut().Key
	cPut, cDel := carrier.Commitment(e.put), carrier.Commitment(e.del)
	m1 := storerec.Map{key: cPut}
	b1 := e.bucketRecord(1, [32]byte(*t0.TxID()), id, m1, []storerec.Change{{Key: key, C: cPut}}, nil)
	t1, err := e.token(must(b1.Encode()), t0, 0)
	if err != nil {
		return err
	}
	b2 := e.bucketRecord(2, [32]byte(*t1.TxID()), id, storerec.Map{}, []storerec.Change{{Key: key, C: cDel, Removal: true}}, nil)
	t2, err := e.token(must(b2.Encode()), t1, 0)
	if err != nil {
		return err
	}
	e.ch.mineTogether(t1, t2)
	e.tokens = []*transaction.Transaction{t0, t1, t2}
	e.buckets = []*storerec.Bucket{b0, b1, b2}
	fold, err := storerec.FoldChain(e.buckets)
	if err != nil {
		return err
	}
	for i, s := range fold.Statuses {
		if s != storerec.StatusConsistent {
			return fmt.Errorf("mined token seq %d folds %s", i, s)
		}
	}

	// Bob's advertisement of the sample root, alice's hold output, and
	// Bob's renewal of the advertisement with a later expiry.
	root := contentBuilt.RootHash
	e.ad = e.ch.mine(e.adTx(e.b, out(1, scr(adScript(Bob, root, adURL(root), 1900000000, uint64(len(contentBuilt.Root)))))))
	e.hold = e.ch.mine(e.adTx(e.a, out(10000, scr(storerec.HoldScript(e.a.key(storerec.HoldDerivation))))))
	r := transaction.NewTransaction()
	r.AddInputFromTx(e.ad, 0, transaction.UnlockingScriptTemplate(adUnlocker{e.b}))
	fr := e.fee(e.b)
	r.AddInputFromTx(fr.Tx, fr.Vout, e.b.feeUnlocker())
	r.AddOutput(out(1, scr(adScript(Bob, root, adURL(root), 1950000000, uint64(len(contentBuilt.Root))))))
	r.AddOutput(out(1000000, e.b.feeLock))
	e.renewal = e.ch.mine(signAll(r))

	// The sweep: entry-tree outputs 1 (the delete's funding outpoint) and 15
	// (unused).
	if e.sweep, err = carrier.Sweep(e.ctx, e.a.w, "", params(storerec.EntryDerivation), e.entryTree, []uint32{1, 15}, nil, 0, nil, e.a.feeLock, 1, 250); err != nil {
		return err
	}
	e.ch.mine(e.sweep)

	e.held = map[transaction.Outpoint]bool{{Txid: *e.sweep.TxID(), Index: 0}: true, {Txid: *e.ad.TxID(), Index: 0}: true}
	for _, k := range []*transaction.Transaction{e.put, e.del, e.outcome, e.front} {
		e.held[transaction.Outpoint{Txid: *k.TxID(), Index: 0}] = true
	}
	for _, t := range e.tokens {
		e.held[transaction.Outpoint{Txid: *t.TxID(), Index: 0}] = true
	}
	return nil
}

// adUnlocker spends an advertisement output: a signature under the
// advertisement key (counterparty anyone).
type adUnlocker struct{ a *actor }

func (u adUnlocker) Sign(tx *transaction.Transaction, i uint32) (*script.Script, error) {
	return flagUnlocker{key: adPriv(u.a.p), flag: 0x41}.Sign(tx, i)
}

func (u adUnlocker) EstimateLength(*transaction.Transaction, uint32) uint32 { return 74 }
