package vectors

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

type admitJSON struct {
	Kind        string   `json:"kind"`
	Outputs     []int    `json:"outputsToAdmit"`
	Retain      []int    `json:"coinsToRetain"`
	Commitment  string   `json:"commitment,omitempty"`
	Record      string   `json:"record,omitempty"`
	FundingTxid string   `json:"fundingTxid,omitempty"`
	FundingVout *int     `json:"fundingVout,omitempty"`
	Seq         *uint64  `json:"seq,omitempty"`
	Parent      string   `json:"parentTxid,omitempty"`
	PredSeq     *uint64  `json:"predecessorSeq,omitempty"`
	Spends      string   `json:"spends,omitempty"`
	Spent       []string `json:"spent,omitempty"`
	Ads         []int    `json:"advertisements,omitempty"`
	Holds       []int    `json:"holds,omitempty"`
}

type txJSON struct {
	Name           string     `json:"name"`
	Note           string     `json:"note"`
	Txid           string     `json:"txid"`
	BEEF           string     `json:"beef"`
	BEEFFill       *fill      `json:"beefFill,omitempty"`
	BEEFLength     int        `json:"beefLength"`
	Held           []int      `json:"previousCoins"`
	MaxSweepInputs int        `json:"maxSweepInputs,omitempty"`
	Scripts        bool       `json:"scriptsVerify"`
	Verdict        string     `json:"verdict"`
	Reason         string     `json:"reason,omitempty"`
	Admits         *admitJSON `json:"admits,omitempty"`
}

type txCase struct {
	name, note string
	tx         *transaction.Transaction
	beef       []byte
	reason     string
	held       []int
	noScripts  bool
}

func (e *txEnv) heldInputs(tx *transaction.Transaction) []int {
	held := []int{}
	for i, in := range tx.Inputs {
		if e.held[transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}] {
			held = append(held, i)
		}
	}
	return held
}

func outpointString(o transaction.Outpoint) string {
	return fmt.Sprintf("%s.%d", o.Txid.String(), o.Index)
}

func admission(a *storerec.Admission) *admitJSON {
	j := &admitJSON{Kind: string(a.Kind), Outputs: append([]int{}, a.Outputs...), Retain: append([]int{}, a.Retain...)}
	if c := a.Carrier; c != nil {
		v := int(c.Funding.Index)
		j.Commitment, j.Record, j.FundingTxid, j.FundingVout = hx(c.Commitment[:]), hx(c.Record), c.Funding.Txid.String(), &v
	}
	if t := a.Token; t != nil {
		seq := t.Bucket.Seq
		j.Seq, j.Record, j.Parent, j.Spends = &seq, hx(t.Record), displayHex(a.Parent), outpointString(a.Spends)
		if a.Predecessor != nil {
			p := a.Predecessor.Bucket.Seq
			j.PredSeq = &p
		}
	}
	for _, o := range a.Spent {
		j.Spent = append(j.Spent, outpointString(o))
	}
	for i := range a.Outputs {
		if _, ok := a.Ads[a.Outputs[i]]; ok {
			j.Ads = append(j.Ads, a.Outputs[i])
		}
	}
	j.Holds = a.Holds
	return j
}

// fat is a mined transaction paying a fee coin of a to the given outputs and
// a data output of size bytes, each fill.
func (e *txEnv) fat(a *actor, size int, fill byte, outs ...*transaction.TransactionOutput) *transaction.Transaction {
	t := transaction.NewTransaction()
	f := e.fee(a)
	t.AddInputFromTx(f.Tx, f.Vout, a.feeUnlocker())
	for _, o := range outs {
		t.AddOutput(o)
	}
	data := &script.Script{script.OpFALSE, script.OpRETURN}
	if err := data.AppendPushData(bytes.Repeat([]byte{fill}, size)); err != nil {
		panic(err)
	}
	t.AddOutput(out(0, data))
	return e.ch.mine(signAll(t))
}

func mutate(rec []byte, k uint64, v cbor.Value) []byte {
	m := must(cbor.DecodeValue(rec)).(cbor.Map)
	return enc(with(m, k, v))
}

func (e *txEnv) cases() ([]txCase, error) {
	a, o, c := e.a, e.o, e.c
	ed := storerec.EntryDerivation
	k1 := e.put
	lock1 := k1.Outputs[0].LockingScript
	rec1 := e.putRec
	eUnlock := a.unlocker(ed)
	ek := a.key(ed)
	var cs []txCase
	add := func(x txCase) { cs = append(cs, x) }
	t0, t1, t2 := e.tokens[0], e.tokens[1], e.tokens[2]

	// ---- admitted ----
	add(txCase{name: "entry-put", note: "alice's put on entry-tree output 0, as Atomic BEEF", tx: e.put})
	add(txCase{name: "entry-delete", note: "alice's delete on entry-tree output 1", tx: e.del})
	add(txCase{name: "outcome-pass", note: "carol's pass outcome, the answerer's signature checked at admission", tx: e.outcome})
	add(txCase{name: "front", note: "carol's front statement for bob", tx: e.front})
	v1 := must(k1.BEEF())
	add(txCase{name: "entry-beef-v1", note: "entry-put as BEEF V1 (BRC-62)", tx: k1, beef: v1})
	add(txCase{name: "token-first", note: "seq 0, spending chain-tree output 0, whose outpoint hashes to the bucket id", tx: t0, beef: tokenBEEF(t0, e.chainTree)})
	add(txCase{name: "token-put", note: "seq 1 puts alice's entry by its commitment; it spends seq 0, which the topic holds and retains", tx: t1, beef: tokenBEEF(t1, t0)})
	add(txCase{name: "token-put-predecessor-not-held", note: "token-put with no previous coins: admitted all the same", tx: t1, beef: tokenBEEF(t1, t0), held: []int{}})
	add(txCase{name: "token-removal-same-block", note: "seq 2 removes the key naming the delete; mined in its parent's block, one merged path", tx: t2, beef: tokenBEEF(t2, t1)})
	add(txCase{name: "token-v2", note: "token-put as BEEF V2 (BRC-96)", tx: t1,
		beef: beefV2([]*transaction.MerklePath{t0.MerklePath, t1.MerklePath}, []*transaction.Transaction{t0, t1}, []int{0, 1}, nil)})
	add(txCase{name: "advertisement", note: "bob's advertisement of the sample root", tx: e.ad})
	add(txCase{name: "hold-output", note: "alice's hold output of 10,000 satoshis", tx: e.hold})
	add(txCase{name: "advertisement-renewal", note: "bob's renewal: spends the advertisement the topic holds (retained) and recreates it with a later expiry", tx: e.renewal})
	add(txCase{name: "sweep", note: "alice's mined sweep of entry-tree outputs 1 (the delete's funding outpoint) and 15", tx: e.sweep})
	add(txCase{name: "funding-tree-alone", note: "alice's entry tree, mined, published alone: it claims nothing and output 0 is funding-shaped, a sweep with more than two outputs is not one: refused", tx: e.entryTree, reason: "not-bstore"})
	root := contentBuilt.RootHash
	adOut := func(p Party, expiry uint64) *transaction.TransactionOutput {
		return out(1, scr(adScript(p, root, adURL(root), expiry, uint64(len(contentBuilt.Root)))))
	}
	holdOut := func(v uint64) *transaction.TransactionOutput {
		return out(v, scr(storerec.HoldScript(a.key(storerec.HoldDerivation))))
	}
	mixed := e.ch.mine(e.adTx(e.b, adOut(Bob, 1900000001), holdOut(500)))
	add(txCase{name: "advertisement-and-hold", note: "an advertisement and a hold output in one mined transaction: both admitted", tx: mixed})
	var sixteen, seventeen []*transaction.TransactionOutput
	for i := range 17 {
		o := holdOut(uint64(100 + i))
		if i%2 == 0 {
			o = adOut(Bob, uint64(1900000100+i))
		}
		if i < 16 {
			sixteen = append(sixteen, o)
		}
		seventeen = append(seventeen, o)
	}
	add(txCase{name: "mined-16", note: "sixteen advertisements and hold outputs in one mined transaction", tx: e.ch.mine(e.adTx(e.b, sixteen...))})
	add(txCase{name: "mined-17", note: "seventeen", tx: e.ch.mine(e.adTx(e.b, seventeen...)), reason: "too-many"})
	anyoneAd := e.ch.mine(e.adTx(e.b, out(1, scr(adScript(Party{Name: "anyone", Priv: anyonePriv()}, root, adURL(root), 1900000000, 100)))))
	add(txCase{name: "advertisement-anyone-key", note: "an advertisement by the anyone key: the SDK admits it, so the host does; bstore never counts it", tx: anyoneAd})
	badAd := adScript(Bob, root, adURL(root), 1900000000, 100)
	badAd[len(badAd)-5] ^= 1
	add(txCase{name: "advertisement-bad-signature", note: "an advertisement whose signature does not verify: not an advertisement, so the transaction claims nothing", tx: e.ch.mine(e.adTx(e.b, out(1, scr(badAd)))), reason: "not-bstore"})
	unminedAd := reproved(e.ad, transaction.NewMerklePath(unproven, e.ad.MerklePath.Path))
	add(txCase{name: "advertisement-unmined", note: "the advertisement under a proof at a height the headers do not hold", tx: unminedAd, reason: "unmined"})
	add(txCase{name: "advertisement-beef-with-parent", note: "the advertisement's BEEF also carrying its funding coin", tx: e.ad,
		beef: beefV1([]*transaction.MerklePath{e.coins["bob"].MerklePath, e.ad.MerklePath}, []*transaction.Transaction{e.coins["bob"], e.ad}, []int{0, 1}), reason: "beef"})
	for _, x := range []struct {
		name, note, reason string
		size               int
	}{
		{"mined-beef-at-ceiling", "an advertisement in a mined transaction carrying a data output, its BEEF exactly 65,536 bytes", "", storerec.MaxMinedBEEF},
		{"mined-beef-over-ceiling", "the same at 65,537 bytes", "beef", storerec.MaxMinedBEEF + 1},
	} {
		t, err := fitBEEF(x.size, func(d int, v byte) (*transaction.Transaction, []byte) {
			t := e.fat(e.b, d, v, adOut(Bob, 1900000002))
			return t, mustAtomic(t)
		})
		if err != nil {
			return nil, err
		}
		add(txCase{name: x.name, note: x.note, tx: t, reason: x.reason})
	}

	// ---- carriers: shape, the classifier ----
	two := carrierFrom(e.entryTree, 11, eUnlock, out(1000, lock1))
	two.AddInputFromTx(e.entryTree, 12, eUnlock)
	add(txCase{name: "carrier-two-inputs", note: "a second funding input", tx: signAll(two), reason: "shape"})
	add(txCase{name: "carrier-two-outputs", note: "the record output with 999 and a second output of 1", tx: carrierFrom(e.entryTree, 2, eUnlock, out(999, lock1), out(1, a.feeLock)), reason: "shape"})
	add(txCase{name: "carrier-two-records", note: "an entry and an outcome output in one transaction", tx: carrierFrom(e.entryTree, 2, eUnlock, out(500, lock1), out(500, e.outcome.Outputs[0].LockingScript)), reason: "shape"})
	add(txCase{name: "carrier-record-and-token", note: "an entry output and a token output", tx: carrierFrom(e.entryTree, 2, eUnlock, out(999, lock1), out(1, t1.Outputs[0].LockingScript)), reason: "shape"})
	add(txCase{name: "piece", note: "alice's piece carrier: pieces belong on the blob topic", tx: e.piece, reason: "piece"})

	// ---- carriers: the BEEF ----
	bare := cloneTx(e.entryTree)
	bare.MerklePath = nil
	add(txCase{name: "carrier-beef-tree-unproven", note: "entry-put with its tree unproven, so its BEEF also carries the tree's parent", tx: spendFrom(k1, bare), reason: "beef"})
	wide := e.ch.wide(cloneTx(e.entryTree))
	add(txCase{name: "carrier-beef-proof-extra-leaf", note: "the tree proven by a path holding a leaf the proof does not need", tx: spendFrom(k1, wide), reason: "beef"})
	add(txCase{name: "carrier-beef-extra-transaction", note: "an unrelated mined transaction riding along", tx: k1,
		beef: beefV1([]*transaction.MerklePath{e.misc.MerklePath, e.entryTree.MerklePath}, []*transaction.Transaction{e.misc, e.entryTree, k1}, []int{0, 1, -1}), reason: "beef"})
	add(txCase{name: "carrier-beef-no-tree", note: "the carrier alone", tx: k1, beef: beefV1(nil, []*transaction.Transaction{k1}, []int{-1}), reason: "beef"})
	for _, x := range []struct {
		size   int
		reason string
	}{{storerec.MaxCarrierBEEF, ""}, {storerec.MaxCarrierBEEF + 1, "beef"}} {
		k, err := fitBEEF(x.size, func(d int, v byte) (*transaction.Transaction, []byte) {
			fatTree := e.fat(a, d, v, out(1000, scr(storerec.FundingScript(ek))))
			k := carrierFrom(fatTree, 0, eUnlock, out(1000, lock1))
			return k, mustAtomic(k)
		})
		if err != nil {
			return nil, err
		}
		name, note := "carrier-beef-at-ceiling", "an entry carrier on a tree carrying a data output, its BEEF exactly 81,920 bytes"
		if x.reason != "" {
			name, note = "carrier-beef-over-ceiling", "the same at 81,921 bytes, within a host's own bound"
		}
		add(txCase{name: name, note: note, tx: k, reason: x.reason})
	}
	huge := transaction.NewTransaction()
	huge.AddInputFromTx(e.entryTree, 6, eUnlock)
	huge.AddOutput(out(1<<53, scr(storerec.FundingScript(ek))))
	e.ch.mine(signAll(huge))
	add(txCase{name: "beef-value-2^53", note: "a mined transaction whose output holds 2^53 satoshis: refused with the BEEF before any rule reads it", tx: huge, reason: "beef", noScripts: true})

	// ---- carriers: the record, bucket ----
	badRec := mutate(rec1, storerec.EKCreated, uint64(0))
	add(txCase{name: "carrier-record-created", note: "an entry with created 0, signed", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, a.lock(ed, true, badRec))), reason: "created"})
	add(txCase{name: "carrier-record-kind", note: "a put without its contentType", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, a.lock(ed, true, mutateDel(rec1, storerec.EKContentType)))), reason: "kind"})
	ob := mutate(rec1, storerec.EKBucket, OtherBucket)
	add(txCase{name: "carrier-other-bucket", note: "a well-formed entry naming another bucket", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, a.lock(ed, true, ob))), reason: "bucket"})
	outRec := must(sampleOutcome(storerec.OutcomePass).Encode())
	od := storerec.OutcomeDerivation
	oUnlock := c.unlocker(od)
	add(txCase{name: "outcome-response-signature", note: "carol's pass outcome naming another response hash", tx: carrierFrom(e.outcomeTree, 1, oUnlock,
		out(1000, c.lock(od, true, mutate(outRec, storerec.OKResponse, must32("other"))))), reason: "response-signature"})
	add(txCase{name: "outcome-no-answer", note: "carol's no-answer outcome: the buyer's statement alone", tx: carrierFrom(e.outcomeTree, 1, oUnlock,
		out(1000, c.lock(od, true, must(sampleOutcome(storerec.OutcomeNoAnswer).Encode()))))})
	frRec := must(sampleFront().Encode())
	fd := storerec.FrontDerivation
	add(txCase{name: "front-bad-url", note: "a front statement whose url is http", tx: carrierFrom(e.frontTree, 1, c.unlocker(fd),
		out(1000, c.lock(fd, true, mutate(frRec, storerec.FKURL, "http://front.example")))), reason: "url"})

	// ---- carriers: mineable, unlock ----
	low := carrierFrom(e.entryTree, 2, eUnlock, out(1000, lock1))
	low.LockTime = carrier.LockTime - 1
	add(txCase{name: "carrier-locktime-below", note: "nLockTime 4102444799", tx: signAll(low), reason: "mineable"})
	final := carrierFrom(e.entryTree, 2, eUnlock, out(1000, lock1))
	final.Inputs[0].SequenceNumber = transaction.MaxTxInSequenceNum
	add(txCase{name: "carrier-final-input", note: "nSequence 0xFFFFFFFF", tx: signAll(final), reason: "mineable"})
	spk := a.derivedPriv(ed)
	add(txCase{name: "carrier-unlock-high-s", note: "entry-put with its input signature's high-S twin", tx: rewriteUnlock(k1, 0, func(sg []byte) []byte { return push(append(highSBytes(sg[:len(sg)-1]), sg[len(sg)-1])) }), reason: "unlock"})
	add(txCase{name: "carrier-unlock-pushdata1", note: "the signature pushed by OP_PUSHDATA1", tx: rewriteUnlock(k1, 0, func(sg []byte) []byte { return append([]byte{script.OpPUSHDATA1, byte(len(sg))}, sg...) }), reason: "unlock"})
	add(txCase{name: "carrier-unlock-extra-push", note: "OP_0 before the signature", tx: rewriteUnlock(k1, 0, func(sg []byte) []byte { return append([]byte{script.Op0}, push(sg)...) }), reason: "unlock"})
	add(txCase{name: "carrier-unlock-sighash-single", note: "signed SIGHASH_SINGLE|FORKID", tx: carrierFrom(e.entryTree, 2, flagUnlocker{spk, sighash.SingleForkID}, out(1000, lock1)), reason: "unlock"})

	// ---- carriers: lock ----
	add(txCase{name: "carrier-under-outcome-key", note: "alice's entry locked and signed under her outcome key", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, a.lock(od, true, rec1))), reason: "lock"})
	add(txCase{name: "carrier-lock-other-writer", note: "alice's entry locked and signed under dave's entry key", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, e.d.lock(ed, true, rec1))), reason: "lock"})
	sig1 := sigOf(lock1, 1)
	head := append(append([]byte{33}, ek.Compressed()...), script.OpCHECKSIG)
	add(txCase{name: "carrier-lock-non-minimal-push", note: "the record pushed with OP_PUSHDATA4", tx: carrierFrom(e.entryTree, 2, eUnlock,
		out(1000, scr(append(append(append(append([]byte{}, head...), push4(rec1)...), push(sig1)...), script.Op2DROP)))), reason: "lock"})
	add(txCase{name: "carrier-lock-unsigned", note: "no field signature", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, scr(pushdrop.Script(ek, [][]byte{rec1}, nil)))), reason: "lock"})
	add(txCase{name: "carrier-lock-trailing-opcode", note: "the canonical script then OP_NOP", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, scr(append(append([]byte{}, *lock1...), script.OpNOP)))), reason: "lock"})
	add(txCase{name: "outcome-under-entry-key", note: "carol's outcome locked under her entry key, a key id that is not the outcome's", tx: carrierFrom(e.outcomeTree, 2, oUnlock,
		out(1000, c.lock(ed, true, outRec))), reason: "lock"})

	// ---- carriers: field signature ----
	add(txCase{name: "carrier-field-signature-high-s", note: "the field signature's high-S twin", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, fieldSigOf(lock1, ek, 1, highSBytes))), reason: "signature"})
	add(txCase{name: "carrier-field-signature-wrong-key", note: "a strict signature over the record by dave's key", tx: carrierFrom(e.entryTree, 2, eUnlock, out(1000, fieldSigOf(lock1, ek, 1, signWith(Dave.Priv, rec1)))), reason: "signature"})

	// ---- carriers: funding ----
	add(txCase{name: "carrier-funding-bare-p2pk", note: "spends a bare pay-to-public-key under alice's entry key", tx: carrierFrom(e.misc, 0, eUnlock, out(1000, lock1)), reason: "funding"})
	add(txCase{name: "carrier-funding-signed-tag", note: "spends an output whose one field is the funding tag with a field signature", tx: carrierFrom(e.misc, 1, eUnlock, out(1000, lock1)), reason: "funding"})
	add(txCase{name: "carrier-funding-hold-tag", note: "spends a hold-tagged output under alice's entry key", tx: carrierFrom(e.misc, 2, eUnlock, out(1000, lock1)), reason: "funding"})
	add(txCase{name: "carrier-funding-outcome-key", note: "an entry spending a funding output under alice's outcome key", tx: carrierFrom(e.misc, 3, a.unlocker(od), out(1000, lock1)), reason: "funding"})
	add(txCase{name: "carrier-funding-chain-key", note: "an entry spending a funding output under alice's chain key: never a carrier's", tx: carrierFrom(e.misc, 5, a.unlocker(storerec.ChainDerivation), out(1000, lock1)), reason: "funding"})
	add(txCase{name: "carrier-value-short", note: "999 of the funding output's 1000", tx: carrierFrom(e.entryTree, 2, eUnlock, out(999, lock1)), reason: "funding"})

	if err := e.tokenCases(add); err != nil {
		return nil, err
	}

	// ---- sweeps and transactions that claim nothing ----
	add(txCase{name: "sweep-unmined", note: "the sweep at a height the headers do not hold", tx: reproved(e.sweep, transaction.NewMerklePath(unproven, e.sweep.MerklePath.Path)), reason: "unmined"})
	add(txCase{name: "sweep-beef-extra-transaction", note: "an unrelated mined transaction riding along", tx: e.sweep,
		beef: beefV1([]*transaction.MerklePath{e.misc.MerklePath, e.sweep.MerklePath}, []*transaction.Transaction{e.misc, e.sweep}, []int{0, 1}), reason: "beef"})
	big, err := e.fundingTree(a, ed, mint.MaxFundingOutputs)
	if err != nil {
		return nil, err
	}
	sweepN := func(n int) *transaction.Transaction {
		t := transaction.NewTransaction()
		for v := 0; v < n && v < len(big.Outputs)-1; v++ {
			t.AddInputFromTx(big, uint32(v), eUnlock)
		}
		for v := uint32(3); len(t.Inputs) < n; v++ {
			t.AddInputFromTx(e.entryTree, v, eUnlock)
		}
		t.AddOutput(out(1000, scr(storerec.FundingScript(ek))))
		return e.ch.mine(signAll(t))
	}
	add(txCase{name: "sweep-1024-inputs", note: "a mined sweep of 1024 inputs: the host's sweep bound at its floor", tx: sweepN(storerec.DefaultMaxSweepInputs)})
	add(txCase{name: "sweep-1025-inputs", note: "1025 inputs, one over the floor", tx: sweepN(storerec.DefaultMaxSweepInputs + 1), reason: "too-large"})
	fatSweep := func(d int, v byte) (*transaction.Transaction, []byte) {
		t := transaction.NewTransaction()
		t.AddInputFromTx(e.entryTree, 9, eUnlock)
		t.AddOutput(out(900, scr(storerec.FundingScript(ek))))
		fd := &script.Script{script.OpFALSE, script.OpRETURN}
		if err := fd.AppendPushData(bytes.Repeat([]byte{v}, d)); err != nil {
			panic(err)
		}
		t.AddOutput(out(0, fd))
		e.ch.mine(signAll(t))
		return t, mustAtomic(t)
	}
	for _, x := range []struct {
		name, note, reason string
		size               int
	}{
		{"sweep-beef-at-ceiling", "a mined sweep of two outputs carrying data, its BEEF exactly 196,608 bytes", "", storerec.MaxSweepBEEF},
		{"sweep-beef-over-ceiling", "the same at 196,609 bytes", "beef", storerec.MaxSweepBEEF + 1},
		{"beef-over-host-bound", "a sweep whose BEEF is 262,145 bytes, one over the host's own bound at its floor", "beef", storerec.DefaultMaxBEEF + 1},
	} {
		t, err := fitBEEF(x.size, fatSweep)
		if err != nil {
			return nil, err
		}
		add(txCase{name: x.name, note: x.note, tx: t, reason: x.reason})
	}
	three := transaction.NewTransaction()
	three.AddInputFromTx(e.entryTree, 7, eUnlock)
	three.AddOutput(out(500, scr(storerec.FundingScript(ek))))
	three.AddOutput(out(200, a.feeLock))
	three.AddOutput(out(100, a.feeLock))
	add(txCase{name: "sweep-three-outputs", note: "output 0 funding-shaped but three outputs: not a sweep", tx: e.ch.mine(signAll(three)), reason: "not-bstore"})
	late := transaction.NewTransaction()
	late.AddInputFromTx(e.entryTree, 8, eUnlock)
	late.AddOutput(out(700, a.feeLock))
	late.AddOutput(out(100, scr(storerec.FundingScript(ek))))
	add(txCase{name: "sweep-tombstone-second", note: "the funding-shaped output is output 1, not 0", tx: e.ch.mine(signAll(late)), reason: "not-bstore"})
	spend := transaction.NewTransaction()
	spend.AddInputFromTx(e.sweep, 0, eUnlock)
	spend.AddOutput(out(e.sweep.Outputs[0].Satoshis-200, a.feeLock))
	add(txCase{name: "spend-held-tombstone", note: "a spend of the sweep's tombstone, which the topic holds: never accepted", tx: e.ch.mine(signAll(spend)), reason: "not-bstore"})
	cancel := transaction.NewTransaction()
	cancel.AddInputFromTx(e.renewal, 0, transaction.UnlockingScriptTemplate(adUnlocker{e.b}))
	cancel.AddOutput(out(1, e.b.feeLock))
	add(txCase{name: "advertisement-cancel", note: "bob spends his renewed advertisement to a payment: a cancellation the host never learns", tx: e.ch.mine(signAll(cancel)), reason: "not-bstore"})
	pay := e.ch.mine(signAll(func() *transaction.Transaction {
		t := transaction.NewTransaction()
		f := e.fee(a)
		t.AddInputFromTx(f.Tx, f.Vout, a.feeUnlocker())
		t.AddOutput(out(5000, e.b.feeLock))
		return t
	}()))
	add(txCase{name: "payment", note: "a P2PKH payment", tx: pay, reason: "not-bstore"})
	_ = o
	return cs, nil
}

func mutateDel(rec []byte, k uint64) []byte {
	m := must(cbor.DecodeValue(rec)).(cbor.Map)
	return enc(without(m, k))
}

// tokenCases are the bucket tokens' refusals.
func (e *txEnv) tokenCases(add func(txCase)) error {
	o := e.o
	bd := storerec.BucketDerivation
	t0, t1 := e.tokens[0], e.tokens[1]
	rec1 := must(e.buckets[1].Encode())
	bk := o.key(bd)
	lockOn := func(lock *script.Script, sats uint64, prev *transaction.Transaction) *transaction.Transaction {
		t, err := e.mintRaw(lock, sats, prev, 0)
		if err != nil {
			panic(err)
		}
		return e.ch.mine(t)
	}
	refuse := func(name, note string, tx, parent *transaction.Transaction, reason string) {
		add(txCase{name: name, note: note, tx: tx, beef: tokenBEEF(tx, parent), reason: reason})
	}
	refuse("token-two-satoshis", "seq 1's token output holding 2 satoshis", lockOn(t1.Outputs[0].LockingScript, 2, t0), t0, "shape")
	refuse("token-three-fields", "a third field before the signature", lockOn(o.lock(bd, true, storerec.TagToken, rec1, []byte{0x01}), 1, t0), t0, "shape")
	refuse("token-unsigned", "the tag and the record with no field signature", lockOn(scr(pushdrop.Script(bk, [][]byte{storerec.TagToken, rec1}, nil)), 1, t0), t0, "shape")
	add(txCase{name: "token-beef-atomic", note: "a token in Atomic BEEF", tx: t1,
		beef: atomic(t1.TxID(), tokenBEEF(t1, t0)), reason: "beef"})
	add(txCase{name: "token-beef-no-parent", note: "the token alone", tx: t1, beef: beefV1([]*transaction.MerklePath{t1.MerklePath}, []*transaction.Transaction{t1}, []int{0}), reason: "beef"})
	add(txCase{name: "token-beef-extra-transaction", note: "an unrelated mined transaction riding along", tx: t1,
		beef: beefV1([]*transaction.MerklePath{e.misc.MerklePath, t0.MerklePath, t1.MerklePath}, []*transaction.Transaction{e.misc, t0, t1}, []int{0, 1, 2}), reason: "beef"})
	for _, x := range []struct {
		name, note, reason string
		size               int
	}{
		{"token-beef-at-ceiling", "a first token on a chain tree carrying a data output, its BEEF exactly 196,608 bytes", "", storerec.MaxTokenBEEF},
		{"token-beef-over-ceiling", "the same at 196,609 bytes", "beef", storerec.MaxTokenBEEF + 1},
	} {
		var parent *transaction.Transaction
		fatTok, err := fitBEEF(x.size, func(d int, v byte) (*transaction.Transaction, []byte) {
			parent = e.fat(o, d, v, out(1000, scr(storerec.FundingScript(o.key(storerec.ChainDerivation)))))
			b := *e.buckets[0]
			b.ID = storerec.BucketID([32]byte(*parent.TxID()), 0)
			t, err := e.token(must(b.Encode()), parent, 0)
			if err != nil {
				panic(err)
			}
			e.ch.mine(t)
			return t, tokenBEEF(t, parent)
		})
		if err != nil {
			return err
		}
		add(txCase{name: x.name, note: x.note, tx: fatTok, beef: tokenBEEF(fatTok, parent), reason: x.reason})
	}
	refuse("token-record-created", "seq 1 whose record has created 0, signed", lockOn(o.lock(bd, true, storerec.TagToken, mutate(rec1, storerec.BKCreated, uint64(0))), 1, t0), t0, "created")
	refuse("token-record-first", "seq 1 whose prev is zero", lockOn(o.lock(bd, true, storerec.TagToken, mutate(rec1, storerec.BKPrev, make([]byte, 32))), 1, t0), t0, "first")
	refuse("token-other-bucket", "seq 1 naming another bucket", lockOn(o.lock(bd, true, storerec.TagToken, mutate(rec1, storerec.BKBucket, OtherBucket)), 1, t0), t0, "bucket")
	refuse("token-under-entry-key", "seq 1's record locked and signed under the owner's entry key", lockOn(o.lock(storerec.EntryDerivation, true, storerec.TagToken, rec1), 1, t0), t0, "lock")
	refuse("token-lock-other-owner", "the owner's record locked and signed under alice's bucket key", lockOn(e.a.lock(bd, true, storerec.TagToken, rec1), 1, t0), t0, "lock")
	refuse("token-field-signature-high-s", "the field signature's high-S twin", lockOn(fieldSigOf(t1.Outputs[0].LockingScript, bk, 2, highSBytes), 1, t0), t0, "signature")
	refuse("token-field-signature-record-only", "a strict signature by the bucket key over the record alone, not the tag and the record",
		lockOn(fieldSigOf(t1.Outputs[0].LockingScript, bk, 2, signWith(o.derivedPriv(bd), rec1)), 1, t0), t0, "signature")
	add(txCase{name: "token-unmined", note: "seq 1 under a proof at a height the headers do not hold", tx: t1,
		beef: tokenBEEF(reproved(t1, transaction.NewMerklePath(unproven, t1.MerklePath.Path)), t0), reason: "unmined"})
	refuse("token-prev-not-parent", "seq 1 whose prev is not its parent's txid", lockOn(o.lock(bd, true, storerec.TagToken, mutate(rec1, storerec.BKPrev, must32("not the parent"))), 1, t0), t0, "token-input")
	refuse("token-first-on-misc", "a seq 0 spending an output that is not the owner's chain funding", func() *transaction.Transaction {
		b := *e.buckets[0]
		b.ID = storerec.BucketID([32]byte(*e.misc.TxID()), 5)
		t, err := e.mintRaw(o.lock(bd, true, storerec.TagToken, must(b.Encode())), 1, nil, 0)
		if err != nil {
			panic(err)
		}
		t.AddInputFromTx(e.misc, 5, e.a.unlocker(storerec.ChainDerivation))
		return e.ch.mine(signAll(t))
	}(), e.misc, "token-input")
	refuse("token-first-wrong-id", "a seq 0 on chain-tree output 1 naming the id of output 0", func() *transaction.Transaction {
		b := *e.buckets[0]
		t, err := e.token(must(b.Encode()), e.chainTree, 1)
		if err != nil {
			panic(err)
		}
		return e.ch.mine(t)
	}(), e.chainTree, "bucket-id")
	refuse("token-seq1-spends-chain-tree", "a seq 1 whose parent is the chain tree: no token of the parent is spent", func() *transaction.Transaction {
		r := mutate(rec1, storerec.BKPrev, e.chainTree.TxID()[:])
		t, err := e.token(r, e.chainTree, 2)
		if err != nil {
			panic(err)
		}
		return e.ch.mine(t)
	}(), e.chainTree, "token-input")
	// token-input: what a token's inputs spend of its parent.
	refuse("token-first-two-inputs-on-parent", "a seq 0 spending chain-tree outputs 1 and 2: two inputs spend its parent", func() *transaction.Transaction {
		b := *e.buckets[0]
		b.ID = storerec.BucketID([32]byte(*e.chainTree.TxID()), 1)
		t, err := e.token(must(b.Encode()), e.chainTree, 1)
		if err != nil {
			panic(err)
		}
		t = cloneTx(t)
		t.AddInputFromTx(e.chainTree, 2, o.unlocker(storerec.ChainDerivation))
		return e.ch.mine(signAll(t))
	}(), e.chainTree, "token-input")
	// A mined transaction of two seq 0 token outputs of the owner (never
	// admissible itself), and a seq 1 spending both.
	twin := transaction.NewTransaction()
	fee := e.fee(o)
	twin.AddInputFromTx(fee.Tx, fee.Vout, o.feeUnlocker())
	rec0 := must(e.buckets[0].Encode())
	twin.AddOutput(out(1, o.lock(bd, true, storerec.TagToken, rec0)))
	twin.AddOutput(out(1, o.lock(bd, true, storerec.TagToken, rec0)))
	twin.AddOutput(out(1000000, o.feeLock))
	e.ch.mine(signAll(twin))
	refuse("token-spends-two-parent-tokens", "a seq 1 spending both token outputs of its parent", func() *transaction.Transaction {
		r := mutate(rec1, storerec.BKPrev, twin.TxID()[:])
		t, err := e.token(r, twin, 0)
		if err != nil {
			panic(err)
		}
		t = cloneTx(t)
		t.AddInputFromTx(twin, 1, o.unlocker(bd))
		return e.ch.mine(signAll(t))
	}(), twin, "token-input")
	refuse("token-spends-wrong-seq", "a seq 2 spending the seq 0 token, its prev that token's txid", func() *transaction.Transaction {
		r := mutate(mutate(rec1, storerec.BKSeq, uint64(2)), storerec.BKPrev, t0.TxID()[:])
		t, err := e.token(r, t0, 0)
		if err != nil {
			panic(err)
		}
		return e.ch.mine(t)
	}(), t0, "token-input")
	// A stranger's token at seq 0 copying the bucket's id, and the owner's
	// seq 1 spending it.
	dr := *e.buckets[0]
	dr.Owner = Dave.Pub()
	daveTok := e.ch.mine(e.adTx(e.d, out(1, e.d.lock(bd, true, storerec.TagToken, must(dr.Encode())))))
	refuse("token-spends-other-owner", "the owner's seq 1 spending a seq 0 token of another owner with the bucket's id", func() *transaction.Transaction {
		r := mutate(rec1, storerec.BKPrev, daveTok.TxID()[:])
		t, err := e.mintRaw(o.lock(bd, true, storerec.TagToken, r), 1, nil, 0)
		if err != nil {
			panic(err)
		}
		t.AddInputFromTx(daveTok, 0, e.d.unlocker(bd))
		return e.ch.mine(signAll(t))
	}(), daveTok, "token-input")
	or := *e.buckets[0]
	or.ID = label("another id")
	otherID := e.ch.mine(e.adTx(o, out(1, o.lock(bd, true, storerec.TagToken, must(or.Encode())))))
	refuse("token-spends-other-id", "a seq 1 spending a seq 0 token of the owner with another id", func() *transaction.Transaction {
		t, err := e.token(mutate(rec1, storerec.BKPrev, otherID.TxID()[:]), otherID, 0)
		if err != nil {
			panic(err)
		}
		return e.ch.mine(t)
	}(), otherID, "token-input")
	return nil
}

func anyHeld(e *txEnv, tx *transaction.Transaction) []int { return e.heldInputs(tx) }

// genTransaction writes transaction-v1.json.
func genTransaction(files map[string][]byte) error {
	e, err := newTxEnv()
	if err != nil {
		return err
	}
	cases, err := e.cases()
	if err != nil {
		return err
	}
	var list []txJSON
	seen := map[string]bool{}
	reasons := map[string]bool{}
	for _, c := range cases {
		if seen[c.name] {
			return fmt.Errorf("%s: two cases of one name", c.name)
		}
		seen[c.name] = true
		beef := c.beef
		if beef == nil {
			if beef, err = c.tx.AtomicBEEF(false); err != nil {
				return fmt.Errorf("%s: beef: %w", c.name, err)
			}
		}
		held := c.held
		if held == nil {
			held = anyHeld(e, c.tx)
		}
		a, err := storerec.Admit(e.ctx, beef, held, storerec.Host{Bucket: Bucket, Headers: e.ch.headers})
		j := txJSON{Name: c.name, Note: c.note, Txid: c.tx.TxID().String(), BEEF: hx(beef), BEEFLength: len(beef), Held: held,
			Scripts: scriptsVerify(e.ctx, c.tx)}
		if !j.Scripts && !c.noScripts {
			return fmt.Errorf("%s: its own scripts do not verify", c.name)
		}
		switch {
		case c.reason == "" && err != nil:
			return fmt.Errorf("%s: refused: %w", c.name, err)
		case c.reason != "" && err == nil:
			return fmt.Errorf("%s: admitted, want %s", c.name, c.reason)
		case c.reason != "" && storerec.Reason(err) != c.reason:
			return fmt.Errorf("%s: reason %s, want %s (%v)", c.name, storerec.Reason(err), c.reason, err)
		case err == nil:
			j.Verdict, j.Admits = "admit", admission(a)
		default:
			j.Verdict, j.Reason = "refuse", c.reason
			reasons[c.reason] = true
		}
		rc := refusalCase{Hex: j.BEEF}
		compact(&rc)
		j.BEEF, j.BEEFFill = rc.Hex, rc.Fill
		if !bytes.Equal(expand(rc), beef) {
			return fmt.Errorf("%s: compaction", c.name)
		}
		list = append(list, j)
	}
	for _, r := range []string{"shape", "beef", "bucket", "mineable", "unlock", "lock", "signature", "funding", "unmined", "token-input",
		"bucket-id", "too-many", "too-large", "not-bstore", "piece", "response-signature", "first", "created", "kind", "url"} {
		if !reasons[r] {
			return fmt.Errorf("no transaction case is refused %s", r)
		}
	}
	type cfgJSON struct {
		MaxBEEF        int    `json:"maxBeef"`
		MaxSweepInputs int    `json:"maxSweepInputs"`
		Result         string `json:"result"`
	}
	var cfgs []cfgJSON
	sb := mustAtomic(e.sweep)
	for _, c := range [][2]int{{0, 0}, {storerec.DefaultMaxBEEF, storerec.DefaultMaxSweepInputs}, {1 << 20, 4096},
		{storerec.DefaultMaxBEEF - 1, 0}, {0, storerec.DefaultMaxSweepInputs - 1}} {
		_, err := storerec.Admit(e.ctx, sb, nil, storerec.Host{Bucket: Bucket, Headers: e.ch.headers, MaxBEEF: c[0], MaxSweepInputs: c[1]})
		r := "ok"
		if err != nil {
			if !errors.Is(err, storerec.ErrHostConfig) {
				return fmt.Errorf("host config %v: %v", c, err)
			}
			r = "host-config"
		}
		cfgs = append(cfgs, cfgJSON{c[0], c[1], r})
	}
	var headers []map[string]any
	for ht := uint32(801); ht <= e.ch.height; ht++ {
		if r, ok := e.ch.headers[ht]; ok {
			headers = append(headers, map[string]any{"height": ht, "merkleRoot": r.String()})
		}
	}
	return put(files, "transaction-v1.json", map[string]any{
		"note": "transactions admitted or refused by the topic manager's rules (spec section 12.1): a carrier's shape, beef, the record's rules, bucket, mineable, unlock, lock, signature, funding; a token's shape, beef, the record's rules, bucket, lock, signature, unmined, token-input, bucket-id; advertisements and hold outputs (beef, unmined, too-many); a sweep's beef, unmined, too-large; a piece refused; anything else not-bstore. " +
			"Each case is the BEEF submitted (Atomic BEEF unless the case says otherwise; a token's is BEEF V1 of its parent then the token) and the previous coins it is admitted with, which decide what is retained and no verdict. " +
			"Admission runs against the bucket and headers below with a host BEEF bound of 262,144 bytes and a sweep input bound of 1024. scriptsVerify says every input passes the script interpreter against the output it spends. Signatures are deterministic (RFC 6979).",
		"bucket":      Bucket,
		"topic":       storerec.RecordsTopic(Bucket),
		"headers":     headers,
		"cases":       list,
		"hostConfigs": cfgs,
	})
}

// fitBEEF builds with a data output of d bytes, each byte v, until the BEEF
// is exactly size bytes long.
func fitBEEF(size int, build func(d int, v byte) (*transaction.Transaction, []byte)) (*transaction.Transaction, error) {
	d := size - 2000
	for range 3 {
		_, b := build(d, 0)
		d += size - len(b)
	}
	for _, off := range []int{0, -1, 1, -2, 2} {
		for v := byte(0); v < 8; v++ {
			t, b := build(d+off, v)
			if len(b) == size {
				return t, nil
			}
		}
	}
	return nil, fmt.Errorf("no data size gives a BEEF of %d bytes", size)
}

func mustAtomic(tx *transaction.Transaction) []byte { return must(tx.AtomicBEEF(false)) }
