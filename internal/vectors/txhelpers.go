package vectors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/spv"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/go-sdk/util"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Headers is the test chain's block headers: a merkle root per height. It is
// the chain tracker the transaction vectors are admitted against.
type Headers map[uint32]chainhash.Hash

// IsValidRootForHeight holds root to the one the test chain has at height.
func (h Headers) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	want, ok := h[height]
	return ok && root != nil && *root == want, nil
}

// CurrentHeight is the highest height held.
func (h Headers) CurrentHeight(context.Context) (uint32, error) {
	var top uint32
	for k := range h {
		top = max(top, k)
	}
	return top, nil
}

// unproven is a height nothing is mined at: a proof there verifies against no
// header the test chain holds.
const unproven = 999

// chain mines transactions into blocks at successive heights.
type chain struct {
	height  uint32
	headers Headers
}

func fixedHash(b byte) *chainhash.Hash {
	h := chainhash.Hash(bytes.Repeat([]byte{b}, 32))
	return &h
}

// mine proves tx at offset 1 of a block of two beside a fixed sibling.
func (c *chain) mine(tx *transaction.Transaction) *transaction.Transaction {
	c.height++
	t := true
	tx.MerklePath = transaction.NewMerklePath(c.height, [][]*transaction.PathElement{{
		{Offset: 0, Hash: fixedHash(0x33)},
		{Offset: 1, Hash: tx.TxID(), Txid: &t},
	}})
	c.root(tx)
	return tx
}

// mineTogether proves a and b in one block of four, at offsets 1 and 2, each
// with its own minimal path.
func (c *chain) mineTogether(a, b *transaction.Transaction) {
	c.height++
	t := true
	s0, s3 := fixedHash(0x35), fixedHash(0x36)
	left := chainhash.DoubleHashH(append(append([]byte{}, s0[:]...), a.TxID()[:]...))
	right := chainhash.DoubleHashH(append(append([]byte{}, b.TxID()[:]...), s3[:]...))
	a.MerklePath = transaction.NewMerklePath(c.height, [][]*transaction.PathElement{
		{{Offset: 0, Hash: s0}, {Offset: 1, Hash: a.TxID(), Txid: &t}},
		{{Offset: 1, Hash: &right}},
	})
	b.MerklePath = transaction.NewMerklePath(c.height, [][]*transaction.PathElement{
		{{Offset: 2, Hash: b.TxID(), Txid: &t}, {Offset: 3, Hash: s3}},
		{{Offset: 0, Hash: &left}},
	})
	c.root(a)
}

// wide proves tx at offset 2 of a block of four with a path that also holds
// the leaf at offset 0, which the proof does not need.
func (c *chain) wide(tx *transaction.Transaction) *transaction.Transaction {
	c.height++
	l0, l1, l3 := fixedHash(0x30), fixedHash(0x31), fixedHash(0x34)
	pair := chainhash.DoubleHashH(append(append([]byte{}, l0[:]...), l1[:]...))
	t := true
	tx.MerklePath = transaction.NewMerklePath(c.height, [][]*transaction.PathElement{
		{{Offset: 0, Hash: l0}, {Offset: 2, Hash: tx.TxID(), Txid: &t}, {Offset: 3, Hash: l3}},
		{{Offset: 0, Hash: &pair}},
	})
	c.root(tx)
	return tx
}

func (c *chain) root(tx *transaction.Transaction) {
	root, err := tx.MerklePath.ComputeRoot(tx.TxID())
	if err != nil {
		panic(err)
	}
	c.headers[c.height] = *root
}

// actor is one test identity's wallet and keys for transactions.
type actor struct {
	p       Party
	w       wallet.Interface
	fee     *ec.PrivateKey // [1, "bchat"] fund, counterparty self
	feeLock *script.Script
}

func newActor(p Party) (*actor, error) {
	a := &actor{p: p, w: p.Wallet()}
	var err error
	d := wallet.NewKeyDeriver(p.Priv)
	if a.fee, err = d.DerivePrivateKey(storerec.Protocol, storerec.KeyFund, wallet.Counterparty{Type: wallet.CounterpartyTypeSelf}); err != nil {
		return nil, err
	}
	addr, err := script.NewAddressFromPublicKey(a.fee.PubKey(), false)
	if err != nil {
		return nil, err
	}
	a.feeLock, err = p2pkh.Lock(addr)
	return a, err
}

func (a *actor) feeUnlocker() transaction.UnlockingScriptTemplate {
	u, err := p2pkh.Unlock(a.fee, nil)
	if err != nil {
		panic(err)
	}
	return u
}

func (a *actor) unlocker(d pushdrop.Derivation) transaction.UnlockingScriptTemplate {
	return d.Unlocker(context.Background(), a.w, "")
}

func (a *actor) lock(d pushdrop.Derivation, sign bool, fields ...[]byte) *script.Script {
	s, err := d.Lock(context.Background(), a.w, "", fields, sign)
	if err != nil {
		panic(err)
	}
	return s
}

func (a *actor) key(d pushdrop.Derivation) *ec.PublicKey { return mustKey(d, a.p.Priv.PubKey()) }

// derivedPriv is the actor's private key under d, counterparty anyone.
func (a *actor) derivedPriv(d pushdrop.Derivation) *ec.PrivateKey {
	k, err := wallet.NewKeyDeriver(a.p.Priv).DerivePrivateKey(d.Protocol, d.KeyID, pushdrop.Anyone())
	if err != nil {
		panic(err)
	}
	return k
}

func mustKey(d pushdrop.Derivation, identity *ec.PublicKey) *ec.PublicKey {
	k, err := d.ExpectedLockingKey(identity)
	if err != nil {
		panic(err)
	}
	return k
}

func scr(b []byte) *script.Script { s := script.Script(b); return &s }

func out(sats uint64, s *script.Script) *transaction.TransactionOutput {
	return &transaction.TransactionOutput{Satoshis: sats, LockingScript: s}
}

func params(d pushdrop.Derivation) carrier.Params {
	return carrier.Params{Derivation: d, FundingTag: storerec.TagFunding,
		ValidatePayload: func([]byte) error { return nil }, ErrIdentity: storerec.ErrIdentity}
}

// ---- helpers that build what the cases change ----

// flagUnlocker signs a single-signature input with key under another
// sighash type.
type flagUnlocker struct {
	key  *ec.PrivateKey
	flag sighash.Flag
}

func (u flagUnlocker) Sign(tx *transaction.Transaction, i uint32) (*script.Script, error) {
	d, err := tx.CalcInputSignatureHash(i, u.flag)
	if err != nil {
		return nil, err
	}
	sig, err := u.key.Sign(d)
	if err != nil {
		return nil, err
	}
	return scr(push(append(sig.Serialize(), byte(u.flag)))), nil
}

func (u flagUnlocker) EstimateLength(*transaction.Transaction, uint32) uint32 { return 74 }

func signAll(tx *transaction.Transaction) *transaction.Transaction {
	if err := tx.Sign(); err != nil {
		panic(err)
	}
	return tx
}

// carrierFrom builds a carrier by hand: one input spending src:vout, the
// given outputs, the carrier's locktime and sequence, signed.
func carrierFrom(src *transaction.Transaction, vout uint32, u transaction.UnlockingScriptTemplate, outs ...*transaction.TransactionOutput) *transaction.Transaction {
	tx := transaction.NewTransaction()
	tx.LockTime = carrier.LockTime
	tx.AddInputFromTx(src, vout, u)
	tx.Inputs[0].SequenceNumber = carrier.Sequence
	for _, o := range outs {
		tx.AddOutput(o)
	}
	return signAll(tx)
}

// highSBytes rewrites a strict low-S DER signature to its high-S twin.
func highSBytes(der []byte) []byte { return highS(der) }

// cloneTx copies tx, keeping each input's source transaction.
func cloneTx(tx *transaction.Transaction) *transaction.Transaction {
	cp := &transaction.Transaction{Version: tx.Version, LockTime: tx.LockTime, MerklePath: tx.MerklePath}
	for _, in := range tx.Inputs {
		c := *in
		cp.Inputs = append(cp.Inputs, &c)
	}
	for _, o := range tx.Outputs {
		c := *o
		cp.Outputs = append(cp.Outputs, &c)
	}
	return cp
}

// rewriteUnlock replaces input i's unlocking script with f of its one push.
func rewriteUnlock(tx *transaction.Transaction, i int, f func(sig []byte) []byte) *transaction.Transaction {
	cp := cloneTx(tx)
	ch, err := tx.Inputs[i].UnlockingScript.Chunks()
	if err != nil {
		panic(err)
	}
	cp.Inputs[i].UnlockingScript = scr(f(ch[0].Data))
	cp.MerklePath = nil
	return cp
}

// spendFrom returns tx with input 0's source replaced by src, the same
// transaction under another proof (or none).
func spendFrom(tx, src *transaction.Transaction) *transaction.Transaction {
	cp := cloneTx(tx)
	cp.Inputs[0].SourceTransaction = src
	return cp
}

// reproved is tx under another proof.
func reproved(tx *transaction.Transaction, mp *transaction.MerklePath) *transaction.Transaction {
	cp := cloneTx(tx)
	cp.MerklePath = mp
	return cp
}

func push(d []byte) []byte {
	s := &script.Script{}
	if err := s.AppendPushData(d); err != nil {
		panic(err)
	}
	return *s
}

// push4 pushes d with OP_PUSHDATA4, which is never the minimal push of
// anything a record or a tag holds.
func push4(d []byte) []byte {
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(d)))
	return append(append([]byte{script.OpPUSHDATA4}, n[:]...), d...)
}

// fieldSigOf rebuilds a signed PushDrop's fields under key with its field
// signature replaced by f(signature), canonically otherwise.
func fieldSigOf(s *script.Script, key *ec.PublicKey, nfields int, f func([]byte) []byte) *script.Script {
	fields, _ := pushdrop.Fields(*s)
	return scr(pushdrop.Script(key, fields[:nfields], f(fields[nfields])))
}

func sigOf(s *script.Script, nfields int) []byte {
	fields, _ := pushdrop.Fields(*s)
	return fields[nfields]
}

func signWith(k *ec.PrivateKey, msg []byte) func([]byte) []byte {
	return func([]byte) []byte {
		d := sha256.Sum256(msg)
		sg, err := k.Sign(d[:])
		if err != nil {
			panic(err)
		}
		return sg.Serialize()
	}
}

// beefV1 writes a BEEF V1 by hand: the BUMPs, then each transaction with the
// index of its BUMP (-1 for none), in the order given; the last is the
// subject.
func beefV1(bumps []*transaction.MerklePath, txs []*transaction.Transaction, bumpOf []int) []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, uint32(0xEFBE0001))
	b.Write(util.VarInt(uint64(len(bumps))).Bytes())
	for _, m := range bumps {
		b.Write(m.Bytes())
	}
	b.Write(util.VarInt(uint64(len(txs))).Bytes())
	for i, tx := range txs {
		b.Write(tx.Bytes())
		if bumpOf[i] < 0 {
			b.WriteByte(0)
			continue
		}
		b.WriteByte(1)
		b.Write(util.VarInt(uint64(bumpOf[i])).Bytes())
	}
	return b.Bytes()
}

// beefV2 writes a BEEF V2 (BRC-96) by hand: format 0 raw, 1 raw with a BUMP
// index, 2 txid only (a nil transaction at that place, its txid in ids).
func beefV2(bumps []*transaction.MerklePath, txs []*transaction.Transaction, bumpOf []int, ids map[int][32]byte) []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, uint32(0xEFBE0002))
	b.Write(util.VarInt(uint64(len(bumps))).Bytes())
	for _, m := range bumps {
		b.Write(m.Bytes())
	}
	b.Write(util.VarInt(uint64(len(txs))).Bytes())
	for i, tx := range txs {
		if id, ok := ids[i]; ok {
			b.WriteByte(2)
			b.Write(id[:])
			continue
		}
		if bumpOf[i] < 0 {
			b.WriteByte(0)
			b.Write(tx.Bytes())
			continue
		}
		b.WriteByte(1)
		b.Write(util.VarInt(uint64(bumpOf[i])).Bytes())
		b.Write(tx.Bytes())
	}
	return b.Bytes()
}

// atomic wraps a BEEF V1 or V2 as Atomic BEEF (BRC-95) of subject.
func atomic(subject *chainhash.Hash, inner []byte) []byte {
	out := []byte{0x01, 0x01, 0x01, 0x01}
	out = append(out, subject[:]...)
	return append(out, inner...)
}

func tokenBEEF(t, parent *transaction.Transaction) []byte {
	b, err := storerec.TokenBEEF(t, parent)
	if err != nil {
		panic(err)
	}
	return b
}

// scriptsVerify runs the interpreter over tx's own inputs, each against the
// output it spends.
func scriptsVerify(ctx context.Context, tx *transaction.Transaction) bool {
	cp := tx.ShallowClone()
	cp.MerklePath = nil
	ok, err := spv.Verify(ctx, cp, &spv.GullibleHeadersClient{}, nil)
	return err == nil && ok
}
