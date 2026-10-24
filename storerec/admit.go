package storerec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/pushdrop"
)

// The transaction refusals of spec section 12.1, beside the record
// refusals. A transaction is refused for the first rule it breaks.
var (
	ErrShape      = errors.New("storerec: transaction has the wrong shape")
	ErrBEEF       = chaintoken.ErrBEEF
	ErrMineable   = errors.New("storerec: carrier is mineable")
	ErrUnlock     = errors.New("storerec: unlocking script is not one canonical signature push")
	ErrLock       = errors.New("storerec: output script is not the canonical one for the owner")
	ErrFieldSig   = errors.New("storerec: field signature is not strict or does not verify")
	ErrFunding    = errors.New("storerec: carrier does not spend a funding output of its owner, or does not carry its value")
	ErrUnmined    = errors.New("storerec: proof does not verify against the host's headers")
	ErrTokenInput = errors.New("storerec: token does not spend its predecessor, or its chain tree at seq 0")
	ErrBucketID   = errors.New("storerec: id is not the bucket id of the outpoint the first token spends")
	ErrTooMany    = errors.New("storerec: more than 16 advertisements and hold outputs")
	ErrNotBstore  = errors.New("storerec: transaction is not for this topic")
	ErrPieceTx    = errors.New("storerec: a piece; pieces belong on the blob topic")
	ErrHostConfig = errors.New("storerec: host configuration or previous coins out of range")

	errNoPred = errors.New("storerec: predecessor is not a well-formed token")
)

// TxKind is what an admitted transaction is to the topic.
type TxKind string

const (
	TxEntry   TxKind = "entry"
	TxOutcome TxKind = "outcome"
	TxFront   TxKind = "front"
	TxToken   TxKind = "bucket-token"
	TxMined   TxKind = "mined"
	TxSweep   TxKind = "sweep"
)

// Host is what admission needs beyond the submission: the topic's bucket
// identifier, a chain tracker over the host's own block headers, the host's
// BEEF bound and its sweep input bound. A bound of zero is its floor; a
// bound below its floor is a configuration the host refuses to run with
// (ErrHostConfig). Nothing the topic holds is here: a verdict never reads
// another record (spec section 12.1).
type Host struct {
	Bucket         string
	Headers        chaintracker.ChainTracker
	MaxBEEF        int
	MaxSweepInputs int
}

// Carrier is an admitted carrier and the record it holds.
type Carrier struct {
	Kind       TxKind
	Record     []byte
	Entry      *Entry
	Outcome    *Outcome
	Front      *Front
	Commitment [32]byte
	Funding    transaction.Outpoint
}

// Token is a bucket token's output, its record decoded.
type Token struct {
	Bucket *Bucket
	Record []byte
	Vout   uint32
}

// Admission is what a host admits from one transaction (BRC-22's admittance
// instructions) and what the lookup service records.
type Admission struct {
	Kind    TxKind
	Txid    [32]byte
	Outputs []int
	Retain  []int
	Carrier *Carrier
	// Token, Predecessor (nil at seq 0), Parent and Spends for a token.
	Token       *Token
	Predecessor *Token
	Parent      [32]byte
	Spends      transaction.Outpoint
	// Ads and Holds index the admitted outputs of a mined object.
	Ads   map[int]*Advertisement
	Holds []int
	// Spent is every outpoint a sweep's inputs spend.
	Spent []transaction.Outpoint
}

// Admit applies spec section 12.1 to one submission: the BEEF as it arrived,
// and held, the indices of its subject's inputs that spend an output the
// topic holds (BRC-22's previous coins). held decides only what is retained,
// never a verdict: every rule reads the submission's own bytes and the
// host's headers alone. SPV of the BEEF is the engine's and is assumed to
// have run. Every refusal is an error, which a topic manager raises.
// Admit is Prepare and then Pending.Admit.
func Admit(ctx context.Context, beef []byte, held []int, h Host) (*Admission, error) {
	return Prepare(ctx, beef, h).Admit(held)
}

// Pending is one submission decided on every rule before held is known:
// Prepare's half of Admit. Since no rule reads what the topic holds, a host
// may prepare a submission before it takes the topic's lock and finish it
// with held under the lock.
type Pending struct {
	// read is whether the BEEF was read: until then a refusal comes before
	// held is looked at, after it held's range is checked first.
	read bool
	tx   *transaction.Transaction
	a    *Admission // the verdict but Retain, when err is nil
	err  error
}

// Prepare applies every rule of Admit to beef but the range of held.
func Prepare(ctx context.Context, beef []byte, h Host) *Pending {
	if CheckBucket(h.Bucket) != nil {
		return &Pending{err: fmt.Errorf("%w: bucket identifier breaks the grammar", ErrHostConfig)}
	}
	bound, maxSweep, err := h.bounds()
	if err != nil {
		return &Pending{err: err}
	}
	w, err := chaintoken.ReadWire(beef, bound)
	if err != nil {
		return &Pending{err: err}
	}
	// A value above 2^53 - 1 satoshis is refused with the BEEF: no amount of
	// money is that large, and a reader that holds values as doubles cannot
	// read it, so both codecs refuse it at the same step.
	for _, e := range w.Entries {
		if e.Tx == nil {
			continue
		}
		for _, o := range e.Tx.Outputs {
			if o != nil && o.Satoshis > MaxSafe {
				return &Pending{err: fmt.Errorf("%w: an output of %d satoshis", ErrBEEF, o.Satoshis)}
			}
		}
	}
	tx := w.SubjectTx()
	a, err := decide(ctx, w, tx, len(beef), maxSweep, h)
	return &Pending{read: true, tx: tx, a: a, err: err}
}

// Admit finishes a prepared admission with held, as Admit would have
// decided it: a BEEF refused stays refused whatever held is, held out of
// range comes before every other rule, and an admission retains held.
func (p *Pending) Admit(held []int) (*Admission, error) {
	if !p.read {
		return nil, p.err
	}
	retain, err := retained(p.tx, held)
	if err != nil {
		return nil, err
	}
	if p.err != nil {
		return nil, p.err
	}
	a := *p.a
	a.Retain = retain
	return &a, nil
}

// decide is Admit's rules after the BEEF was read, but held's range.
func decide(ctx context.Context, w *chaintoken.Wire, tx *transaction.Transaction, size, maxSweep int, h Host) (*Admission, error) {
	a := &Admission{Txid: [32]byte(*tx.TxID())}

	claim, claims, at := ClaimNone, 0, 0
	for i, o := range tx.Outputs {
		if o == nil || o.LockingScript == nil {
			continue
		}
		if k := Classify(*o.LockingScript); k.Claims() {
			claim, at = k, i
			claims++
		}
	}
	if claims > 1 {
		return nil, fmt.Errorf("%w: %d outputs claim a token or a record", ErrShape, claims)
	}
	switch claim {
	case ClaimPiece:
		return nil, ErrPieceTx
	case ClaimEntry, ClaimOutcome, ClaimFront:
		c, err := checkCarrier(w, tx, claim, size, h)
		if err != nil {
			return nil, err
		}
		a.Kind, a.Carrier, a.Outputs = c.Kind, c, []int{0}
		return a, nil
	case ClaimToken:
		if err := a.checkToken(ctx, w, tx, at, size, h); err != nil {
			return nil, err
		}
		a.Kind, a.Outputs = TxToken, []int{at}
		return a, nil
	}

	// Mined objects: advertisements and hold-shaped outputs.
	ads := map[int]*Advertisement{}
	var holds, admitted []int
	for i, o := range tx.Outputs {
		if o == nil || o.LockingScript == nil {
			continue
		}
		s := *o.LockingScript
		if Classify(s) == ClaimHold {
			holds = append(holds, i)
			admitted = append(admitted, i)
			continue
		}
		if ad, err := DecodeAdvertisement(s); err == nil {
			ads[i] = ad
			admitted = append(admitted, i)
		}
	}
	if len(admitted) > 0 {
		if err := w.Alone(tx); err != nil {
			return nil, err
		}
		if size > MaxMinedBEEF {
			return nil, fmt.Errorf("%w: a mined object's BEEF of %d bytes", ErrBEEF, size)
		}
		if !chaintoken.Mined(ctx, tx, h.Headers) {
			return nil, ErrUnmined
		}
		if len(admitted) > MaxAdsPerTx {
			return nil, fmt.Errorf("%w: %d", ErrTooMany, len(admitted))
		}
		a.Kind, a.Outputs, a.Ads, a.Holds = TxMined, admitted, ads, holds
		return a, nil
	}

	// A sweep: at most two outputs, output 0 funding-shaped under any key.
	if len(tx.Outputs) >= 1 && len(tx.Outputs) <= 2 && tx.Outputs[0] != nil && tx.Outputs[0].LockingScript != nil &&
		Classify(*tx.Outputs[0].LockingScript) == ClaimFunding {
		if err := w.Alone(tx); err != nil {
			return nil, err
		}
		if size > MaxSweepBEEF {
			return nil, fmt.Errorf("%w: a sweep's BEEF of %d bytes", ErrBEEF, size)
		}
		if !chaintoken.Mined(ctx, tx, h.Headers) {
			return nil, ErrUnmined
		}
		if len(tx.Inputs) > maxSweep {
			return nil, fmt.Errorf("%w: a sweep of %d inputs, host bound %d", ErrTooLarge, len(tx.Inputs), maxSweep)
		}
		a.Kind, a.Outputs = TxSweep, []int{0}
		for _, in := range tx.Inputs {
			a.Spent = append(a.Spent, transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex})
		}
		return a, nil
	}
	return nil, ErrNotBstore
}

// bounds is the host's BEEF and sweep input bounds, zero meaning the floor.
func (h Host) bounds() (int, int, error) {
	b, s := h.MaxBEEF, h.MaxSweepInputs
	if b == 0 {
		b = DefaultMaxBEEF
	}
	if s == 0 {
		s = DefaultMaxSweepInputs
	}
	if b < DefaultMaxBEEF || s < DefaultMaxSweepInputs {
		return 0, 0, fmt.Errorf("%w: a BEEF bound of %d or a sweep bound of %d is below its floor", ErrHostConfig, b, s)
	}
	return b, s, nil
}

func retained(tx *transaction.Transaction, held []int) ([]int, error) {
	seen := map[int]bool{}
	out := []int{}
	for _, i := range held {
		if i < 0 || i >= len(tx.Inputs) {
			return nil, fmt.Errorf("%w: previous coin names input %d of %d", ErrHostConfig, i, len(tx.Inputs))
		}
		if !seen[i] {
			seen[i] = true
			out = append(out, i)
		}
	}
	sort.Ints(out)
	return out, nil
}

// carrierRecord decodes a carrier's record by its claim and returns its
// owner (writer, buyer or front), the key id it is locked under and its
// bucket.
func carrierRecord(claim Claim, rec []byte) (*Carrier, []byte, pushdrop.Derivation, string, error) {
	switch claim {
	case ClaimEntry:
		e, err := DecodeEntry(rec)
		if err != nil {
			return nil, nil, pushdrop.Derivation{}, "", err
		}
		return &Carrier{Kind: TxEntry, Entry: e}, e.Writer, EntryDerivation, e.Bucket, nil
	case ClaimOutcome:
		o, err := DecodeOutcome(rec)
		if err != nil {
			return nil, nil, pushdrop.Derivation{}, "", err
		}
		return &Carrier{Kind: TxOutcome, Outcome: o}, o.Buyer, OutcomeDerivation, o.Bucket, nil
	default:
		f, err := DecodeFront(rec)
		if err != nil {
			return nil, nil, pushdrop.Derivation{}, "", err
		}
		return &Carrier{Kind: TxFront, Front: f}, f.Front, FrontDerivation, f.Bucket, nil
	}
}

// checkCarrier is a carrier's rules 1 to 9.
func checkCarrier(w *chaintoken.Wire, tx *transaction.Transaction, claim Claim, size int, h Host) (*Carrier, error) {
	// 1. shape.
	if len(tx.Inputs) != 1 || len(tx.Outputs) != 1 {
		return nil, fmt.Errorf("%w: %d inputs, %d outputs", ErrShape, len(tx.Inputs), len(tx.Outputs))
	}
	in, out := tx.Inputs[0], tx.Outputs[0]
	// 2. beef: the carrier and its funding tree with the tree's minimal
	// path, within the carrier's ceiling.
	tree, err := w.Carrier(tx)
	if err != nil {
		return nil, err
	}
	if size > MaxCarrierBEEF {
		return nil, fmt.Errorf("%w: a carrier's BEEF of %d bytes", ErrBEEF, size)
	}
	// 3. the record's own rules.
	rec, _ := pushdrop.FirstPush(*out.LockingScript)
	c, owner, d, bucket, err := carrierRecord(claim, rec)
	if err != nil {
		return nil, err
	}
	c.Record, c.Commitment = rec, carrier.Commitment(tx)
	c.Funding = transaction.Outpoint{Txid: tree.Txid, Index: in.SourceTxOutIndex}
	// 4. bucket.
	if bucket != h.Bucket {
		return nil, fmt.Errorf("%w: not this topic's bucket", ErrBucket)
	}
	// 5. mineable.
	if tx.LockTime < carrier.LockTime || in.SequenceNumber == transaction.MaxTxInSequenceNum {
		return nil, ErrMineable
	}
	// 6. unlock.
	if err := carrier.CheckUnlocking(tx); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnlock, err)
	}
	// 7. lock: the one-field signed PushDrop under derive(owner, k).
	key := mustLockingKey(d, owner)
	fields, _ := pushdrop.Fields(*out.LockingScript)
	if len(fields) != 2 {
		return nil, fmt.Errorf("%w: %d pushes, want the record and a signature", ErrLock, len(fields))
	}
	sig := fields[1]
	if !bytes.Equal(*out.LockingScript, pushdrop.Script(key, [][]byte{rec}, sig)) {
		return nil, ErrLock
	}
	// 8. signature.
	if !chaintoken.VerifyField(key, rec, sig) {
		return nil, ErrFieldSig
	}
	// 9. funding.
	if int(in.SourceTxOutIndex) >= len(tree.Tx.Outputs) {
		return nil, fmt.Errorf("%w: the funding tree has no such output", ErrFunding)
	}
	src := tree.Tx.Outputs[in.SourceTxOutIndex]
	if src == nil || src.LockingScript == nil || !bytes.Equal(*src.LockingScript, FundingScript(key)) {
		return nil, ErrFunding
	}
	if src.Satoshis != out.Satoshis {
		return nil, fmt.Errorf("%w: output %d of input %d", ErrFunding, out.Satoshis, src.Satoshis)
	}
	return c, nil
}

// checkToken is a bucket token's rules 1 to 9.
func (a *Admission) checkToken(ctx context.Context, w *chaintoken.Wire, tx *transaction.Transaction, at, size int, h Host) error {
	out := tx.Outputs[at]
	// 1. shape: a PushDrop of two fields and a signature, of 1 satoshi.
	to, ok := chaintoken.ReadOutput(out, uint32(at), 2)
	if !ok {
		return fmt.Errorf("%w: a token is two fields, a signature and 1 satoshi", ErrShape)
	}
	// 2. beef: the token and its parent, each proven minimally, or one
	// merged path; never an Atomic BEEF; within the token's ceiling.
	parent, err := w.Token(tx)
	if err != nil {
		return err
	}
	if size > MaxTokenBEEF {
		return fmt.Errorf("%w: a token's BEEF of %d bytes", ErrBEEF, size)
	}
	// 3. the record's own rules.
	bk, err := DecodeBucket(to.Fields[1])
	if err != nil {
		return err
	}
	t := &Token{Bucket: bk, Record: to.Fields[1], Vout: uint32(at)}
	// 4. bucket.
	if bk.Bucket != h.Bucket {
		return fmt.Errorf("%w: not this topic's bucket", ErrBucket)
	}
	// 5. lock.
	key := mustLockingKey(BucketDerivation, bk.Owner)
	if !to.LockedTo(*out.LockingScript, key) {
		return ErrLock
	}
	// 6. signature.
	if !to.SignedBy(key) {
		return ErrFieldSig
	}
	// 7. unmined: the token's and the parent's proofs.
	if !chaintoken.Mined(ctx, tx, h.Headers) || !chaintoken.Mined(ctx, parent.Tx, h.Headers) {
		return ErrUnmined
	}
	// 8. token-input: every spend is read from the parent in the BEEF.
	ptxid := [32]byte(parent.Txid)
	spends := chaintoken.Spends(tx, parent)
	a.Token, a.Parent = t, ptxid
	if bk.Seq == 0 {
		if len(spends) != 1 {
			return fmt.Errorf("%w: seq 0 has %d inputs spending its parent, want one", ErrTokenInput, len(spends))
		}
		sp := spends[0]
		if !bytes.Equal(*sp.Output.LockingScript, FundingScript(mustLockingKey(ChainDerivation, bk.Owner))) {
			return fmt.Errorf("%w: seq 0 does not spend a chain-tree output of the owner", ErrTokenInput)
		}
		// 9. bucket-id.
		if bk.ID != BucketID(ptxid, sp.Vout) {
			return ErrBucketID
		}
		a.Spends = transaction.Outpoint{Txid: parent.Txid, Index: sp.Vout}
		return nil
	}
	var pred *Token
	spendsToken := 0
	for _, sp := range spends {
		if Classify(*sp.Output.LockingScript) != ClaimToken {
			continue
		}
		spendsToken++
		if p, err := readTokenOutput(sp.Output, sp.Vout); err == nil {
			pred = p
		}
	}
	if ptxid != bk.Prev || spendsToken != 1 || pred == nil || !samePredecessor(bk, pred.Bucket) {
		return fmt.Errorf("%w: the parent's token at seq - 1 is not spent exactly once", ErrTokenInput)
	}
	a.Predecessor = pred
	a.Spends = transaction.Outpoint{Txid: parent.Txid, Index: pred.Vout}
	return nil
}

// samePredecessor is rule 8's "a well-formed bucket token of the same
// bucket, owner and id at seq - 1".
func samePredecessor(t, p *Bucket) bool {
	return p.Bucket == t.Bucket && bytes.Equal(p.Owner, t.Owner) && p.ID == t.ID && p.Seq+1 == t.Seq
}

// readTokenOutput is the well-formed reading of one token output: it
// classifies as a bucket token, is a PushDrop of two fields and a signature
// holding 1 satoshi, its record decodes, its script is the canonical one
// under the key derived from the record's owner, and its field signature is
// strict and verifies.
func readTokenOutput(o *transaction.TransactionOutput, vout uint32) (*Token, error) {
	if o == nil || o.LockingScript == nil || Classify(*o.LockingScript) != ClaimToken {
		return nil, errNoPred
	}
	to, ok := chaintoken.ReadOutput(o, vout, 2)
	if !ok {
		return nil, errNoPred
	}
	bk, err := DecodeBucket(to.Fields[1])
	if err != nil {
		return nil, err
	}
	key := mustLockingKey(BucketDerivation, bk.Owner)
	if !to.LockedTo(*o.LockingScript, key) || !to.SignedBy(key) {
		return nil, errNoPred
	}
	return &Token{Bucket: bk, Record: to.Fields[1], Vout: vout}, nil
}

// ReadToken reads the one bucket token a transaction holds, well formed,
// without its parent: how an index and a reader read a stored token.
func ReadToken(tx *transaction.Transaction) (*Token, error) {
	var found *Token
	for i, o := range tx.Outputs {
		if o == nil || o.LockingScript == nil || Classify(*o.LockingScript) != ClaimToken {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("%w: two token outputs", ErrShape)
		}
		t, err := readTokenOutput(o, uint32(i))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrShape, err)
		}
		found = t
	}
	if found == nil {
		return nil, ErrNotBstore
	}
	return found, nil
}

// TokenBEEF assembles the BEEF a bucket token is admitted in (bcommon
// chaintoken.TokenBEEF): a replayer builds it from the token and its parent.
func TokenBEEF(token, parent *transaction.Transaction) ([]byte, error) {
	return chaintoken.TokenBEEF(token, parent)
}
