package storerec

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/pushdrop"
)

// PieceCarrier is a piece carrier a provider took from a blob topic: the
// piece, its carrier's commitment and the funding outpoint it spends.
type PieceCarrier struct {
	Piece      *Piece
	Commitment [32]byte
	Funding    transaction.Outpoint
}

// CheckPieceCarrier holds a submission on a blob topic to a piece carrier's
// rules, as a host holds an entry carrier to its own (spec sections 5, 12.1
// and 13.2): the carrier and its funding tree's minimal path, within the
// piece carrier's ceiling of 4,259,840 bytes; one output claiming a piece,
// whose record is a piece of this bucket; not mineable; one canonical
// signature push; the one-field signed PushDrop under derive(writer,
// "piece"); its field signature; and the funding output of the writer it
// spends, whose value it carries. When headers is set the funding tree's
// proof is checked against them as well: a provider takes pieces only from
// a writer that paid for a mined funding output.
//
// No host admits a piece; this is a provider's check before it stores the
// bytes, so that what it keeps is a writer's signed piece and not anyone's
// junk. The bytes are trusted for nothing else: an object is named by its
// hash, and the closure check decides what it is.
func CheckPieceCarrier(ctx context.Context, beef []byte, bucket string, headers chaintracker.ChainTracker) (*PieceCarrier, error) {
	if CheckBucket(bucket) != nil {
		return nil, fmt.Errorf("%w: bucket identifier breaks the grammar", ErrHostConfig)
	}
	if len(beef) > MaxPieceBEEF {
		return nil, fmt.Errorf("%w: a piece carrier's BEEF of %d bytes", ErrBEEF, len(beef))
	}
	w, err := chaintoken.ReadWire(beef, MaxPieceBEEF)
	if err != nil {
		return nil, err
	}
	tx := w.SubjectTx()
	if tx == nil {
		return nil, fmt.Errorf("%w: no subject transaction", ErrBEEF)
	}
	// The wire's subject is the hash of the transaction carried (ReadWire
	// held it to be: the subject names an entry, and each entry's txid is
	// hashed from its bytes), and nothing below changes the transaction. Set
	// as its cached txid, the shape check and the commitment read it rather
	// than each serializing and hashing the 4 MiB again.
	subject := w.Subject
	tx.SetTxHash(&subject)
	for _, e := range w.Entries {
		if e.Tx == nil {
			continue
		}
		for _, o := range e.Tx.Outputs {
			if o != nil && o.Satoshis > MaxSafe {
				return nil, fmt.Errorf("%w: an output of %d satoshis", ErrBEEF, o.Satoshis)
			}
		}
	}
	// 1. shape: one input, one output, and that output claims a piece.
	if len(tx.Inputs) != 1 || len(tx.Outputs) != 1 {
		return nil, fmt.Errorf("%w: %d inputs, %d outputs", ErrShape, len(tx.Inputs), len(tx.Outputs))
	}
	in, out := tx.Inputs[0], tx.Outputs[0]
	if out == nil || out.LockingScript == nil || Classify(*out.LockingScript) != ClaimPiece {
		return nil, fmt.Errorf("%w: the output does not claim a piece", ErrShape)
	}
	// 2. beef.
	tree, err := w.Carrier(tx)
	if err != nil {
		return nil, err
	}
	// The funding tree's txid, which ReadWire hashed from its bytes, cached
	// the same way for the proof check (Mined), which would otherwise
	// serialize and hash the tree again.
	tree.Tx.SetTxHash(&tree.Txid)
	// 3. the record's own rules. The record is read in place: the piece is
	// a slice of the locking script, not a copy (decodePieceInPlace).
	rec, _ := pushdrop.FirstPush(*out.LockingScript)
	p, err := decodePieceInPlace(rec)
	if err != nil {
		return nil, err
	}
	// 4. bucket.
	if p.Bucket != bucket {
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
	// 7. lock.
	key := mustLockingKey(PieceDerivation, p.Writer)
	fields, _ := pushdrop.Fields(*out.LockingScript)
	if len(fields) != 2 {
		return nil, fmt.Errorf("%w: %d pushes, want the record and a signature", ErrLock, len(fields))
	}
	// Compared with the lock pushdrop.Script writes, in place: rebuilding it
	// would copy the 4 MiB record twice.
	if !isLock(*out.LockingScript, key, [][]byte{rec}, fields[1]) {
		return nil, ErrLock
	}
	// 8. signature.
	if !chaintoken.VerifyField(key, rec, fields[1]) {
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
	if headers != nil && !chaintoken.Mined(ctx, tree.Tx, headers) {
		return nil, ErrUnmined
	}
	return &PieceCarrier{Piece: p, Commitment: carrier.Commitment(tx),
		Funding: transaction.Outpoint{Txid: tree.Txid, Index: in.SourceTxOutIndex}}, nil
}
