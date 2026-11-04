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

// CheckPieceCarrierRef is the piece carrier check as it was before the
// provider's ingest stopped copying the piece: the lock rebuilt and
// compared byte for byte, the record decoded by DecodePiece. The tests hold
// CheckPieceCarrier to its verdict on every input they make.
var CheckPieceCarrierRef = checkPieceCarrierRef

func checkPieceCarrierRef(ctx context.Context, beef []byte, bucket string, headers chaintracker.ChainTracker) (*PieceCarrier, error) {
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
	// 3. the record's own rules.
	rec, _ := pushdrop.FirstPush(*out.LockingScript)
	p, err := DecodePiece(rec)
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
	if !bytes.Equal(*out.LockingScript, pushdrop.Script(key, [][]byte{rec}, fields[1])) {
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
