package publish

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// LocalPiece is a piece carrier a gateway hands to a provider in its own
// process (a storage node: s3d's local_provider) instead of sending it over
// HTTP. BEEF is the carrier as it would be sent, and Topic the blob topic it
// would be sent on.
//
// When Known is set, the rest is the gateway's own account of what it
// minted: the bucket, root, writer and piece of the record, the piece's
// SHA-256, the funding output the carrier spends and the carrier's txid. A
// provider configured to trust its gateway takes those as given instead of
// deriving them from the BEEF again; one that verifies checks the BEEF as it
// checks any submission. Piece is a slice of BEEF. Known is unset for an
// item read back from the outbox after a restart, which kept only its bytes.
type LocalPiece struct {
	Topic string
	BEEF  []byte

	Known      bool
	Bucket     string
	Root       [32]byte
	Writer     []byte
	Piece      []byte
	Hash       [32]byte
	Funding    transaction.Outpoint
	Commitment [32]byte
}

// Refused is how a local provider says it took nothing for a reason the same
// bytes will not change: the outbox neither counts the piece delivered there
// nor sends it again, as with a provider that answers a refusal over HTTP.
func Refused(why string) error { return fmt.Errorf("%w by the local provider: %s", errRefused, why) }

// localMeta is what a piece's local hand-off needs beyond its BEEF, kept in
// memory beside the outbox item from its mint to its send.
type localMeta struct {
	bucket     string
	root, hash [32]byte
	funding    transaction.Outpoint
	commitment [32]byte
	// at and n place the piece in the BEEF minted with it.
	at, n int
}

// sendLocal hands an item to the local provider (Delivery.Local).
func (o *Outbox) sendLocal(ctx context.Context, it *outItem, t Target, beef []byte) error {
	lp := LocalPiece{Topic: t.Topic, BEEF: beef}
	if m := it.local; m != nil && m.at >= 0 && m.at+m.n <= len(beef) {
		lp.Known, lp.Bucket, lp.Root, lp.Writer = true, m.bucket, m.root, o.writer
		lp.Piece, lp.Hash, lp.Funding, lp.Commitment = beef[m.at:m.at+m.n:m.at+m.n], m.hash, m.funding, m.commitment
	}
	return o.d.Local(ctx, lp)
}

// owesLocal reports whether the item is still owed to the local provider.
// A BEEF handed to it is not given back to the pool after the send: the
// provider keeps the piece by reference (its recent pieces, its closure
// walk), so the buffer is the provider's from then on, collected when it
// lets go.
func (it *outItem) owesLocal() bool {
	for _, t := range it.Targets {
		if t.Local {
			return true
		}
	}
	return false
}

// outpoint is a funding output from its tree's txid and index.
func outpoint(tree *chainhash.Hash, vout uint32) transaction.Outpoint {
	return transaction.Outpoint{Txid: *tree, Index: vout}
}
