package storerec

import (
	"bytes"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/pushdrop"
	"github.com/lightwebinc/bcommon/record"
)

// Claim names what an output claims to be (spec section 12.1, Classify).
type Claim string

const (
	ClaimNone    Claim = ""
	ClaimToken   Claim = "bucket-token"
	ClaimEntry   Claim = "entry"
	ClaimOutcome Claim = "outcome"
	ClaimFront   Claim = "front"
	ClaimPiece   Claim = "piece"
	ClaimFunding Claim = "funding"
	ClaimHold    Claim = "hold"
)

// Claims reports whether a claim is one of the claiming kinds (a token or a
// record), as against a shape (funding, hold) or nothing.
func (c Claim) Claims() bool {
	switch c {
	case ClaimToken, ClaimEntry, ClaimOutcome, ClaimFront, ClaimPiece:
		return true
	}
	return false
}

// Classify says what a locking script claims, reading nothing else of it:
// 0x21, 33 bytes, 0xac, then one push whose data is exactly 73 74 01 (a
// bucket token) or starts with a CBOR definite-length map head, 00 44 and
// the magic of an entry, an outcome, a front statement or a piece; or the
// whole script 21 <33> ac 03 73 74 02 75 (funding-shaped) or 21 <33> ac 03
// 73 74 03 75 (hold-shaped).
func Classify(s []byte) Claim {
	p, ok := pushdrop.FirstPush(s)
	if !ok {
		return ClaimNone
	}
	switch {
	case bytes.Equal(p, TagToken):
		return ClaimToken
	case record.Claims(p, MagicEntry):
		return ClaimEntry
	case record.Claims(p, MagicOutcome):
		return ClaimOutcome
	case record.Claims(p, MagicFront):
		return ClaimFront
	case record.Claims(p, MagicPiece):
		return ClaimPiece
	case len(s) == 40 && s[35] == 0x03 && s[39] == 0x75 && bytes.Equal(p, TagFunding):
		return ClaimFunding
	case len(s) == 40 && s[35] == 0x03 && s[39] == 0x75 && bytes.Equal(p, TagHold):
		return ClaimHold
	}
	return ClaimNone
}

// FundingScript is a funding output locked to key: <key> OP_CHECKSIG <73 74
// 02> OP_DROP, unsigned.
func FundingScript(key *ec.PublicKey) []byte { return pushdrop.Script(key, [][]byte{TagFunding}, nil) }

// HoldScript is a hold output locked to key: <key> OP_CHECKSIG <73 74 03>
// OP_DROP.
func HoldScript(key *ec.PublicKey) []byte { return pushdrop.Script(key, [][]byte{TagHold}, nil) }

// LockingKey is the key an identity's output is locked to under d, derived
// from the identity key alone (counterparty anyone). A key derived is
// remembered (keycache.go).
func LockingKey(d pushdrop.Derivation, identity []byte) (*ec.PublicKey, error) {
	if len(identity) != 33 {
		return nil, ErrIdentity // guard.ParsePubKey's first rule
	}
	ref := lockingRef{d: d, id: [33]byte(identity)}
	if k, ok := lockingKeys.get(ref); ok {
		return k, nil
	}
	pub, err := guard.ParsePubKey(identity)
	if err != nil {
		return nil, ErrIdentity
	}
	k, err := d.ExpectedLockingKey(pub)
	if err != nil {
		return nil, err
	}
	lockingKeys.put(ref, k)
	return k, nil
}

func mustLockingKey(d pushdrop.Derivation, identity []byte) *ec.PublicKey {
	k, err := LockingKey(d, identity)
	if err != nil {
		panic(err) // the record's key rule already held the identity to a canonical key
	}
	return k
}
