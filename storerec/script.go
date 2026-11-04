package storerec

import (
	"bytes"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"

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

// isLock reports whether s is exactly pushdrop.Script(key, fields, sig),
// read in place: the key's push and OP_CHECKSIG, each field and then the
// signature in the push pushdrop's template writes for it, and the drops
// that clear them, with nothing after. Rebuilding the script to compare it
// copies every field twice, which for a piece carrier is 8 MiB.
func isLock(s []byte, key *ec.PublicKey, fields [][]byte, sig []byte) bool {
	if len(s) < 35 || s[0] != 33 || !bytes.Equal(s[1:34], key.Compressed()) || s[34] != script.OpCHECKSIG {
		return false
	}
	s = s[35:]
	n := len(fields)
	var ok bool
	for _, f := range fields {
		if s, ok = cutPush(s, f); !ok {
			return false
		}
	}
	if sig != nil {
		if s, ok = cutPush(s, sig); !ok {
			return false
		}
		n++
	}
	for ; n > 1; n -= 2 {
		if s, ok = cutOp(s, script.Op2DROP); !ok {
			return false
		}
	}
	if n == 1 {
		if s, ok = cutOp(s, script.OpDROP); !ok {
			return false
		}
	}
	return len(s) == 0
}

// cutPush cuts d's push from the front of s, as pushdrop's template writes
// it (go-sdk pushdrop.CreateMinimallyEncodedScriptChunk): OP_0 for no bytes
// or a zero byte, OP_1 to OP_16 and OP_1NEGATE for those single bytes, then
// a direct push up to 75 bytes and OP_PUSHDATA1, 2 or 4 by length.
func cutPush(s, d []byte) ([]byte, bool) {
	n := len(d)
	var head [5]byte
	var h []byte
	switch {
	case n == 0 || (n == 1 && d[0] == 0):
		return cutOp(s, script.Op0)
	case n == 1 && d[0] >= 1 && d[0] <= 16:
		return cutOp(s, script.Op1-1+d[0])
	case n == 1 && d[0] == 0x81:
		return cutOp(s, script.Op1NEGATE)
	case n <= 75:
		h = append(head[:0], byte(n))
	case n <= 0xff:
		h = append(head[:0], script.OpPUSHDATA1, byte(n))
	case n <= 0xffff:
		h = append(head[:0], script.OpPUSHDATA2, byte(n), byte(n>>8))
	case uint64(n) <= 0xffffffff:
		h = append(head[:0], script.OpPUSHDATA4, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
	default:
		return nil, false
	}
	if len(s) < len(h)+n || !bytes.Equal(s[:len(h)], h) || !bytes.Equal(s[len(h):len(h)+n], d) {
		return nil, false
	}
	return s[len(h)+n:], true
}

// cutOp cuts the one opcode op from the front of s.
func cutOp(s []byte, op byte) ([]byte, bool) {
	if len(s) == 0 || s[0] != op {
		return nil, false
	}
	return s[1:], true
}
