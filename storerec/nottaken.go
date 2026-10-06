package storerec

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/record"
)

// The "payment not taken" note (frozen.md section 14, row C30): a provider that refuses a priced
// challenge whose request came with a BRC-105 payment, and did not take
// that payment, says so in a note signed under its challenge key,
// derive(answerer, "challenge"). The buyer that holds such a note for its
// own payment releases the payment's coin at once, instead of holding it
// until the node shows the payment was never broadcast.
//
// The note's body is a record under the magic "stn" 0x01:
//
//	0 magic     "stn" 0x01
//	1 payment   32 bytes, the payment's txid, hash byte order
//	2 request   32 bytes, SHA-256 of the request the payment came with
//	3 reason    text, 1 to 64 of a-z, 0-9 and "-": why nothing was taken
//	4 time      unsigned, Unix seconds, 0 to 2^32 - 1, the provider's clock
//	5 answerer  33 bytes, the answerer's identity key
//
// The signed note is the CBOR array [body, signature]: a strict low-S DER
// signature over SHA-256(body) (BRC-100 createSignature over the body)
// under derive(answerer, "challenge"), at most 512 bytes in all. A
// response's signature covers 64 bytes (two hashes) and a note's body is
// always longer, so one can never be taken for the other.

// MagicNotTaken is the not-taken note's magic.
var MagicNotTaken = []byte{'s', 't', 'n', 0x01}

// Record keys of the not-taken note.
const (
	NKMagic uint64 = iota
	NKPayment
	NKRequest
	NKReason
	NKTime
	NKAnswerer
	NKLast = NKAnswerer
)

// MaxNotTaken bounds a signed note; MaxNoteReason a reason.
const (
	MaxNotTaken   = 512
	MaxNoteReason = 64
)

// ErrNotTaken is a signed note that does not decode or verify; its rule
// names the step.
var ErrNotTaken = errors.New("storerec: not a payment-not-taken note under row C30")

var (
	errNoteReason = errors.New("storerec: reason breaks its rule")
	errNoteTime   = errors.New("storerec: time out of range")
)

// NotTaken is the body of a payment-not-taken note.
type NotTaken struct {
	// Payment is the payment's txid, hash byte order; Request the SHA-256
	// of the request it came with.
	Payment  [32]byte
	Request  [32]byte
	Reason   string
	Time     uint64
	Answerer []byte
}

// Encode writes the body, checked by DecodeNotTakenBody.
func (n *NotTaken) Encode() ([]byte, error) {
	m := cbor.Map{
		{Key: NKMagic, Val: MagicNotTaken},
		{Key: NKPayment, Val: b(n.Payment)},
		{Key: NKRequest, Val: b(n.Request)},
		{Key: NKReason, Val: n.Reason},
		{Key: NKTime, Val: n.Time},
		{Key: NKAnswerer, Val: n.Answerer},
	}
	out, err := record.Encode(m, nil, MaxNotTaken)
	if err != nil {
		return nil, asNote(err)
	}
	if _, err := DecodeNotTakenBody(out); err != nil {
		return nil, err
	}
	return out, nil
}

func asNote(err error) error {
	if err == nil {
		return nil
	}
	rule := Reason(err)
	switch {
	case errors.Is(err, errNoteReason):
		rule = "reason"
	case errors.Is(err, errNoteTime):
		rule = "time"
	case errors.Is(err, errUnknownKey):
		rule = "unknown-key"
	}
	return fmt.Errorf("%w: %v", ruled(ErrNotTaken, rule), err)
}

// DecodeNotTakenBody reads a note's body. Every refusal is ErrNotTaken; its
// rule is, in order: too-large, cbor, key-type, magic, unknown-key (a key
// above 5), then per key missing, type and its rule: reason, time,
// identity (the answerer).
func DecodeNotTakenBody(raw []byte) (*NotTaken, error) {
	n, err := decodeNotTaken(raw)
	return n, asNote(err)
}

func decodeNotTaken(raw []byte) (*NotTaken, error) {
	f, err := readRecord(raw, MaxNotTaken, NKLast, MagicNotTaken)
	if err != nil {
		return nil, err
	}
	if len(f.extra()) > 0 {
		return nil, errUnknownKey
	}
	n := &NotTaken{}
	if n.Payment, err = f.b32(NKPayment); err != nil {
		return nil, err
	}
	if n.Request, err = f.b32(NKRequest); err != nil {
		return nil, err
	}
	if n.Reason, err = f.text(NKReason); err != nil {
		return nil, err
	}
	if !noteReason(n.Reason) {
		return nil, errNoteReason
	}
	if n.Time, err = f.uint(NKTime, 0, MaxU32, errNoteTime); err != nil {
		return nil, err
	}
	if n.Answerer, err = f.key(NKAnswerer); err != nil {
		return nil, err
	}
	return n, nil
}

func noteReason(s string) bool {
	if len(s) < 1 || len(s) > MaxNoteReason {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// SignNotTaken signs a note's body with the answerer's derived challenge
// key and returns the signed note [body, signature].
func SignNotTaken(priv *ec.PrivateKey, body []byte) ([]byte, error) {
	d := sha256.Sum256(body)
	sig, err := priv.Sign(d[:])
	if err != nil {
		return nil, err
	}
	return cbor.Encode([]cbor.Value{body, sig.Serialize()})
}

// SignedNotTaken wraps a body and its signature as a signed note.
func SignedNotTaken(body, sig []byte) ([]byte, error) {
	out, err := cbor.Encode([]cbor.Value{body, sig})
	if err != nil {
		return nil, err
	}
	if len(out) > MaxNotTaken {
		return nil, ruled(ErrNotTaken, "too-large")
	}
	return out, nil
}

// VerifyNotTaken reads a signed note and checks its signature under
// derive(answerer, "challenge") of the answerer the body names. Its rules,
// beyond the body's: too-large (over 512 bytes in all), shape (not
// [body, signature] as byte strings), signature (not strict low-S DER, or
// not verifying).
func VerifyNotTaken(signed []byte) (*NotTaken, error) {
	if len(signed) > MaxNotTaken {
		return nil, ruled(ErrNotTaken, "too-large")
	}
	v, err := cbor.DecodeValue(signed)
	if err != nil {
		return nil, ruled(ErrNotTaken, "shape")
	}
	a, ok := v.([]cbor.Value)
	if !ok || len(a) != 2 {
		return nil, ruled(ErrNotTaken, "shape")
	}
	body, ok1 := a[0].([]byte)
	sig, ok2 := a[1].([]byte)
	if !ok1 || !ok2 {
		return nil, ruled(ErrNotTaken, "shape")
	}
	n, err := DecodeNotTakenBody(body)
	if err != nil {
		return nil, err
	}
	if chaintoken.CheckDER(sig) != nil {
		return nil, ruled(ErrNotTaken, "signature")
	}
	key, err := ChallengeKey(n.Answerer)
	if err != nil {
		return nil, ruled(ErrNotTaken, "signature")
	}
	s, err := ec.ParseDERSignature(sig)
	if err != nil {
		return nil, ruled(ErrNotTaken, "signature")
	}
	d := sha256.Sum256(body)
	if !s.Verify(d[:], key) {
		return nil, ruled(ErrNotTaken, "signature")
	}
	return n, nil
}

// CheckNotTaken is the buyer's check of a note it was handed for its own
// payment: it verifies, and names the payment (hash byte order), the
// request it sent and the answerer it asked. Anything else is refused, and
// the payment's coin stays held.
func CheckNotTaken(signed []byte, payment [32]byte, request []byte, answerer []byte) (*NotTaken, error) {
	n, err := VerifyNotTaken(signed)
	if err != nil {
		return nil, err
	}
	switch {
	case n.Payment != payment:
		return nil, ruled(ErrNotTaken, "payment")
	case n.Request != sha256.Sum256(request):
		return nil, ruled(ErrNotTaken, "request")
	case !bytes.Equal(n.Answerer, answerer):
		return nil, ruled(ErrNotTaken, "answerer")
	}
	return n, nil
}
