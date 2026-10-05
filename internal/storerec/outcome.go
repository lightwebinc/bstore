package storerec

import (
	"crypto/sha256"
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
)

// Outcome is the outcome record (magic sto 0x01).
type Outcome struct {
	Bucket  string
	Buyer   []byte
	Created uint64
	// Request is the request's exact encoding; Req is it decoded.
	Request []byte
	Req     *Request
	Outcome uint64
	// Response and Signature, for outcomes 1 and 2 only.
	Response  *[32]byte
	Signature []byte
	Extra     cbor.Map
}

// Encode writes the outcome canonically; it refuses what DecodeOutcome
// refuses.
func (o *Outcome) Encode() ([]byte, error) {
	m := cbor.Map{
		{Key: OKMagic, Val: MagicOutcome},
		{Key: OKBucket, Val: o.Bucket},
		{Key: OKBuyer, Val: o.Buyer},
		{Key: OKCreated, Val: o.Created},
		{Key: OKRequest, Val: o.Request},
		{Key: OKOutcome, Val: o.Outcome},
	}
	if o.Response != nil {
		m = append(m, cbor.Pair{Key: OKResponse, Val: b(*o.Response)})
	}
	if o.Signature != nil {
		m = append(m, cbor.Pair{Key: OKSignature, Val: o.Signature})
	}
	out, err := encodeRecord(m, o.Extra, OKLast, MaxOutcome)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeOutcome(out); err != nil {
		return nil, err
	}
	return out, nil
}

// DecodeOutcome reads an outcome record in the order of spec section 8.1:
// too-large, cbor, key-type, magic, per key missing, type and its rule
// (bucket, identity, created, request, outcome; response and signature,
// where present, by type), then kind (keys 6 and 7 present exactly for
// outcomes 1 and 2), then response-signature (for outcomes 1 and 2, the
// signature is strict low-S DER and verifies over SHA-256(request) ||
// response under derive(answerer, "challenge"), answerer from the request).
func DecodeOutcome(raw []byte) (*Outcome, error) {
	f, err := readRecord(raw, MaxOutcome, OKLast, MagicOutcome)
	if err != nil {
		return nil, err
	}
	o := &Outcome{Extra: f.extra()}
	if o.Bucket, err = f.bucket(OKBucket); err != nil {
		return nil, err
	}
	if o.Buyer, err = f.key(OKBuyer); err != nil {
		return nil, err
	}
	if o.Created, err = f.created(OKCreated); err != nil {
		return nil, err
	}
	if o.Request, err = f.bytes(OKRequest); err != nil {
		return nil, err
	}
	if o.Req, err = DecodeRequest(o.Request); err != nil {
		return nil, err
	}
	if o.Outcome, err = f.uint(OKOutcome, OutcomePass, OutcomeUnretrievable, ErrOutcome); err != nil {
		return nil, err
	}
	if f.has(OKResponse) {
		r, err := f.b32(OKResponse)
		if err != nil {
			return nil, err
		}
		o.Response = &r
	}
	if f.has(OKSignature) {
		if o.Signature, err = f.bytes(OKSignature); err != nil {
			return nil, err
		}
	}
	signed := o.Outcome == OutcomePass || o.Outcome == OutcomeInvalid
	if signed != f.has(OKResponse) || signed != f.has(OKSignature) {
		return nil, fmt.Errorf("%w: outcome %d and its response keys", ErrKind, o.Outcome)
	}
	if signed && !VerifyChallengeSig(o.Req.Answerer, sha256.Sum256(o.Request), *o.Response, o.Signature) {
		return nil, ErrRespSig
	}
	return o, nil
}
