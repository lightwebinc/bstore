package storerec

import (
	"crypto/sha256"
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
)

// Piece is one CHIRP object of one closure (magic stp 0x01).
type Piece struct {
	Bucket string
	Writer []byte
	Root   [32]byte
	Piece  []byte
	Extra  cbor.Map
}

// Hash is the piece's identity, SHA-256 of its bytes.
func (p *Piece) Hash() [32]byte { return sha256.Sum256(p.Piece) }

// Encode writes the piece canonically; it refuses what DecodePiece refuses.
func (p *Piece) Encode() ([]byte, error) {
	out, err := encodeRecord(cbor.Map{
		{Key: PKMagic, Val: MagicPiece},
		{Key: PKBucket, Val: p.Bucket},
		{Key: PKWriter, Val: p.Writer},
		{Key: PKRoot, Val: b(p.Root)},
		{Key: PKPiece, Val: p.Piece},
	}, p.Extra, PKLast, MaxPieceRecord)
	if err != nil {
		return nil, err
	}
	if _, err := DecodePiece(out); err != nil {
		return nil, err
	}
	return out, nil
}

// DecodePiece reads a piece: too-large (over 4,194,560 bytes), cbor,
// key-type, magic, then per key missing, type and its rule: bucket, identity
// (writer), root bytes(32), piece (1 to 4,194,304 bytes). Spec section 5
// lists no order; this is the one every other record follows.
func DecodePiece(raw []byte) (*Piece, error) {
	f, err := readRecord(raw, MaxPieceRecord, PKLast, MagicPiece)
	if err != nil {
		return nil, err
	}
	p := &Piece{Extra: f.extra()}
	if p.Bucket, err = f.bucket(PKBucket); err != nil {
		return nil, err
	}
	if p.Writer, err = f.key(PKWriter); err != nil {
		return nil, err
	}
	if p.Root, err = f.b32(PKRoot); err != nil {
		return nil, err
	}
	if p.Piece, err = f.bytes(PKPiece); err != nil {
		return nil, err
	}
	if len(p.Piece) < 1 || len(p.Piece) > MaxPiece {
		return nil, fmt.Errorf("%w: %d bytes", ErrPiece, len(p.Piece))
	}
	return p, nil
}

// Front is the front statement (magic stf 0x01).
type Front struct {
	Bucket     string
	Front      []byte
	Advertiser []byte
	URL        string
	Created    uint64
	Extra      cbor.Map
}

// Encode writes the statement canonically; it refuses what DecodeFront
// refuses.
func (s *Front) Encode() ([]byte, error) {
	out, err := encodeRecord(cbor.Map{
		{Key: FKMagic, Val: MagicFront},
		{Key: FKBucket, Val: s.Bucket},
		{Key: FKFront, Val: s.Front},
		{Key: FKAdvertiser, Val: s.Advertiser},
		{Key: FKURL, Val: s.URL},
		{Key: FKCreated, Val: s.Created},
	}, s.Extra, FKLast, MaxFront)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeFront(out); err != nil {
		return nil, err
	}
	return out, nil
}

// DecodeFront reads a front statement in the order of spec section 8.2:
// too-large, cbor, key-type, magic, per key missing, type and its rule
// (bucket, identity, identity, url, created).
func DecodeFront(raw []byte) (*Front, error) {
	f, err := readRecord(raw, MaxFront, FKLast, MagicFront)
	if err != nil {
		return nil, err
	}
	s := &Front{Extra: f.extra()}
	if s.Bucket, err = f.bucket(FKBucket); err != nil {
		return nil, err
	}
	if s.Front, err = f.key(FKFront); err != nil {
		return nil, err
	}
	if s.Advertiser, err = f.key(FKAdvertiser); err != nil {
		return nil, err
	}
	if s.URL, err = f.text(FKURL); err != nil {
		return nil, err
	}
	if err := CheckFrontURL(s.URL); err != nil {
		return nil, err
	}
	if s.Created, err = f.created(FKCreated); err != nil {
		return nil, err
	}
	return s, nil
}
