package storerec

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/record"
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

// PieceAt is where the piece's n bytes begin in a record Encode wrote: its
// last field is key PKPiece, a byte string of n bytes, so the piece ends
// where the record does. It is -1 when the record does not end that way (a
// record with Extra keys, or n not its piece's length).
func PieceAt(rec []byte, n int) int {
	var buf [7]byte
	h := appendHead(appendHead(buf[:0], 0, PKPiece), 2, uint64(n))
	at := len(rec) - n
	if n < 1 || at < len(h) || !bytes.Equal(rec[at-len(h):at], h) {
		return -1
	}
	return at
}

// appendHead appends a CBOR head: the major type and the shortest encoding
// of n, as package cbor writes it.
func appendHead(b []byte, mt byte, n uint64) []byte {
	mt <<= 5
	switch {
	case n < 24:
		return append(b, mt|byte(n))
	case n <= 0xff:
		return append(b, mt|24, byte(n))
	case n <= 0xffff:
		return append(b, mt|25, byte(n>>8), byte(n))
	case n <= 0xffffffff:
		return append(b, mt|26, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(b, mt|27, byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32), byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

// Encode writes the piece canonically; it refuses what DecodePiece refuses.
// A piece without Extra is written directly, its 4 MiB appended once, and
// held to DecodePiece's rules without decoding it; one with Extra goes
// through the record codec and is read back.
func (p *Piece) Encode() ([]byte, error) {
	if len(p.Extra) == 0 {
		return p.encode()
	}
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

// encode is Encode for a piece without Extra: the canonical map of the five
// defined keys, in key order, as the record codec writes it, with the
// refusals in DecodePiece's order (the record's bound, then the bucket, the
// writer and the piece's length; the magic and the root cannot be wrong).
func (p *Piece) encode() ([]byte, error) {
	size, err := p.encodeChecked()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, size)
	out = p.appendHead(out)
	out = append(out, p.Piece...)
	if len(out) != size {
		return nil, fmt.Errorf("storerec: a piece record of %d bytes was sized at %d", len(out), size)
	}
	return out, nil
}

// encodeChecked runs encode's refusals in DecodePiece's order and returns the record's size.
func (p *Piece) encodeChecked() (int, error) {
	if !utf8.ValidString(p.Bucket) {
		return 0, fmt.Errorf("%w: %v", record.ErrCBOR, cbor.ErrUTF8)
	}
	var head [7]byte
	size := 1 + 1 + len(appendHead(head[:0], 2, uint64(len(MagicPiece)))) + len(MagicPiece) +
		1 + len(appendHead(head[:0], 3, uint64(len(p.Bucket)))) + len(p.Bucket) +
		1 + len(appendHead(head[:0], 2, uint64(len(p.Writer)))) + len(p.Writer) +
		1 + len(appendHead(head[:0], 2, 32)) + 32 +
		1 + len(appendHead(head[:0], 2, uint64(len(p.Piece)))) + len(p.Piece)
	if size > MaxPieceRecord {
		return 0, record.ErrTooLarge
	}
	if err := CheckBucket(p.Bucket); err != nil {
		return 0, err
	}
	if _, err := CheckKey(p.Writer); err != nil {
		return 0, fmt.Errorf("key %d: %w", PKWriter, err)
	}
	if len(p.Piece) < 1 || len(p.Piece) > MaxPiece {
		return 0, fmt.Errorf("%w: %d bytes", ErrPiece, len(p.Piece))
	}
	return size, nil
}

// EncodeHead is Encode without the piece's bytes, for a piece without Extra: the record is
// EncodeHead() followed by p.Piece, its last field, so a carrier can be minted over the two parts and
// the piece copied once, to where it is sent (carrier.MintOnce). The same refusals as Encode.
func (p *Piece) EncodeHead() ([]byte, error) {
	if len(p.Extra) != 0 {
		return nil, errors.New("storerec: EncodeHead is for a piece without Extra")
	}
	if _, err := p.encodeChecked(); err != nil {
		return nil, err
	}
	return p.appendHead(make([]byte, 0, 160)), nil
}

// appendHead appends the record up to the piece's bytes: the map head and every field but the piece's
// content, whose CBOR head is the last thing written.
func (p *Piece) appendHead(out []byte) []byte {
	out = appendHead(out, 5, 5)
	out = append(appendHead(appendHead(out, 0, PKMagic), 2, uint64(len(MagicPiece))), MagicPiece...)
	out = append(appendHead(appendHead(out, 0, PKBucket), 3, uint64(len(p.Bucket))), p.Bucket...)
	out = append(appendHead(appendHead(out, 0, PKWriter), 2, uint64(len(p.Writer))), p.Writer...)
	out = append(appendHead(appendHead(out, 0, PKRoot), 2, 32), p.Root[:]...)
	return appendHead(appendHead(out, 0, PKPiece), 2, uint64(len(p.Piece)))
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

// decodePieceInPlace is DecodePiece for a record a piece carrier holds,
// without copying the piece: when rec is the one encoding Encode writes of a
// piece without Extra, the Piece returned is a slice of rec (its Bucket,
// Writer and Root are copies), and rec must not change while it is used.
// Any other record, including every one DecodePiece refuses, is decoded by
// DecodePiece, so the verdict and the reason are always DecodePiece's.
func decodePieceInPlace(rec []byte) (*Piece, error) {
	if p := pieceInPlace(rec); p != nil {
		return p, nil
	}
	return DecodePiece(rec)
}

// pieceInPlace reads rec as the five keys Encode writes in order, each with
// its value, the piece's bytes ending the record, and returns the piece only
// when Encode would write exactly rec for it: every head in rec is the one
// encode writes for the value read, and the value meets encode's rules. A
// record that does not is nil, for DecodePiece to read.
func pieceInPlace(rec []byte) *Piece {
	if len(rec) > MaxPieceRecord {
		return nil
	}
	r := rec
	// next reads a CBOR head of major type mt and the n bytes it declares
	// when string is true.
	next := func(mt byte, str bool) (uint64, []byte, bool) {
		if len(r) == 0 || r[0]>>5 != mt {
			return 0, nil, false
		}
		ai, n, w := r[0]&31, uint64(0), 1
		switch {
		case ai < 24:
			n = uint64(ai)
		case ai <= 27:
			w += 1 << (ai - 24)
			if len(r) < w {
				return 0, nil, false
			}
			for _, c := range r[1:w] {
				n = n<<8 | uint64(c)
			}
		default:
			return 0, nil, false
		}
		r = r[w:]
		if !str {
			return n, nil, true
		}
		if n > uint64(len(r)) {
			return 0, nil, false
		}
		v := r[:n]
		r = r[n:]
		return n, v, true
	}
	var vals [5][]byte
	if n, _, ok := next(5, false); !ok || n != 5 {
		return nil
	}
	for k := range uint64(5) {
		if n, _, ok := next(0, false); !ok || n != k {
			return nil
		}
		mt := byte(2)
		if k == PKBucket {
			mt = 3
		}
		_, v, ok := next(mt, true)
		if !ok {
			return nil
		}
		vals[k] = v
	}
	if len(r) != 0 || !bytes.Equal(vals[PKMagic], MagicPiece) || len(vals[PKRoot]) != 32 {
		return nil
	}
	p := &Piece{Bucket: string(vals[PKBucket]), Writer: bytes.Clone(vals[PKWriter]), Root: [32]byte(vals[PKRoot]), Piece: vals[PKPiece]}
	// encode's rules, in its order, and its heads: the shortest of each.
	if !utf8.ValidString(p.Bucket) || CheckBucket(p.Bucket) != nil || len(p.Piece) < 1 || len(p.Piece) > MaxPiece {
		return nil
	}
	if _, err := CheckKey(p.Writer); err != nil {
		return nil
	}
	at := len(rec) - len(p.Piece)
	var head [96]byte
	h := appendHead(head[:0], 5, 5)
	h = append(appendHead(appendHead(h, 0, PKMagic), 2, uint64(len(MagicPiece))), MagicPiece...)
	h = append(appendHead(appendHead(h, 0, PKBucket), 3, uint64(len(p.Bucket))), p.Bucket...)
	h = append(appendHead(appendHead(h, 0, PKWriter), 2, uint64(len(p.Writer))), p.Writer...)
	h = append(appendHead(appendHead(h, 0, PKRoot), 2, 32), p.Root[:]...)
	h = appendHead(appendHead(h, 0, PKPiece), 2, uint64(len(p.Piece)))
	if !bytes.Equal(rec[:at], h) {
		return nil
	}
	return p
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
