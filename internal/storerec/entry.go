package storerec

import (
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
)

// Hold is an entry's hold terms (spec section 4.3).
type Hold struct {
	Txid      [32]byte // hash byte order
	Vout      uint64
	Satoshis  uint64
	T         uint64
	K         uint64
	Retention uint64
	Deadline  uint64
	CheckC    uint64
}

// MetaPair is one pair of an entry's meta map.
type MetaPair struct {
	Key, Value string
}

// Entry is the object entry (magic ste 0x01).
type Entry struct {
	Bucket  string
	ID      [32]byte
	Writer  []byte
	Key     string
	Kind    uint64
	Created uint64

	// A put's keys.
	Size         uint64
	Root         [32]byte
	ContentHash  [32]byte
	ProofRoot    [32]byte
	ProofProfile uint64
	ContentType  string
	ETag         *string
	Meta         []MetaPair // nil when absent; ascending by key as CBOR orders them
	Hold         *Hold

	// Unknown holds, for a kind from 3 to 255, its keys 7 to 15 as read,
	// unchecked, so that it re-encodes verbatim.
	Unknown cbor.Map
	// Extra holds keys 16 and above, verbatim.
	Extra cbor.Map
}

func (h *Hold) value() cbor.Value {
	return []cbor.Value{b(h.Txid), h.Vout, h.Satoshis, h.T, h.K, h.Retention, h.Deadline, h.CheckC}
}

func metaValue(m []MetaPair) cbor.Map {
	out := make(cbor.Map, len(m))
	for i, p := range m {
		out[i] = cbor.Pair{Key: p.Key, Val: p.Value}
	}
	return out
}

// Encode writes the entry canonically; it refuses what DecodeEntry refuses.
func (e *Entry) Encode() ([]byte, error) {
	m := cbor.Map{
		{Key: EKMagic, Val: MagicEntry},
		{Key: EKBucket, Val: e.Bucket},
		{Key: EKID, Val: b(e.ID)},
		{Key: EKWriter, Val: e.Writer},
		{Key: EKKey, Val: e.Key},
		{Key: EKKind, Val: e.Kind},
		{Key: EKCreated, Val: e.Created},
	}
	switch e.Kind {
	case KindPut:
		m = append(m,
			cbor.Pair{Key: EKSize, Val: e.Size},
			cbor.Pair{Key: EKRoot, Val: b(e.Root)},
			cbor.Pair{Key: EKContentHash, Val: b(e.ContentHash)},
			cbor.Pair{Key: EKProofRoot, Val: b(e.ProofRoot)},
			cbor.Pair{Key: EKProofProfile, Val: e.ProofProfile},
			cbor.Pair{Key: EKContentType, Val: e.ContentType})
		if e.ETag != nil {
			m = append(m, cbor.Pair{Key: EKETag, Val: *e.ETag})
		}
		if e.Meta != nil {
			m = append(m, cbor.Pair{Key: EKMeta, Val: metaValue(e.Meta)})
		}
		if e.Hold != nil {
			m = append(m, cbor.Pair{Key: EKHold, Val: e.Hold.value()})
		}
	case KindDelete:
	default:
		m = append(m, e.Unknown...)
	}
	out, err := encodeRecord(m, e.Extra, EKLast, MaxEntry)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeEntry(out); err != nil {
		return nil, err
	}
	return out, nil
}

// DecodeEntry reads an object entry in the order of spec section 4.2:
// too-large, cbor, key-type, magic, then keys 0 to 6 in key order (missing,
// type, own rule); then, for a put, keys 7 to 15 in key order where present
// (type, own rule); then kind (a key the kind does not carry is present, or
// one it carries is missing); then empty. A delete's keys 7 to 15 are not
// read: their presence is kind. A kind from 3 to 255 is checked for keys 0
// to 6 only and its keys 7 to 15 kept as read.
func DecodeEntry(raw []byte) (*Entry, error) {
	f, err := readRecord(raw, MaxEntry, EKLast, MagicEntry)
	if err != nil {
		return nil, err
	}
	e := &Entry{Extra: f.extra()}
	if e.Bucket, err = f.bucket(EKBucket); err != nil {
		return nil, err
	}
	if e.ID, err = f.b32(EKID); err != nil {
		return nil, err
	}
	if e.Writer, err = f.key(EKWriter); err != nil {
		return nil, err
	}
	key, err := f.text(EKKey)
	if err != nil {
		return nil, err
	}
	if err := CheckObjectKey(key); err != nil {
		return nil, err
	}
	e.Key = key
	if e.Kind, err = f.uint(EKKind, 1, MaxKind, ErrKind); err != nil {
		return nil, err
	}
	if e.Created, err = f.created(EKCreated); err != nil {
		return nil, err
	}
	switch e.Kind {
	case KindPut:
		if err := e.readPut(f); err != nil {
			return nil, err
		}
	case KindDelete:
		for k := EKSize; k <= EKLast; k++ {
			if f.has(k) {
				return nil, fmt.Errorf("%w: a delete carries key %d", ErrKind, k)
			}
		}
	default:
		for k := EKSize; k <= EKLast; k++ {
			if v, ok := f.f.Get(k); ok {
				e.Unknown = append(e.Unknown, cbor.Pair{Key: k, Val: v})
			}
		}
	}
	return e, nil
}

func (e *Entry) readPut(f fields) error {
	var err error
	if f.has(EKSize) {
		if e.Size, err = f.uint(EKSize, 0, MaxSize, ErrSize); err != nil {
			return err
		}
	}
	for _, k := range []struct {
		key uint64
		dst *[32]byte
	}{{EKRoot, &e.Root}, {EKContentHash, &e.ContentHash}, {EKProofRoot, &e.ProofRoot}} {
		if f.has(k.key) {
			if *k.dst, err = f.b32(k.key); err != nil {
				return err
			}
		}
	}
	if f.has(EKProofProfile) {
		if e.ProofProfile, err = f.uint(EKProofProfile, ProofProfile, ProofProfile, ErrProfile); err != nil {
			return err
		}
	}
	if f.has(EKContentType) {
		if e.ContentType, err = f.text(EKContentType); err != nil {
			return err
		}
		if err := CheckContentType(e.ContentType); err != nil {
			return err
		}
	}
	if f.has(EKETag) {
		s, err := f.text(EKETag)
		if err != nil {
			return err
		}
		if err := CheckETag(s); err != nil {
			return err
		}
		e.ETag = &s
	}
	if f.has(EKMeta) {
		if e.Meta, err = readMeta(f); err != nil {
			return err
		}
	}
	if f.has(EKHold) {
		a, err := f.array(EKHold)
		if err != nil {
			return err
		}
		h, err := ReadHold(a)
		if err != nil {
			return err
		}
		e.Hold = h
	}
	for k := EKSize; k <= EKContentType; k++ {
		if !f.has(k) {
			return fmt.Errorf("%w: a put lacks key %d", ErrKind, k)
		}
	}
	if e.Size == 0 {
		if e.Root != EmptyRoot || e.ContentHash != EmptyHash || e.ProofRoot != EmptyHash || e.Hold != nil {
			return fmt.Errorf("%w: a put of size 0", ErrEmpty)
		}
	} else if e.Root == EmptyRoot {
		return fmt.Errorf("%w: the empty root for %d bytes", ErrEmpty, e.Size)
	}
	return nil
}

// readMeta: a CBOR map (type), of 1 to 32 pairs, each key 1 to 128 bytes of
// a-z, 0-9 and "-", each value one-line text of 0 to 1024 bytes, the map at
// most 2048 bytes encoded. Every failure but the type is meta.
func readMeta(f fields) ([]MetaPair, error) {
	v, _ := f.f.Get(EKMeta)
	m, ok := v.(cbor.Map)
	if !ok {
		return nil, typeErr(EKMeta)
	}
	if len(m) < 1 || len(m) > MaxMetaPairs {
		return nil, fmt.Errorf("%w: %d pairs", ErrMeta, len(m))
	}
	out := make([]MetaPair, len(m))
	for i, p := range m {
		k, ok := p.Key.(string)
		if !ok || !CheckMetaKey(k) {
			return nil, fmt.Errorf("%w: key of pair %d", ErrMeta, i)
		}
		val, ok := p.Val.(string)
		if !ok || len(val) > MaxMetaValue || !OneLine(val) {
			return nil, fmt.Errorf("%w: value of %q", ErrMeta, k)
		}
		out[i] = MetaPair{Key: k, Value: val}
	}
	enc, err := cbor.Encode(m)
	if err != nil || len(enc) > MaxMetaBytes {
		return nil, fmt.Errorf("%w: over %d bytes", ErrMeta, MaxMetaBytes)
	}
	return out, nil
}

// ReadHold holds a hold array to spec section 4.3. Every failure is hold.
func ReadHold(a []cbor.Value) (*Hold, error) {
	if len(a) != 8 {
		return nil, fmt.Errorf("%w: %d elements", ErrHold, len(a))
	}
	txid, ok := a[0].([]byte)
	if !ok || len(txid) != 32 {
		return nil, fmt.Errorf("%w: txid", ErrHold)
	}
	var n [7]uint64
	bounds := [7][2]uint64{{0, MaxU32}, {1, MaxSatoshis}, {1, MaxT}, {1, MaxT}, {1, MaxU32}, {1, MaxU32}, {1, MaxCheckC}}
	for i := range n {
		x, ok := a[i+1].(uint64)
		if !ok || x < bounds[i][0] || x > bounds[i][1] {
			return nil, fmt.Errorf("%w: element %d", ErrHold, i+1)
		}
		n[i] = x
	}
	h := &Hold{Txid: [32]byte(txid), Vout: n[0], Satoshis: n[1], T: n[2], K: n[3], Retention: n[4], Deadline: n[5], CheckC: n[6]}
	if h.K < h.T {
		return nil, fmt.Errorf("%w: K below t", ErrHold)
	}
	return h, nil
}
