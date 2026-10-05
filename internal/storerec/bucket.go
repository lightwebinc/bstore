package storerec

import (
	"bytes"
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
)

// Change is one change of a bucket record: a put [key, C] sets the key to
// the put entry's commitment C; a removal [key, Z, D] removes the key, D
// being the delete entry's commitment.
type Change struct {
	Key     string
	C       [32]byte
	Removal bool
}

// Bucket is the bucket record (magic stb 0x01) a token carries.
type Bucket struct {
	Bucket   string
	Owner    []byte
	ID       [32]byte
	Seq      uint64
	Prev     [32]byte
	Writers  [][]byte
	Changes  []Change
	Count    uint64
	MapRoot  [32]byte
	Created  uint64
	Consumed [][32]byte
	// Extra holds keys 12 and above, verbatim.
	Extra cbor.Map
}

var zero32 [32]byte

func (c Change) value() cbor.Value {
	if c.Removal {
		return []cbor.Value{c.Key, b(zero32), b(c.C)}
	}
	return []cbor.Value{c.Key, b(c.C)}
}

// Encode writes the record canonically. It refuses what DecodeBucket
// refuses, so a record this package writes is one it reads.
func (r *Bucket) Encode() ([]byte, error) {
	out, err := encodeRecord(r.fields(), r.Extra, BKLast, MaxBucketRecord)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeBucket(out); err != nil {
		return nil, err
	}
	return out, nil
}

// EncodedSize is the length the record encodes to, whatever its bounds:
// what the owner's tooling measures when it splits changes over tokens.
func (r *Bucket) EncodedSize() (int, error) {
	out, err := encodeRecord(r.fields(), r.Extra, BKLast, 1<<30)
	if err != nil {
		return 0, err
	}
	return len(out), nil
}

func (r *Bucket) fields() cbor.Map {
	writers := make([]cbor.Value, len(r.Writers))
	for i, w := range r.Writers {
		writers[i] = append([]byte(nil), w...)
	}
	changes := make([]cbor.Value, len(r.Changes))
	for i, c := range r.Changes {
		changes[i] = c.value()
	}
	consumed := make([]cbor.Value, len(r.Consumed))
	for i, c := range r.Consumed {
		consumed[i] = b(c)
	}
	return cbor.Map{
		{Key: BKMagic, Val: MagicBucket},
		{Key: BKBucket, Val: r.Bucket},
		{Key: BKOwner, Val: r.Owner},
		{Key: BKID, Val: b(r.ID)},
		{Key: BKSeq, Val: r.Seq},
		{Key: BKPrev, Val: b(r.Prev)},
		{Key: BKWriters, Val: writers},
		{Key: BKChanges, Val: changes},
		{Key: BKCount, Val: r.Count},
		{Key: BKMapRoot, Val: b(r.MapRoot)},
		{Key: BKCreated, Val: r.Created},
		{Key: BKConsumed, Val: consumed},
	}
}

// DecodeBucket reads a bucket record, refusing it for the first rule it
// breaks in the order of spec section 3.2: too-large, cbor, key-type, magic,
// then per key in key order missing, type and the key's own rule, then
// first.
func DecodeBucket(raw []byte) (*Bucket, error) {
	f, err := readRecord(raw, MaxBucketRecord, BKLast, MagicBucket)
	if err != nil {
		return nil, err
	}
	r := &Bucket{Extra: f.extra()}
	if r.Bucket, err = f.bucket(BKBucket); err != nil {
		return nil, err
	}
	if r.Owner, err = f.key(BKOwner); err != nil {
		return nil, err
	}
	if r.ID, err = f.b32(BKID); err != nil {
		return nil, err
	}
	if r.Seq, err = f.uint(BKSeq, 0, MaxSafe, ErrSeq); err != nil {
		return nil, err
	}
	if r.Prev, err = f.b32(BKPrev); err != nil {
		return nil, err
	}
	if r.Writers, err = readWriters(f, r.Owner); err != nil {
		return nil, err
	}
	if r.Changes, err = readChanges(f); err != nil {
		return nil, err
	}
	if r.Count, err = f.uint(BKCount, 0, MaxKeysInMap, ErrCount); err != nil {
		return nil, err
	}
	if r.MapRoot, err = f.b32(BKMapRoot); err != nil {
		return nil, err
	}
	if r.Created, err = f.created(BKCreated); err != nil {
		return nil, err
	}
	if r.Consumed, err = readConsumed(f, r.Changes); err != nil {
		return nil, err
	}
	if r.Seq == 0 {
		if r.Prev != zero32 || len(r.Changes) != 0 || len(r.Consumed) != 0 || r.Count != 0 || r.MapRoot != EmptyHash {
			return nil, fmt.Errorf("%w: a first token", ErrFirst)
		}
	} else if r.Prev == zero32 {
		return nil, fmt.Errorf("%w: prev is zero at seq %d", ErrFirst, r.Seq)
	}
	return r, nil
}

// readWriters: at most 16 (writers), each a key (identity), strictly
// ascending (writers), none the owner (writers).
func readWriters(f fields, owner []byte) ([][]byte, error) {
	a, err := f.array(BKWriters)
	if err != nil {
		return nil, err
	}
	if len(a) > MaxWriters {
		return nil, fmt.Errorf("%w: %d writers", ErrWriters, len(a))
	}
	out := make([][]byte, len(a))
	for i, e := range a {
		k, ok := e.([]byte)
		if !ok {
			return nil, fmt.Errorf("%w: writer %d is not a byte string", ErrIdentity, i)
		}
		if _, err := CheckKey(k); err != nil {
			return nil, fmt.Errorf("writer %d: %w", i, err)
		}
		out[i] = k
	}
	for i := range out {
		if i > 0 && bytes.Compare(out[i-1], out[i]) >= 0 {
			return nil, fmt.Errorf("%w: not strictly ascending", ErrWriters)
		}
		if bytes.Equal(out[i], owner) {
			return nil, fmt.Errorf("%w: names the owner", ErrWriters)
		}
	}
	return out, nil
}

// readChanges: at most 1024; each a put [key, C] or a removal [key, Z, D],
// key an object key, Z 32 zero bytes, C and D 32 bytes and not zero; keys
// strictly ascending by their bytes. Every failure is changes.
func readChanges(f fields) ([]Change, error) {
	a, err := f.array(BKChanges)
	if err != nil {
		return nil, err
	}
	if len(a) > MaxChanges {
		return nil, fmt.Errorf("%w: %d changes", ErrChanges, len(a))
	}
	out := make([]Change, len(a))
	for i, e := range a {
		c, ok := readChange(e)
		if !ok {
			return nil, fmt.Errorf("%w: change %d", ErrChanges, i)
		}
		if i > 0 && out[i-1].Key >= c.Key {
			return nil, fmt.Errorf("%w: keys not strictly ascending at %d", ErrChanges, i)
		}
		out[i] = c
	}
	return out, nil
}

func readChange(e cbor.Value) (Change, bool) {
	a, ok := e.([]cbor.Value)
	if !ok || (len(a) != 2 && len(a) != 3) {
		return Change{}, false
	}
	key, ok := a[0].(string)
	if !ok || CheckObjectKey(key) != nil {
		return Change{}, false
	}
	last, ok := a[len(a)-1].([]byte)
	if !ok || len(last) != 32 || [32]byte(last) == zero32 {
		return Change{}, false
	}
	c := Change{Key: key, C: [32]byte(last)}
	if len(a) == 3 {
		z, ok := a[1].([]byte)
		if !ok || len(z) != 32 || [32]byte(z) != zero32 {
			return Change{}, false
		}
		c.Removal = true
	}
	return c, true
}

// readConsumed: at most 1024 commitments of 32 bytes, strictly ascending,
// none a C or D of this record's changes. Every failure is consumed.
func readConsumed(f fields, changes []Change) ([][32]byte, error) {
	a, err := f.array(BKConsumed)
	if err != nil {
		return nil, err
	}
	if len(a) > MaxConsumed {
		return nil, fmt.Errorf("%w: %d commitments", ErrConsumed, len(a))
	}
	named := make(map[[32]byte]bool, len(changes))
	for _, c := range changes {
		named[c.C] = true
	}
	out := make([][32]byte, len(a))
	for i, e := range a {
		x, ok := e.([]byte)
		if !ok || len(x) != 32 {
			return nil, fmt.Errorf("%w: element %d", ErrConsumed, i)
		}
		out[i] = [32]byte(x)
		if i > 0 && bytes.Compare(out[i-1][:], out[i][:]) >= 0 {
			return nil, fmt.Errorf("%w: not strictly ascending", ErrConsumed)
		}
		if named[out[i]] {
			return nil, fmt.Errorf("%w: names a change's commitment", ErrConsumed)
		}
	}
	return out, nil
}
