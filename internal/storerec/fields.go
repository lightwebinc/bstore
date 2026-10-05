package storerec

import (
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/record"
)

// fields reads one record's known keys with bstore's labels: missing for an
// absent key, type for a value of another CBOR type (a fixed-length byte
// string of another length included), and the key's own label for a value
// of the right type that breaks its rule.
type fields struct{ f *record.Fields }

func readRecord(b []byte, max int, last uint64, magic []byte) (fields, error) {
	f, err := record.Decode(b, max, last, magic)
	if err != nil {
		return fields{}, err
	}
	return fields{f}, nil
}

func (r fields) has(k uint64) bool { return r.f.Has(k) }

func (r fields) extra() cbor.Map { return r.f.Extra }

func (r fields) need(k uint64) (cbor.Value, error) {
	v, ok := r.f.Get(k)
	if !ok {
		return nil, fmt.Errorf("%w: key %d", ErrMissing, k)
	}
	return v, nil
}

func typeErr(k uint64) error { return fmt.Errorf("%w: key %d", ErrType, k) }

func (r fields) bytes(k uint64) ([]byte, error) {
	v, err := r.need(k)
	if err != nil {
		return nil, err
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, typeErr(k)
	}
	return b, nil
}

func (r fields) bytesN(k uint64, n int) ([]byte, error) {
	b, err := r.bytes(k)
	if err != nil {
		return nil, err
	}
	if len(b) != n {
		return nil, typeErr(k)
	}
	return b, nil
}

func (r fields) b32(k uint64) ([32]byte, error) {
	b, err := r.bytesN(k, 32)
	if err != nil {
		return [32]byte{}, err
	}
	return [32]byte(b), nil
}

func (r fields) text(k uint64) (string, error) {
	v, err := r.need(k)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", typeErr(k)
	}
	return s, nil
}

// uint reads an unsigned integer and holds it to lo..hi under label.
func (r fields) uint(k uint64, lo, hi uint64, label error) (uint64, error) {
	v, err := r.need(k)
	if err != nil {
		return 0, err
	}
	n, ok := v.(uint64)
	if !ok {
		return 0, typeErr(k)
	}
	if n < lo || n > hi {
		return 0, fmt.Errorf("%w: key %d is %d", label, k, n)
	}
	return n, nil
}

func (r fields) array(k uint64) ([]cbor.Value, error) {
	v, err := r.need(k)
	if err != nil {
		return nil, err
	}
	a, ok := v.([]cbor.Value)
	if !ok {
		return nil, typeErr(k)
	}
	return a, nil
}

// key reads an identity key: a byte string, then the key rule (identity).
func (r fields) key(k uint64) ([]byte, error) {
	b, err := r.bytes(k)
	if err != nil {
		return nil, err
	}
	if _, err := CheckKey(b); err != nil {
		return nil, fmt.Errorf("key %d: %w", k, err)
	}
	return b, nil
}

func (r fields) bucket(k uint64) (string, error) {
	s, err := r.text(k)
	if err != nil {
		return "", err
	}
	if err := CheckBucket(s); err != nil {
		return "", err
	}
	return s, nil
}

func (r fields) created(k uint64) (uint64, error) {
	return r.uint(k, MinTime, MaxTime, ErrCreated)
}

// encodeRecord writes the defined pairs and the preserved ones, held to max.
func encodeRecord(m cbor.Map, extra cbor.Map, last uint64, max int) ([]byte, error) {
	if err := record.CheckExtra(extra, last, len(m)); err != nil {
		return nil, err
	}
	return record.Encode(m, extra, max)
}

func b(x [32]byte) []byte { return append([]byte(nil), x[:]...) }
