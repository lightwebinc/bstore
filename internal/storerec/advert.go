package storerec

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"unicode/utf8"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/guard"
)

// MaxAdPayload bounds an advertisement's fields together, as the SDK does.
const MaxAdPayload = MaxAdURL + 256

// ErrAdvertisement is every refusal of an advertisement; its rule names the
// SDK check that failed. ErrNarrowed marks a url this codec refuses that
// the WHATWG parser may admit (url.go).
var (
	ErrAdvertisement = errors.New("storerec: not an advertisement as the SDK validates it")
	ErrNarrowed      = errors.New("storerec: an internationalised host this codec does not parse")
)

// Advertisement is a BRC-26 advertisement output as the SDK decodes and
// verifies it (ts-stack @bsv/sdk storage/UHRPAdvertisementValidation.ts).
type Advertisement struct {
	Host      []byte
	Hash      [32]byte
	URL       string
	Expiry    uint64
	FileSize  uint64
	Signature []byte
}

func adErr(rule string) error { return ruled(ErrAdvertisement, rule) }

// minimalPush is the SDK's canonical push of one PushDrop field: OP_0 for
// no bytes or a single 0, OP_1 to OP_16, OP_1NEGATE for 0x81, else the
// shortest data push.
func minimalPush(d []byte) []byte {
	switch {
	case len(d) == 0 || (len(d) == 1 && d[0] == 0):
		return []byte{0x00}
	case len(d) == 1 && d[0] >= 1 && d[0] <= 16:
		return []byte{0x50 + d[0]}
	case len(d) == 1 && d[0] == 0x81:
		return []byte{0x4f}
	case len(d) <= 75:
		return append([]byte{byte(len(d))}, d...)
	case len(d) <= 0xff:
		return append([]byte{0x4c, byte(len(d))}, d...)
	case len(d) <= 0xffff:
		return append(binary.LittleEndian.AppendUint16([]byte{0x4d}, uint16(len(d))), d...)
	default:
		return append(binary.LittleEndian.AppendUint32([]byte{0x4e}, uint32(len(d))), d...)
	}
}

// readPush reads one PushDrop field at s[i:], as the SDK's PushDrop.decode
// maps it: a data push's bytes, OP_0 as [0], OP_1 to OP_16 as [n],
// OP_1NEGATE as [0x81]. Any other opcode is not a field.
func readPush(s []byte, i int) ([]byte, int, bool) {
	if i >= len(s) {
		return nil, 0, false
	}
	op := s[i]
	n, hdr := 0, 1
	switch {
	case op == 0x00:
		return []byte{0}, i + 1, true
	case op >= 0x51 && op <= 0x60:
		return []byte{op - 0x50}, i + 1, true
	case op == 0x4f:
		return []byte{0x81}, i + 1, true
	case op >= 1 && op <= 75:
		n = int(op)
	case op == 0x4c && i+1 < len(s):
		n, hdr = int(s[i+1]), 2
	case op == 0x4d && i+2 < len(s):
		n, hdr = int(binary.LittleEndian.Uint16(s[i+1:])), 3
	case op == 0x4e && i+4 < len(s):
		n, hdr = int(binary.LittleEndian.Uint32(s[i+1:])), 5
	default:
		return nil, 0, false
	}
	if n < 0 || i+hdr+n > len(s) {
		return nil, 0, false
	}
	return s[i+hdr : i+hdr+n], i + hdr + n, true
}

// strictCompact reads a whole field as one canonical CompactSize, positive
// and at most 2^53 - 1, as the SDK's readVarIntNumStrict(false) and its
// end-of-field and positivity checks do.
func strictCompact(f []byte) (uint64, bool) {
	if len(f) == 0 {
		return 0, false
	}
	var v uint64
	var n int
	switch f[0] {
	case 0xfd:
		if len(f) < 3 {
			return 0, false
		}
		v, n = uint64(binary.LittleEndian.Uint16(f[1:])), 3
		if v < 0xfd {
			return 0, false
		}
	case 0xfe:
		if len(f) < 5 {
			return 0, false
		}
		v, n = uint64(binary.LittleEndian.Uint32(f[1:])), 5
		if v <= 0xffff {
			return 0, false
		}
	case 0xff:
		if len(f) < 9 {
			return 0, false
		}
		v, n = binary.LittleEndian.Uint64(f[1:]), 9
		if v <= 0xffffffff || v > MaxSafe {
			return 0, false
		}
	default:
		v, n = uint64(f[0]), 1
	}
	return v, n == len(f) && v >= 1
}

// canonicalDER parses a signature the SDK's Signature.fromDER reads and
// toDER writes back unchanged: a DER sequence of two minimal positive
// integers, any S.
func canonicalDER(sig []byte) (*ec.Signature, bool) {
	if len(sig) < 8 || len(sig) > 80 || sig[0] != 0x30 || sig[1] >= 0x80 || int(sig[1])+2 != len(sig) {
		return nil, false
	}
	readInt := func(i int) (*big.Int, int, bool) {
		if i+2 > len(sig) || sig[i] != 0x02 || sig[i+1] >= 0x80 {
			return nil, 0, false
		}
		n := int(sig[i+1])
		v := sig[i+2:]
		if n < 1 || n > len(v) {
			return nil, 0, false
		}
		v = v[:n]
		if v[0]&0x80 != 0 || (v[0] == 0 && (n == 1 || v[1]&0x80 == 0)) {
			return nil, 0, false
		}
		return new(big.Int).SetBytes(v), i + 2 + n, true
	}
	r, i, ok := readInt(2)
	if !ok {
		return nil, false
	}
	s, j, ok := readInt(i)
	if !ok || j != len(sig) {
		return nil, false
	}
	return &ec.Signature{R: r, S: s}, true
}

// controlRune is Unicode's general category Cc.
func controlRune(r rune) bool { return r <= 0x1f || (r >= 0x7f && r <= 0x9f) }

// DecodeAdvertisement decodes and verifies one advertisement output's
// locking script exactly as the SDK does: a canonical PushDrop of six
// fields, at most 2,048 bytes each and 2,304 together, locked before; a
// canonical compressed host key; a 32-byte hash; a url of 1 to 2,048 bytes,
// strict UTF-8 (one leading BOM dropped), no control character, a
// credential-free https URL with no fragment; expiry and file size as
// canonical positive CompactSize integers; a canonical DER signature of 8 to
// 80 bytes, any S; the lock the key derived for the host under [2, "uhrp
// advertisement"] key id 1, counterparty anyone; and the signature over the
// first five fields concatenated under that key.
func DecodeAdvertisement(script []byte) (*Advertisement, error) {
	if len(script) < 35 || script[0] != 0x21 || script[34] != 0xac {
		return nil, adErr("pushdrop")
	}
	lock, err := guard.ParsePubKey(script[1:34])
	if err != nil {
		return nil, adErr("pushdrop")
	}
	var fields [][]byte
	i := 35
	for len(fields) < 6 {
		f, next, ok := readPush(script, i)
		if !ok {
			return nil, adErr("pushdrop")
		}
		fields = append(fields, f)
		i = next
	}
	total := 0
	for _, f := range fields {
		if len(f) > MaxAdURL {
			return nil, adErr("field-size")
		}
		total += len(f)
	}
	if total > MaxAdPayload {
		return nil, adErr("field-size")
	}
	want := append([]byte{0x21}, script[1:34]...)
	want = append(want, 0xac)
	for _, f := range fields {
		want = append(want, minimalPush(f)...)
	}
	want = append(want, 0x6d, 0x6d, 0x6d)
	if !bytes.Equal(script, want) {
		return nil, adErr("pushdrop")
	}
	host := fields[0]
	if len(host) != 33 || (host[0] != 2 && host[0] != 3) {
		return nil, adErr("host")
	}
	hostKey, err := guard.ParsePubKey(host)
	if err != nil {
		return nil, adErr("host")
	}
	if len(fields[1]) != 32 {
		return nil, adErr("hash")
	}
	if len(fields[2]) < 1 || len(fields[2]) > MaxAdURL {
		return nil, adErr("url")
	}
	raw := fields[2]
	if !utf8.Valid(raw) {
		return nil, adErr("url")
	}
	raw = bytes.TrimPrefix(raw, []byte{0xef, 0xbb, 0xbf})
	u := string(raw)
	for _, r := range u {
		if controlRune(r) {
			return nil, adErr("url")
		}
	}
	if ok, narrowed := httpsURL(u); !ok {
		if narrowed {
			return nil, fmt.Errorf("%w: %w", adErr("url"), ErrNarrowed)
		}
		return nil, adErr("url")
	}
	expiry, ok := strictCompact(fields[3])
	if !ok {
		return nil, adErr("expiry")
	}
	size, ok := strictCompact(fields[4])
	if !ok {
		return nil, adErr("file-size")
	}
	sig, ok := canonicalDER(fields[5])
	if !ok {
		return nil, adErr("signature")
	}
	want2, err := AdDerivation.ExpectedLockingKey(hostKey)
	if err != nil || !want2.IsEqual(lock) {
		return nil, adErr("lock")
	}
	var signed []byte
	for _, f := range fields[:5] {
		signed = append(signed, f...)
	}
	d := sha256.Sum256(signed)
	if !sig.Verify(d[:], want2) {
		return nil, adErr("verify")
	}
	return &Advertisement{Host: host, Hash: [32]byte(fields[1]), URL: u, Expiry: expiry, FileSize: size, Signature: fields[5]}, nil
}

// CHIRPURLSuffix is the path a CHIRP root's advertisement url ends in
// (BRC-167): /chirp/v1/<rootIdentifier>/objects/<rootIdentifier>.
func CHIRPURLSuffix(root [32]byte) string {
	id := chirp.Identifier(root)
	return "/chirp/v1/" + id + "/objects/" + id
}

// Eligibility is why an advertisement does or does not count for an entry's
// bounty (spec section 9.3); "" is eligible.
func Eligibility(ad *Advertisement, e *Entry, rootLen uint64) string {
	switch {
	case e.Kind != KindPut || e.Hold == nil:
		return "no-hold"
	case ad.Hash != e.Root:
		return "root"
	case bytes.Equal(ad.Host, AnyoneKey):
		return "anyone"
	case ad.Expiry < e.Created/1000+e.Hold.Retention:
		return "expiry"
	case ad.FileSize != rootLen:
		return "file-size"
	case strings.ContainsAny(ad.URL, "?#") || !strings.HasSuffix(ad.URL, CHIRPURLSuffix(e.Root)):
		return "url"
	}
	return ""
}

// CompactSize is Bitcoin's CompactSize encoding of v.
func CompactSize(v uint64) []byte {
	switch {
	case v < 0xfd:
		return []byte{byte(v)}
	case v <= 0xffff:
		return binary.LittleEndian.AppendUint16([]byte{0xfd}, uint16(v))
	case v <= 0xffffffff:
		return binary.LittleEndian.AppendUint32([]byte{0xfe}, uint32(v))
	default:
		return binary.LittleEndian.AppendUint64([]byte{0xff}, v)
	}
}

// AdvertisementScript is the canonical PushDrop of six fields under lock, as
// the SDK writes an advertisement.
func AdvertisementScript(lock *ec.PublicKey, fields [][]byte) []byte {
	out := append([]byte{0x21}, lock.Compressed()...)
	out = append(out, 0xac)
	for _, f := range fields {
		out = append(out, minimalPush(f)...)
	}
	for n := len(fields); n > 0; n -= 2 {
		if n == 1 {
			out = append(out, 0x75)
		} else {
			out = append(out, 0x6d)
		}
	}
	return out
}

// AdvertisementFields are an advertisement's six fields, signed by the
// host's key derived under [2, "uhrp advertisement"] key id 1, counterparty
// anyone (adKey), and the lock that key's public key makes.
func AdvertisementFields(host []byte, adKey *ec.PrivateKey, hash [32]byte, url string, expiry, fileSize uint64) ([][]byte, error) {
	fields := [][]byte{host, hash[:], []byte(url), CompactSize(expiry), CompactSize(fileSize)}
	var signed []byte
	for _, f := range fields {
		signed = append(signed, f...)
	}
	d := sha256.Sum256(signed)
	sig, err := adKey.Sign(d[:])
	if err != nil {
		return nil, err
	}
	return append(fields, sig.Serialize()), nil
}
