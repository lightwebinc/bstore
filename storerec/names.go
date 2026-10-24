package storerec

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode/utf8"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/guard"
)

// AnyoneKey is BRC-43's anyone counterparty: the generator point, whose
// private key is 1.
var AnyoneKey = func() []byte {
	k, _ := ec.PrivateKeyFromBytes(append(make([]byte, 31), 1))
	return k.PubKey().Compressed()
}()

// CheckKey holds b to the key rule of spec section 2: a canonical compressed
// point (bcommon guard.ParsePubKey) that is not the anyone key. A key that
// held is remembered (keycache.go).
func CheckKey(b []byte) (*ec.PublicKey, error) {
	if len(b) == 33 {
		if k, ok := checkedKeys.get([33]byte(b)); ok {
			return k, nil
		}
	}
	k, err := guard.ParsePubKey(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIdentity, err)
	}
	if bytes.Equal(b, AnyoneKey) {
		return nil, fmt.Errorf("%w: the anyone key", ErrIdentity)
	}
	checkedKeys.put([33]byte(b), k)
	return k, nil
}

func lower(c byte) bool { return c >= 'a' && c <= 'z' }

// CheckBucket holds s to the bucket identifier grammar of spec section 3.1:
// <name>_<suffix>, the name 1 to 25 bytes of lowercase letters and single
// underscores with a letter at each end, the suffix exactly 10 lowercase
// letters; split at the last underscore.
func CheckBucket(s string) error {
	if len(s) < MinBucket || len(s) > MaxBucket {
		return fmt.Errorf("%w: %d bytes", ErrBucket, len(s))
	}
	i := strings.LastIndexByte(s, '_')
	if i < 0 {
		return fmt.Errorf("%w: no underscore", ErrBucket)
	}
	name, suffix := s[:i], s[i+1:]
	if len(suffix) != SuffixLen {
		return fmt.Errorf("%w: suffix of %d bytes", ErrBucket, len(suffix))
	}
	for j := 0; j < len(suffix); j++ {
		if !lower(suffix[j]) {
			return fmt.Errorf("%w: suffix", ErrBucket)
		}
	}
	if len(name) < 1 || len(name) > MaxName || !lower(name[0]) || !lower(name[len(name)-1]) {
		return fmt.Errorf("%w: name", ErrBucket)
	}
	for j := 0; j < len(name); j++ {
		c := name[j]
		if c == '_' {
			if name[j-1] == '_' {
				return fmt.Errorf("%w: double underscore", ErrBucket)
			}
			continue
		}
		if !lower(c) {
			return fmt.Errorf("%w: name", ErrBucket)
		}
	}
	return nil
}

// RecordsTopic and BlobTopic are a bucket's two topics.
func RecordsTopic(bucket string) string { return RecordsPrefix + bucket }
func BlobTopic(bucket string) string    { return BlobPrefix + bucket }

// S3Name is a bucket identifier as an S3 bucket name (spec section 16.1),
// and whether the gateway refuses it for S3 use (the sthree- prefix).
func S3Name(bucket string) (string, bool) {
	n := strings.ReplaceAll(bucket, "_", "-")
	return n, !strings.HasPrefix(n, "sthree-")
}

// BucketID is SHA-256("bstore bucket id v1" || txid || uint32be(vout)) of
// the outpoint a bucket's first token spends, txid in hash byte order.
func BucketID(txid [32]byte, vout uint32) [32]byte {
	h := sha256.New()
	h.Write([]byte(DomainBucketID))
	h.Write(txid[:])
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], vout)
	h.Write(v[:])
	return [32]byte(h.Sum(nil))
}

// OneLine reports whether s holds no control character (U+0000 to U+001F,
// U+007F to U+009F) and neither U+2028 nor U+2029 (spec section 2). s is
// valid UTF-8 (the CBOR decoder refuses anything else).
func OneLine(s string) bool {
	for _, r := range s {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) || r == 0x2028 || r == 0x2029 {
			return false
		}
	}
	return true
}

// CheckObjectKey holds k to the object key rule: 1 to 1024 bytes, one line.
func CheckObjectKey(k string) error {
	if len(k) < 1 || len(k) > MaxObjectKey || !utf8.ValidString(k) || !OneLine(k) {
		return ErrObjKey
	}
	return nil
}

// CheckContentType holds s to spec section 4.1: 3 to 127 bytes of
// lowercase printable ASCII (0x21 to 0x7e, no A to Z), exactly one "/", and
// no parameters (no ";").
func CheckContentType(s string) error {
	if len(s) < MinContentType || len(s) > MaxContentType {
		return ErrContentType
	}
	slashes := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x21 || c > 0x7e || (c >= 'A' && c <= 'Z') || c == ';' {
			return ErrContentType
		}
		if c == '/' {
			slashes++
		}
	}
	if slashes != 1 {
		return ErrContentType
	}
	return nil
}

// CheckETag holds s to spec section 4.1: 1 to 64 bytes of printable ASCII
// (0x20 to 0x7e) without '"'.
func CheckETag(s string) error {
	if len(s) < 1 || len(s) > MaxETag {
		return ErrETag
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c > 0x7e || c == '"' {
			return ErrETag
		}
	}
	return nil
}

// CheckMetaKey holds a meta key to 1 to 128 bytes of a-z, 0-9 and "-".
func CheckMetaKey(s string) bool {
	if len(s) < 1 || len(s) > MaxMetaKey {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !lower(c) && !(c >= '0' && c <= '9') && c != '-' {
			return false
		}
	}
	return true
}

// CheckFrontURL holds a front statement's url to spec section 8.2:
// "https://", a non-empty authority without user information, no query or
// fragment, 12 to 512 bytes of 0x21 to 0x7e, not ending in "/"; and the
// WHATWG parse the advertisement check runs (an https URL with a non-empty
// host, no username, password or fragment), its host ASCII after
// percent-decoding with no label beginning "xn--". The last rule is
// httpsURL's narrowing, made the rule here, so that Go and TypeScript
// decide every front url alike.
func CheckFrontURL(s string) error {
	if len(s) < MinURL || len(s) > MaxURL || !strings.HasPrefix(s, "https://") || strings.HasSuffix(s, "/") {
		return ErrURL
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x21 || c > 0x7e || c == '?' || c == '#' {
			return ErrURL
		}
	}
	auth := s[len("https://"):]
	if i := strings.IndexByte(auth, '/'); i >= 0 {
		auth = auth[:i]
	}
	if auth == "" || strings.IndexByte(auth, '@') >= 0 {
		return ErrURL
	}
	if ok, _ := httpsURL(s); !ok {
		return ErrURL
	}
	return nil
}

// DerivedFront is the front of a provider that answers for itself: its
// advertisement's url up to, not including, the last "/chirp/v1/",
// followed by "/bstore/v1" (spec section 10.1). ok is false when the url
// holds no "/chirp/v1/".
func DerivedFront(adURL string) (string, bool) {
	i := strings.LastIndex(adURL, "/chirp/v1/")
	if i < 0 {
		return "", false
	}
	return adURL[:i] + FrontSuffix, true
}
