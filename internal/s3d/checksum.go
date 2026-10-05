package s3d

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"strings"
)

// The flexible checksums S3 clients send, in a header or a trailer
// (x-amz-checksum-<algorithm>, base64 of the big-endian digest).
var checksumAlgos = map[string]func() hash.Hash{
	"crc32":     func() hash.Hash { return crc32.NewIEEE() },
	"crc32c":    func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
	"crc64nvme": func() hash.Hash { return crc64.New(crc64NVME) },
	"sha1":      sha1.New,
	"sha256":    sha256.New,
}

// crc64NVME is CRC-64/NVME (reflected polynomial 0x9a6c9329ac4bc9b5, as
// hash/crc64 takes it; initial and final value all ones).
var crc64NVME = crc64.MakeTable(0x9a6c9329ac4bc9b5)

// bodyHashes are every digest a write checks, computed as the body streams.
type bodyHashes struct {
	md5    hash.Hash
	sha256 hash.Hash
	sums   map[string]hash.Hash
	n      int64
}

// newBodyHashes computes MD5 (the ETag), SHA-256 when the payload hash is
// signed, and each flexible checksum the request declares in a header, in
// x-amz-trailer, or by x-amz-sdk-checksum-algorithm.
func newBodyHashes(h http.Header, trailer string) *bodyHashes {
	b := &bodyHashes{md5: md5.New(), sums: map[string]hash.Hash{}}
	want := func(name string) {
		name = strings.ToLower(strings.TrimSpace(name))
		if f, ok := checksumAlgos[name]; ok && b.sums[name] == nil {
			b.sums[name] = f()
		}
	}
	for name := range checksumAlgos {
		if h.Get("X-Amz-Checksum-"+name) != "" {
			want(name)
		}
	}
	for _, t := range strings.Split(trailer, ",") {
		if n, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(t)), "x-amz-checksum-"); ok {
			want(n)
		}
	}
	if a := h.Get("X-Amz-Sdk-Checksum-Algorithm"); a != "" {
		want(a)
	}
	return b
}

func (b *bodyHashes) Write(p []byte) (int, error) {
	b.md5.Write(p)
	if b.sha256 != nil {
		b.sha256.Write(p)
	}
	for _, h := range b.sums {
		h.Write(p)
	}
	b.n += int64(len(p))
	return len(p), nil
}

// reader tees r through the hashes.
func (b *bodyHashes) reader(r io.Reader) io.Reader { return io.TeeReader(r, b) }

func (b *bodyHashes) md5Hex() string { return hex.EncodeToString(b.md5.Sum(nil)) }

// check holds each declared checksum, from the headers or the trailers, to
// the body: nil, or the S3 error.
func (b *bodyHashes) check(h http.Header, trailers map[string]string) *s3Error {
	for name, hs := range b.sums {
		got := base64.StdEncoding.EncodeToString(hs.Sum(nil))
		want := h.Get("X-Amz-Checksum-" + name)
		if v, ok := trailers["x-amz-checksum-"+name]; ok {
			want = v
		}
		if want == "" {
			// Declared by algorithm only: nothing to compare.
			continue
		}
		if want != got {
			return &s3Error{Code: "BadDigest", Status: http.StatusBadRequest,
				Message: "The " + strings.ToUpper(name) + " you specified did not match the calculated checksum."}
		}
	}
	return nil
}

// responseHeaders echo the checksums a put declared, as S3 does.
func (b *bodyHashes) responseHeaders(w http.Header) {
	for name, hs := range b.sums {
		w.Set("x-amz-checksum-"+name, base64.StdEncoding.EncodeToString(hs.Sum(nil)))
	}
}

// partETag is S3's multipart ETag: the MD5 of the parts' MD5s, a hyphen
// and the number of parts.
func partETag(md5s [][]byte) string {
	h := md5.New()
	for _, m := range md5s {
		h.Write(m)
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + itoa(len(md5s))
}

func itoa(n int) string {
	var buf [20]byte
	i := len(buf)
	if n == 0 {
		return "0"
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func base64Std(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
