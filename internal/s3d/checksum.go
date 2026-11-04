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
	"sync"
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

	lanes   []chan []byte
	pending sync.WaitGroup
	synced  bool
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

// Write hands p to every digest. Each digest runs on a goroutine of its
// own, fed a copy of p, so the request's read loop waits for none of them
// and the digests overlap each other and the closure's own hashing; sync
// waits for them all before any digest is read.
func (b *bodyHashes) Write(p []byte) (int, error) {
	b.n += int64(len(p))
	if b.synced {
		b.write(p)
		return len(p), nil
	}
	if b.lanes == nil {
		for _, h := range b.all() {
			ch := make(chan []byte, 8)
			b.lanes = append(b.lanes, ch)
			go func(h hash.Hash) {
				for c := range ch {
					h.Write(c)
					b.pending.Done()
				}
			}(h)
		}
	}
	c := append([]byte(nil), p...)
	b.pending.Add(len(b.lanes))
	for _, ch := range b.lanes {
		ch <- c
	}
	return len(p), nil
}

// newSums computes only the flexible checksums h declares: a completed
// upload's whole-object checksums, where no MD5 is needed.
func newSums(h http.Header) *bodyHashes {
	b := newBodyHashes(h, "")
	b.md5 = nil
	return b
}

// any reports whether there is a digest to compute.
func (b *bodyHashes) any() bool { return len(b.all()) > 0 }

func (b *bodyHashes) all() []hash.Hash {
	var hs []hash.Hash
	if b.md5 != nil {
		hs = append(hs, b.md5)
	}
	if b.sha256 != nil {
		hs = append(hs, b.sha256)
	}
	for _, h := range b.sums {
		hs = append(hs, h)
	}
	return hs
}

func (b *bodyHashes) write(p []byte) {
	for _, h := range b.all() {
		h.Write(p)
	}
}

// sync waits for every digest to take what was written; later writes are
// hashed in line.
func (b *bodyHashes) sync() {
	if b.synced {
		return
	}
	b.synced = true
	for _, ch := range b.lanes {
		close(ch)
	}
	b.pending.Wait()
}

// reader tees r through the hashes.
func (b *bodyHashes) reader(r io.Reader) io.Reader { return io.TeeReader(r, b) }

func (b *bodyHashes) md5Hex() string { b.sync(); return hex.EncodeToString(b.md5.Sum(nil)) }

// check holds each declared checksum, from the headers or the trailers, to
// the body: nil, or the S3 error.
func (b *bodyHashes) check(h http.Header, trailers map[string]string) *s3Error {
	b.sync()
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
	b.sync()
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
