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
	"sync/atomic"
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
	// staged stands in for the body's SHA-256 where a stage computes it
	// as the content hash (sha256Staged): the bytes are hashed once.
	staged *stagedSHA256

	lanes   []chan *hashJob
	started bool
	pending sync.WaitGroup
	synced  bool
}

// hashJob is one buffer handed to every lane; done, when set, runs once
// the last lane has hashed it, and the buffer is the caller's again.
type hashJob struct {
	p    []byte
	left atomic.Int32
	done func()
}

// stagedSHA256 is a SHA-256 whose sum is set from elsewhere: the content
// hash a stage computes over the same bytes. Writes are nothing.
type stagedSHA256 struct{ sum [32]byte }

func (s *stagedSHA256) Write(p []byte) (int, error) { return len(p), nil }
func (s *stagedSHA256) Sum(b []byte) []byte         { return append(b, s.sum[:]...) }
func (s *stagedSHA256) Reset()                      {}
func (s *stagedSHA256) Size() int                   { return sha256.Size }
func (s *stagedSHA256) BlockSize() int              { return sha256.BlockSize }

// sha256Staged makes every SHA-256 of the body (a signed payload hash, an
// x-amz-checksum-sha256) one the stage sets (setSHA256) instead of one
// hashed here: a put's body is the content, whose SHA-256 the stage
// computes. Before the first write.
func (b *bodyHashes) sha256Staged() {
	if b.sha256 == nil && b.sums["sha256"] == nil {
		return
	}
	b.staged = &stagedSHA256{}
	if b.sha256 != nil {
		b.sha256 = b.staged
	}
	if b.sums["sha256"] != nil {
		b.sums["sha256"] = b.staged
	}
}

// setSHA256 is the body's SHA-256 as the stage computed it, after
// sha256Staged.
func (b *bodyHashes) setSHA256(sum [32]byte) {
	if b.staged != nil {
		b.staged.sum = sum
	}
}

// newBodyHashes computes MD5 when wantMD5 (an MD5 ETag, or a Content-MD5
// to check), SHA-256 when the payload hash is signed, and each flexible
// checksum the request declares in a header, in x-amz-trailer, or by
// x-amz-sdk-checksum-algorithm.
func newBodyHashes(h http.Header, trailer string, wantMD5 bool) *bodyHashes {
	b := &bodyHashes{sums: map[string]hash.Hash{}}
	if wantMD5 {
		b.md5 = md5.New()
	}
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

// Write hands a copy of p to every digest (feed): an io.Writer, for a
// reader teed through the hashes.
func (b *bodyHashes) Write(p []byte) (int, error) {
	b.feed(append([]byte(nil), p...), nil)
	return len(p), nil
}

// feed hands p itself to every digest, in order with what was fed before,
// and calls done (when not nil) once the last of them has hashed it; until
// then p is theirs. Each digest runs on a goroutine of its own, so the
// caller's read loop waits for none of them and the digests overlap each
// other and the closure's own hashing; sync waits for them all before any
// digest is read. Once synced, feed hashes in line.
func (b *bodyHashes) feed(p []byte, done func()) {
	b.n += int64(len(p))
	if !b.synced && !b.started {
		b.started = true
		for _, h := range b.hashing() {
			ch := make(chan *hashJob, 8)
			b.lanes = append(b.lanes, ch)
			go func(h hash.Hash) {
				for j := range ch {
					h.Write(j.p)
					if j.left.Add(-1) == 0 && j.done != nil {
						j.done()
					}
					b.pending.Done()
				}
			}(h)
		}
	}
	if b.synced || len(b.lanes) == 0 {
		b.write(p)
		if done != nil {
			done()
		}
		return
	}
	j := &hashJob{p: p, done: done}
	j.left.Store(int32(len(b.lanes)))
	b.pending.Add(len(b.lanes))
	for _, ch := range b.lanes {
		ch <- j
	}
}

// newSums computes only the flexible checksums h declares: a completed
// upload's whole-object checksums, where no MD5 is needed.
func newSums(h http.Header) *bodyHashes { return newBodyHashes(h, "", false) }

// any reports whether there is a digest to compute over the bytes.
func (b *bodyHashes) any() bool { return len(b.hashing()) > 0 }

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

// hashing is every digest that hashes the bytes: all but the staged one.
func (b *bodyHashes) hashing() []hash.Hash {
	var hs []hash.Hash
	for _, h := range b.all() {
		if b.staged == nil || h != hash.Hash(b.staged) {
			hs = append(hs, h)
		}
	}
	return hs
}

func (b *bodyHashes) write(p []byte) {
	for _, h := range b.hashing() {
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

// needSHA256 adds a SHA-256 of the body when none is computed already (a
// part's ETag when there is no MD5). Before the first write.
func (b *bodyHashes) needSHA256() {
	if b.sha256 == nil && b.sums["sha256"] == nil {
		b.sha256 = sha256.New()
	}
}

// sha256Hex is the body's SHA-256, after needSHA256.
func (b *bodyHashes) sha256Hex() string {
	b.sync()
	h := b.sha256
	if h == nil {
		h = b.sums["sha256"]
	}
	return hex.EncodeToString(h.Sum(nil))
}

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

// uploadETag is a completed upload's ETag: with MD5 ETags, S3's multipart
// ETag, the MD5 of the parts' MD5s, a hyphen and the number of parts;
// otherwise, or when a part was kept without an MD5, the content hash.
func uploadETag(etagMD5 bool, recs []partRecord, contentHash [32]byte) string {
	if !etagMD5 {
		return hex.EncodeToString(contentHash[:])
	}
	h := md5.New()
	for _, rec := range recs {
		m, err := hex.DecodeString(rec.MD5)
		if err != nil || len(m) != md5.Size {
			return hex.EncodeToString(contentHash[:])
		}
		h.Write(m)
	}
	return hex.EncodeToString(h.Sum(nil)) + "-" + itoa(len(recs))
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
