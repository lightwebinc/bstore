package s3d

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/storerec"
)

// onePassHeaders declare every flexible checksum, so every digest runs.
var onePassHeaders = http.Header{"X-Amz-Sdk-Checksum-Algorithm": {"crc64nvme"}, "X-Amz-Checksum-Crc32": {"x"},
	"X-Amz-Checksum-Crc32c": {"x"}, "X-Amz-Checksum-Sha1": {"x"}, "X-Amz-Checksum-Sha256": {"x"}}

// digests are a body's digests as the old path computes them: the body
// teed through the hashes, each fed a copy of every read.
func digests(t *testing.T, b *bodyHashes) map[string]string {
	t.Helper()
	b.sync()
	out := map[string]string{"md5": b.md5Hex(), "sha256": b.sha256Hex()}
	h := http.Header{}
	b.responseHeaders(h)
	for k, v := range h {
		out[k] = v[0]
	}
	return out
}

func oldDigests(t *testing.T, content []byte) map[string]string {
	t.Helper()
	b := newBodyHashes(onePassHeaders, "", true)
	if _, err := io.Copy(io.Discard, b.reader(bytes.NewReader(content))); err != nil {
		t.Fatal(err)
	}
	return digests(t, b)
}

func sameDigests(t *testing.T, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("digests %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: %s, want %s", k, got[k], v)
		}
	}
}

// TestOnePassMultipart: parts read straight into the part writer's blob
// buffers, every digest fed from there, give the part digests of the old
// path (the body teed through the digests), blob hashes that are the
// protocol's (SHA-256 and commit.SegmentRoot of each blob), and an object
// that is what a put of the same bytes stages.
func TestOnePassMultipart(t *testing.T) {
	const c = chirp.ChunkSize
	r := newMPRig(t)
	up := r.create()
	parts := [][]byte{randBytes(2 * c), randBytes(c), randBytes(c + 12345)}
	for i, p := range parts {
		bh := newBodyHashes(onePassHeaders, "", true)
		part, pw, closed, err := up.openPart(i+1, int64(len(p)))
		if err != nil {
			t.Fatal(err)
		}
		pw.(*partWriter).tee = bh.feed
		if _, err := io.Copy(pw, io.LimitReader(bytes.NewReader(p), int64(len(p))+1)); err != nil {
			t.Fatal(err)
		}
		hashes, segs, err := pw.finish()
		if err != nil {
			t.Fatal(err)
		}
		if bh.n != int64(len(p)) {
			t.Fatalf("part %d: %d bytes fed, want %d", i+1, bh.n, len(p))
		}
		sameDigests(t, digests(t, bh), oldDigests(t, p))
		for j := range hashes {
			blob := p[j*c : min((j+1)*c, len(p))]
			seg, err := commit.SegmentRoot(blob, storerec.SegmentSize)
			if err != nil {
				t.Fatal(err)
			}
			if hashes[j] != sha256.Sum256(blob) || segs[j] != seg {
				t.Fatalf("part %d blob %d: hashes differ", i+1, j)
			}
		}
		part.fill(bh.n, bh.md5Hex(), "", hashes, segs)
		if e := up.keep(i+1, part, pw, 1<<40); e != nil {
			t.Fatal(e)
		}
		closed()
	}
	staged, bad, linked := r.complete(up, []int{1, 2, 3}, nil)
	if bad != nil || !linked {
		t.Fatalf("bad %v linked %v", bad, linked)
	}
	defer staged.Release()
	r.same(staged, cat(parts...))
}

// TestOnePassPut: a body staged with its digests fed from the stage's
// read buffers gives the digests of the old path and the same closure;
// the body's SHA-256 is the content hash, set from the stage.
func TestOnePassPut(t *testing.T) {
	r := newMPRig(t)
	for _, n := range []int{0, 1, 300<<10 + 7, 2*chirp.ChunkSize + 12345} {
		content := randBytes(n)
		bh := newBodyHashes(onePassHeaders, "", true)
		bh.sha256 = sha256.New() // a signed payload hash
		bh.sha256Staged()
		if bh.any() && len(bh.hashing()) != len(bh.all())-2 {
			t.Fatal("the staged SHA-256 is still hashed")
		}
		staged, err := r.s.store.StageTee(context.Background(), bytes.NewReader(content), uint64(n), bh.feed)
		if err != nil {
			t.Fatal(err)
		}
		bh.setSHA256(staged.ContentHash)
		if bh.n != int64(n) {
			t.Fatalf("%d bytes fed, want %d", bh.n, n)
		}
		sameDigests(t, digests(t, bh), oldDigests(t, content))
		if want := sha256.Sum256(content); bh.sha256Hex() != hex.EncodeToString(want[:]) {
			t.Fatal("the signed payload hash is not the body's SHA-256")
		}
		r.same(staged, content)
		staged.Release()
	}
}
