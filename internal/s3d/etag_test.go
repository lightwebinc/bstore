package s3d

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// A body's ETag digest in each mode: the MD5 with etag_md5, else one
// SHA-256, and none added when a SHA-256 is computed already.
func TestBodyHashesETagModes(t *testing.T) {
	content := randBytes(300<<10 + 7)
	md5sum := md5.Sum(content)
	shasum := sha256.Sum256(content)
	for _, tc := range []struct {
		name    string
		etagMD5 bool
		header  http.Header
		want    string
	}{
		{"md5", true, http.Header{}, hex.EncodeToString(md5sum[:])},
		{"sha256", false, http.Header{}, hex.EncodeToString(shasum[:])},
		{"sha256 declared", false, http.Header{"X-Amz-Checksum-Sha256": {base64.StdEncoding.EncodeToString(shasum[:])}}, hex.EncodeToString(shasum[:])},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := newBodyHashes(tc.header, "", tc.etagMD5)
			if !tc.etagMD5 {
				bh.needSHA256()
			}
			if n := len(bh.all()); n != 1 {
				t.Fatalf("%d digests, want one", n)
			}
			if _, err := io.Copy(io.Discard, bh.reader(bytes.NewReader(content))); err != nil {
				t.Fatal(err)
			}
			got := bh.sha256Hex
			if tc.etagMD5 {
				got = bh.md5Hex
			}
			if g := got(); g != tc.want {
				t.Fatalf("etag %s, want %s", g, tc.want)
			}
			if e := bh.check(tc.header, nil); e != nil {
				t.Fatal(e)
			}
		})
	}
}

// A multipart upload completes in each mode by the ETags its parts were
// given, its ETag the MD5 of MD5s or the content hash; a part ETag that
// does not match is refused.
func TestMultipartCompletesInEachMode(t *testing.T) {
	for _, etagMD5 := range []bool{true, false} {
		t.Run(fmt.Sprintf("etag_md5=%v", etagMD5), func(t *testing.T) {
			r := newMPRig(t)
			up := r.create()
			p := [][]byte{randBytes(minPart + 3), randBytes(1234)}
			r.putETag(up, 1, p[0], etagMD5)
			r.putETag(up, 2, p[1], etagMD5)
			up.mu.Lock()
			held := map[int]partRecord{1: up.parts[1], 2: up.parts[2]}
			up.mu.Unlock()
			for i, c := range p {
				var want string
				if etagMD5 {
					sum := md5.Sum(c)
					want = hex.EncodeToString(sum[:])
				} else {
					sum := sha256.Sum256(c)
					want = hex.EncodeToString(sum[:])
				}
				if got := held[i+1].etag(); got != want {
					t.Fatalf("part %d etag %s, want %s", i+1, got, want)
				}
			}
			parts := []completePart{{1, `"` + held[1].etag() + `"`}, {2, `"` + held[2].etag() + `"`}}
			names, recs, total, e := pickParts(parts, held)
			if e != nil || len(names) != 2 || total != int64(len(p[0])+len(p[1])) {
				t.Fatal(names, total, e)
			}
			wrong := []completePart{{1, parts[0].ETag}, {2, `"` + held[1].etag() + `"`}}
			if _, _, _, e := pickParts(wrong, held); e == nil || e.Code != "InvalidPart" {
				t.Fatalf("mismatched part ETag: %v, want InvalidPart", e)
			}
			staged, bad, _ := r.complete(up, names, nil)
			if bad != nil {
				t.Fatal(bad)
			}
			defer staged.Release()
			content := cat(p...)
			if sum := sha256.Sum256(content); staged.ContentHash != sum {
				t.Fatal("the content hash is not the content's SHA-256")
			}
			want := hex.EncodeToString(staged.ContentHash[:])
			if etagMD5 {
				h := md5.New()
				for _, c := range p {
					sum := md5.Sum(c)
					h.Write(sum[:])
				}
				want = hex.EncodeToString(h.Sum(nil)) + "-2"
			}
			if got := uploadETag(etagMD5, recs, staged.ContentHash); got != want {
				t.Fatalf("upload etag %s, want %s", got, want)
			}
		})
	}
}

// A part record written by the older binary, an MD5 under "md5" only,
// still completes in the default mode.
func TestMultipartOldPartRecordCompletes(t *testing.T) {
	r := newMPRig(t)
	up := r.create()
	content := randBytes(777)
	if err := os.WriteFile(filepath.Join(up.dir, "part-00001"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum(content)
	md5h := hex.EncodeToString(sum[:])
	_ = writeSynced(filepath.Join(up.dir, "part-00001.json"), []byte(fmt.Sprintf(`{"size":%d,"md5":%q}`, len(content), md5h)))
	up.mu.Lock()
	up.loadParts()
	held := map[int]partRecord{1: up.parts[1]}
	up.mu.Unlock()
	if held[1].etag() != md5h {
		t.Fatalf("etag %q, want the record's MD5", held[1].etag())
	}
	names, recs, _, e := pickParts([]completePart{{1, `"` + md5h + `"`}}, held)
	if e != nil {
		t.Fatal(e)
	}
	staged, bad, _ := r.complete(up, names, nil)
	if bad != nil {
		t.Fatal(bad)
	}
	defer staged.Release()
	if got := uploadETag(false, recs, staged.ContentHash); got != hex.EncodeToString(staged.ContentHash[:]) {
		t.Fatalf("upload etag %s, want the content hash", got)
	}
	ofMD5s := md5.Sum(sum[:])
	if got := uploadETag(true, recs, staged.ContentHash); got != hex.EncodeToString(ofMD5s[:])+"-1" {
		t.Fatalf("upload etag %s, want the MD5 of MD5s", got)
	}
}
