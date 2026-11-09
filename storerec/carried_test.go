package storerec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lightwebinc/bcommon/chirp"
)

// The leaves NewParallelLeaves hands over with each blob are LeavesOf's,
// their subtree root is the blob's share of the proof root, and the build
// is NewParallel's.
func TestParallelLeaves(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, size := range []int{0, 1, SegmentSize + 1, BlobSize, 2*BlobSize + 7} {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(rng.Uint32())
		}
		want, err := Build(content)
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		got := map[int][]byte{}
		p := NewParallelLeaves(3, func(i int, h [32]byte, blob, leaves []byte) error {
			wantLeaves, sub := LeavesOf(blob)
			if !bytes.Equal(leaves, wantLeaves) || len(leaves) != LeavesLen(len(blob)) {
				t.Errorf("size %d blob %d: leaves differ from LeavesOf", size, i)
			}
			if RootOfLeaves(leaves) != sub || sub != SegmentRootOf(blob) {
				t.Errorf("size %d blob %d: the leaves' root is not the blob's segment root", size, i)
			}
			mu.Lock()
			got[i] = leaves
			mu.Unlock()
			return nil
		})
		if _, err := p.Write(content); err != nil {
			t.Fatal(err)
		}
		b, err := p.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if b.RootHash != want.RootHash || b.ProofRoot != want.ProofRoot || len(got) != len(want.Blobs) {
			t.Fatalf("size %d: the build differs from Build's", size)
		}
	}
	if RootOfLeaves(nil) != SegmentRootOf(nil) || LeavesLen(0) != 0 || LeavesLen(SegmentSize) != 32 || LeavesLen(SegmentSize+1) != 64 {
		t.Fatal("empty leaves or LeavesLen")
	}
}

// BRC-167's own vectors: with no blob held, verifyCarried refuses every
// closure exactly as chirp.Verify does, but for the content hash, which it
// leaves to the account it is called on. With every blob served at its
// reference's length held, it also takes a blob's bytes unread, so a blob
// served as other bytes is not refused; every node is still hashed.
func TestVerifyCarriedVectors(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/lightwebinc/bcommon").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(out)), "testdata", "vectors", "chirp-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Closures []struct {
			Name, RootHashHex string
			Objects           []struct{ HashHex, Hex string }
			MaxReferences     int
			MaxLength         uint64
		}
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	unhex := func(s string) []byte {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	contentHash, blobHash := 0, 0
	for _, c := range v.Closures {
		objs := map[[32]byte][]byte{}
		for _, o := range c.Objects {
			objs[[32]byte(unhex(o.HashHex))] = unhex(o.Hex)
		}
		fetch := func(h [32]byte) ([]byte, error) {
			if o, ok := objs[h]; ok {
				return o, nil
			}
			return nil, errors.New("not served")
		}
		root := [32]byte(unhex(c.RootHashHex))
		lim := chirp.Limits{MaxReferences: c.MaxReferences, MaxLength: c.MaxLength}
		want, werr := chirp.Verify(root, fetch, lim)
		wantCode, _ := chirp.Reason(werr)

		none, nerr := verifyCarried(root, fetch, func(chirp.Child) bool { return false }, lim)
		noneCode, _ := chirp.Reason(nerr)
		switch {
		case wantCode == "content-hash":
			contentHash++
			if nerr != nil {
				t.Errorf("%s, none held: %v, want the content hash left to the account", c.Name, nerr)
			}
		case noneCode != wantCode:
			t.Errorf("%s, none held: %q, want %q", c.Name, noneCode, wantCode)
		case werr == nil && (none.Canonical != want.Canonical || len(none.Blobs) != len(want.Blobs) || len(none.Objects) != len(want.Objects)):
			t.Errorf("%s, none held: the closure differs from chirp.Verify's", c.Name)
		}

		held := func(r chirp.Child) bool { o, ok := objs[r.Hash]; return ok && uint64(len(o)) == r.Length }
		_, aerr := verifyCarried(root, fetch, held, lim)
		allCode, _ := chirp.Reason(aerr)
		switch {
		case noneCode == "hash" && strings.Contains(nerr.Error(), "blob ") && allCode == "":
			blobHash++
		case wantCode == "content-hash" && aerr == nil:
		case allCode != noneCode:
			t.Errorf("%s, all held: %q, want %q", c.Name, allCode, noneCode)
		}
	}
	t.Logf("%d closures: %d content-hash refusals left to the account, %d blob-hash refusals taken unread when held", len(v.Closures), contentHash, blobHash)
	if contentHash == 0 || blobHash == 0 {
		t.Fatalf("the vectors no longer cover a content hash (%d) or a blob's hash (%d) refusal", contentHash, blobHash)
	}
}

// A closure with branches, every blob held: only the root and the branches
// are read; a blob that is not held is read, held to its hash, and refused
// as VerifyCHIRP refuses it.
func TestVerifyCarriedReadsOnlyNodes(t *testing.T) {
	tail := []byte("the last blob, read")
	refs := make([]chirp.Child, chirp.Fanout+44)
	for i := range refs {
		refs[i] = chirp.Child{Kind: chirp.ChildBlob, Length: chirp.ChunkSize, Hash: sha256.Sum256([]byte{byte(i), byte(i >> 8)})}
	}
	last := len(refs) - 1
	refs[last] = chirp.Child{Kind: chirp.ChildBlob, Length: uint64(len(tail)), Hash: sha256.Sum256(tail)}
	built, err := chirp.BuildTree(refs, sha256.Sum256([]byte("the gateway's content hash")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(built.Branches) != 2 {
		t.Fatalf("%d branches", len(built.Branches))
	}
	serve := func(swap map[[32]byte][]byte, reads *int) chirp.Fetch {
		objs := map[[32]byte][]byte{built.RootHash: built.Root, refs[last].Hash: tail}
		for _, b := range built.Branches {
			objs[sha256.Sum256(b)] = b
		}
		return func(h [32]byte) ([]byte, error) {
			*reads++
			if o, ok := swap[h]; ok {
				return o, nil
			}
			if o, ok := objs[h]; ok {
				return o, nil
			}
			return nil, errors.New("not served")
		}
	}
	all := func(chirp.Child) bool { return true }
	reads := 0
	c, err := VerifyCarried(built.RootHash, serve(nil, &reads), all)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Canonical || len(c.Blobs) != len(refs) || len(c.Objects) != 1+2+len(refs) || reads != 3 {
		t.Fatalf("canonical %v, %d blobs, %d objects, %d reads (want 3: the nodes)", c.Canonical, len(c.Blobs), len(c.Objects), reads)
	}

	allBut := func(i int) func(chirp.Child) bool {
		return func(r chirp.Child) bool { return r.Hash != refs[i].Hash }
	}
	reads = 0
	if _, err := VerifyCarried(built.RootHash, serve(nil, &reads), allBut(last)); err != nil || reads != 4 {
		t.Fatalf("a blob not held, served: %v, %d reads", err, reads)
	}
	damaged := bytes.Clone(tail)
	damaged[0] ^= 1
	cases := []struct {
		name  string
		swap  map[[32]byte][]byte
		held  func(chirp.Child) bool
		label error
		rule  string
	}{
		{"a blob not held, not served", nil, allBut(7), ErrClosure, "missing"},
		{"a blob not held, served as other bytes", map[[32]byte][]byte{refs[last].Hash: damaged}, allBut(last), ErrClosure, "hash"},
		{"a blob not held, served short", map[[32]byte][]byte{refs[last].Hash: tail[1:]}, allBut(last), ErrRoot, "length"},
		{"a branch served as other bytes", map[[32]byte][]byte{sha256.Sum256(built.Branches[1]): built.Branches[0]}, all, ErrClosure, "hash"},
		{"the root served as other bytes", map[[32]byte][]byte{built.RootHash: built.Branches[0]}, all, ErrClosure, "hash"},
	}
	for _, tc := range cases {
		reads = 0
		_, err := VerifyCarried(built.RootHash, serve(tc.swap, &reads), tc.held)
		if !errors.Is(err, tc.label) || Rule(err) != tc.rule {
			t.Errorf("%s: %v (rule %q), want %v %q", tc.name, err, Rule(err), tc.label, tc.rule)
		}
	}

	// Two short blobs are not profile 1's construction: refused as a root
	// fault, without reading a blob.
	two, err := chirp.EncodeRoot(chirp.Root{Profile: chirp.Profile1, Length: 2 * refs[last].Length, ContentHash: sha256.Sum256([]byte("x")),
		Children: []chirp.Child{refs[last], refs[last]}})
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(two)
	reads = 0
	if _, err := VerifyCarried(h, serve(map[[32]byte][]byte{h: two}, &reads), all); !errors.Is(err, ErrRoot) || Rule(err) != "canonical" || reads != 1 {
		t.Fatalf("two short blobs: %v (rule %q), %d reads", err, Rule(err), reads)
	}
}
