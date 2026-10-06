package provider

import (
	"crypto/sha256"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/lightwebinc/bstore/backend"
	"github.com/lightwebinc/bstore/storerec"
)

// The closure checked on arrival, pieces in any order, is the one the full
// check verifies; anything missing leaves it to the full check.
func TestQuickClosureMatchesFull(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, size := range []int{1, storerec.BlobSize, 3*storerec.BlobSize + 7} {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(rng.Uint32())
		}
		b, err := storerec.Build(content)
		if err != nil {
			t.Fatal(err)
		}
		st, err := backend.OpenNative(t.TempDir(), "http://x")
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(dir, "roots"), 0o700)
		p := &Provider{store: st, inc: newIncremental(), cfg: &Config{Data: dir}}
		objs := b.Objects()
		rng.Shuffle(len(objs), func(i, j int) { objs[i], objs[j] = objs[j], objs[i] })
		for k, o := range objs {
			h := sha256.Sum256(o)
			if err := st.Put(h, o); err != nil {
				t.Fatal(err)
			}
			p.arrived(h, o, b.RootHash)
			if k < len(objs)-1 && p.quickClosure(b.RootHash, b.Root) != nil {
				t.Fatalf("size %d: a closure before its last piece", size)
			}
		}
		q := p.quickClosure(b.RootHash, b.Root)
		if q == nil {
			t.Fatalf("size %d: no quick closure", size)
		}
		full, err := storerec.VerifyCHIRP(b.RootHash, p.fetch)
		if err != nil {
			t.Fatal(err)
		}
		if q.RootHash != full.RootHash || !q.Canonical || !slices.Equal(q.Objects, full.Objects) || !slices.Equal(q.Blobs, full.Blobs) {
			t.Fatalf("size %d: quick closure differs", size)
		}
		pr, _, err := p.writeProofFiles("ab", q)
		if err != nil {
			t.Fatal(err)
		}
		if pr != b.ProofRoot {
			t.Fatalf("size %d: proof root", size)
		}
	}
}
