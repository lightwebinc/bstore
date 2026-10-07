package provider

import (
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/lightwebinc/bstore/backend"
	"github.com/lightwebinc/bstore/storerec"
)

// The closure walked as pieces arrive, in any order, is the one the full
// check verifies, and is missing (closure) until the last piece; a walk
// made after a restart reads back what the store holds and agrees too.
func TestIncrementalClosureMatchesFull(t *testing.T) {
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
			p.arrived(h, o, b.RootHash, true)
			if k < len(objs)-1 {
				if _, err := p.checkClosure(b.RootHash); !errors.Is(err, storerec.ErrClosure) {
					t.Fatalf("size %d: before the last piece: %v", size, err)
				}
			}
		}
		full, err := storerec.VerifyCHIRP(b.RootHash, p.fetch)
		if err != nil {
			t.Fatal(err)
		}
		restarted := &Provider{store: st, inc: newIncremental(), cfg: p.cfg}
		for _, pp := range []*Provider{p, restarted} {
			q, err := pp.checkClosure(b.RootHash)
			if err != nil {
				t.Fatalf("size %d: %v", size, err)
			}
			if q.RootHash != full.RootHash || !q.Canonical || !slices.Equal(q.Objects, full.Objects) || !slices.Equal(q.Blobs, full.Blobs) {
				t.Fatalf("size %d: incremental closure differs", size)
			}
			pr, _, err := pp.writeProofFiles("ab", q)
			if err != nil {
				t.Fatal(err)
			}
			if pr != b.ProofRoot {
				t.Fatalf("size %d: proof root", size)
			}
		}
	}
}

// A store serving other bytes for a piece is labeled closure hash, as
// VerifyCHIRP labels it, and a bad content hash is content.
func TestIncrementalClosureLabels(t *testing.T) {
	content := make([]byte, 2*storerec.BlobSize+1)
	b, err := storerec.Build(content)
	if err != nil {
		t.Fatal(err)
	}
	st, err := backend.OpenNative(t.TempDir(), "http://x")
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{store: st, inc: newIncremental(), cfg: &Config{Data: t.TempDir()}}
	objs := b.Objects()
	for _, o := range objs[:len(objs)-1] {
		h := sha256.Sum256(o)
		_ = st.Put(h, o)
		p.arrived(h, o, b.RootHash, true)
	}
	last := objs[len(objs)-1]
	lh := sha256.Sum256(last)
	_ = st.Put(lh, append([]byte{1}, last[1:]...))
	_, err = p.checkClosure(b.RootHash)
	_, want := storerec.VerifyCHIRP(b.RootHash, p.fetch)
	if !errors.Is(err, storerec.ErrClosure) || err.Error() != want.Error() {
		t.Fatalf("got %v, VerifyCHIRP %v", err, want)
	}
}

// A good copy sent again does not vouch for a damaged copy the store
// holds: the walk reads the store's, as VerifyCHIRP does.
func TestIncrementalResentPieceDoesNotMaskDamage(t *testing.T) {
	content := make([]byte, 2*storerec.BlobSize+1)
	content[0] = 7
	b, err := storerec.Build(content)
	if err != nil {
		t.Fatal(err)
	}
	st, err := backend.OpenNative(t.TempDir(), "http://x")
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{store: st, inc: newIncremental(), cfg: &Config{Data: t.TempDir()}}
	objs := b.Objects()
	last := objs[len(objs)-1]
	lh := sha256.Sum256(last)
	_ = st.Put(lh, append([]byte{last[0] ^ 1}, last[1:]...))
	for _, o := range objs[:len(objs)-1] {
		h := sha256.Sum256(o)
		_ = st.Put(h, o)
		p.arrived(h, o, b.RootHash, true)
	}
	p.arrived(lh, last, b.RootHash, false)
	_, err = p.checkClosure(b.RootHash)
	_, want := storerec.VerifyCHIRP(b.RootHash, p.fetch)
	if !errors.Is(err, storerec.ErrClosure) || want == nil || err.Error() != want.Error() {
		t.Fatalf("got %v, VerifyCHIRP %v", err, want)
	}
}

// Walks are dropped least recently used first, and a dropped walk is not
// started again by a piece other than its root's.
func TestIncrementalWalksBounded(t *testing.T) {
	st, err := backend.OpenNative(t.TempDir(), "http://x")
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{store: st, inc: newIncremental(), cfg: &Config{Data: t.TempDir()}}
	root := func(i int) [32]byte { return sha256.Sum256([]byte{byte(i), byte(i >> 8)}) }
	for i := range maxRoots + 10 {
		r := root(i)
		p.arrived(r, []byte{byte(i), byte(i >> 8)}, r, true)
		if i > 0 {
			p.walk(root(0), false) // kept in use
		}
	}
	if len(p.inc.walks) != maxRoots || len(p.inc.wfifo) != maxRoots {
		t.Fatalf("%d walks, %d in order", len(p.inc.walks), len(p.inc.wfifo))
	}
	if p.walk(root(0), false) == nil {
		t.Fatal("the walk in use was dropped")
	}
	if p.walk(root(1), false) != nil {
		t.Fatal("the oldest idle walk was kept")
	}
	p.arrived(sha256.Sum256([]byte("blob")), []byte("blob"), root(1), true)
	if p.walk(root(1), false) != nil {
		t.Fatal("a dropped walk was started again by a blob")
	}
	p.inc.forget(root(0), nil)
	if len(p.inc.wfifo) != maxRoots-1 {
		t.Fatal("forget left the walk in the order")
	}
}
