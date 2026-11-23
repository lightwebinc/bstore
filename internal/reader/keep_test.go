package reader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/storerec"
)

// keepLocal is a Keeper over a map: what it was told is counted.
type keepLocal struct {
	mu       sync.Mutex
	m        map[[32]byte][]byte
	keeps    int
	closures []keptClosure
	failKeep bool
}

type keptClosure struct {
	root     [32]byte
	size     uint64
	branches [][32]byte
	blobs    [][32]byte
}

func (l *keepLocal) Get(h [32]byte) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.m[h]
	if !ok {
		return nil, errors.New("missing")
	}
	return b, nil
}

func (l *keepLocal) Keep(h [32]byte, b []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failKeep {
		return errors.New("disk full")
	}
	l.keeps++
	l.m[h] = b
	return nil
}

func (l *keepLocal) Closure(root [32]byte, size uint64, branches, blobs [][32]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closures = append(l.closures, keptClosure{root, size, branches, blobs})
	return nil
}

// advertised is an object's closure served by one advertiser, and a reader
// that knows it as the root's only source. hits counts the advertiser's
// answers.
func advertised(t *testing.T, content []byte) (*Reader, *storerec.Entry, map[[32]byte][]byte, *atomic.Int64) {
	t.Helper()
	objects := map[[32]byte][]byte{}
	var blobs []chirp.Child
	for off := 0; off < len(content); off += storerec.BlobSize {
		b := content[off:min(off+storerec.BlobSize, len(content))]
		h := sha256.Sum256(b)
		objects[h] = b
		blobs = append(blobs, chirp.Child{Kind: chirp.ChildBlob, Length: uint64(len(b)), Hash: h})
	}
	contentHash := sha256.Sum256(content)
	built, err := chirp.BuildTree(blobs, contentHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	objects[built.RootHash] = built.Root
	for _, br := range built.Branches {
		objects[sha256.Sum256(br)] = br
	}
	byID := map[string][]byte{}
	for h, b := range objects {
		byID[chirp.Identifier(h)] = b
	}
	hits := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := byID[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		hits.Add(1)
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)
	r := &Reader{Logf: t.Logf, Fetch: srv.Client()}
	root := built.RootHash
	r.ads.put(root, []Source{{Advertiser: strings.Repeat("02", 33), URL: srv.URL + "/chirp/v1/x/objects/" + chirp.Identifier(root)}}, r.now())
	return r, &storerec.Entry{Bucket: "b", Root: root, Size: uint64(len(content)), ContentHash: contentHash}, objects, hits
}

func randomContent(n int) []byte {
	b := make([]byte, n)
	rng := rand.NewChaCha8([32]byte{7})
	_, _ = rng.Read(b)
	return b
}

// A whole read through a Keeper keeps every object it fetched, tells the
// closure once, and the next read asks no advertiser.
func TestKeeperKeepsWhatIsFetched(t *testing.T) {
	content := randomContent(2*storerec.BlobSize + 12345)
	r, e, objects, hits := advertised(t, content)
	l := &keepLocal{m: map[[32]byte][]byte{}}
	ctx := context.Background()

	o, err := r.Open(ctx, e, l)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := o.WriteRange(ctx, &out, 0, e.Size); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), content) {
		t.Fatal("the read differs")
	}
	if l.keeps != len(objects) || len(l.m) != len(objects) {
		t.Fatalf("kept %d object(s), %d held, of %d", l.keeps, len(l.m), len(objects))
	}
	if len(l.closures) != 1 {
		t.Fatalf("the closure was told %d time(s)", len(l.closures))
	}
	c := l.closures[0]
	if c.root != e.Root || c.size != e.Size || len(c.blobs) != 3 {
		t.Fatalf("closure: root %x size %d blobs %d", c.root[:4], c.size, len(c.blobs))
	}
	for i, h := range c.blobs {
		if h != sha256.Sum256(content[i*storerec.BlobSize:min((i+1)*storerec.BlobSize, len(content))]) {
			t.Fatalf("blob %d is not in content order", i)
		}
	}

	before := hits.Load()
	o, err = r.Open(ctx, e, l)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := o.WriteRange(ctx, &out, 0, e.Size); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), content) {
		t.Fatal("the second read differs")
	}
	if hits.Load() != before {
		t.Fatalf("the second read asked the advertiser %d time(s)", hits.Load()-before)
	}
	if len(l.closures) != 1 {
		t.Fatal("a read served locally told the closure again")
	}
}

// A range keeps the blobs it read; the closure is not told until every
// object is held, and is told when a later read completes it.
func TestKeeperRangeThenWhole(t *testing.T) {
	content := randomContent(2*storerec.BlobSize + 999)
	r, e, objects, _ := advertised(t, content)
	l := &keepLocal{m: map[[32]byte][]byte{}}
	ctx := context.Background()
	o, err := r.Open(ctx, e, l)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := o.WriteRange(ctx, &out, 10, 100); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), content[10:110]) {
		t.Fatal("the range differs")
	}
	if len(l.closures) != 0 {
		t.Fatal("a closure held in part was told")
	}
	if len(l.m) >= len(objects) || len(l.m) == 0 {
		t.Fatalf("a range kept %d of %d object(s)", len(l.m), len(objects))
	}
	o, err = r.Open(ctx, e, l)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := o.WriteRange(ctx, &out, 0, e.Size); err != nil {
		t.Fatal(err)
	}
	if len(l.closures) != 1 || len(l.m) != len(objects) {
		t.Fatalf("after the whole read: %d closure(s), %d of %d held", len(l.closures), len(l.m), len(objects))
	}
}

// A keep that fails costs nothing but the keeping; a Local that is no
// Keeper is read as before and nothing is kept.
func TestKeepFailureAndPlainLocal(t *testing.T) {
	content := randomContent(storerec.BlobSize + 5)
	ctx := context.Background()

	r, e, _, _ := advertised(t, content)
	l := &keepLocal{m: map[[32]byte][]byte{}, failKeep: true}
	o, err := r.Open(ctx, e, l)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := o.WriteRange(ctx, &out, 0, e.Size); err != nil || !bytes.Equal(out.Bytes(), content) {
		t.Fatalf("a failed keep broke the read: %v", err)
	}
	if len(l.m) != 0 || len(l.closures) != 0 {
		t.Fatal("a failed keep was counted as held")
	}

	r, e, _, hits := advertised(t, content)
	plain := &mapLocal{m: map[[32]byte][]byte{}, fail: map[[32]byte]bool{}}
	for i := 0; i < 2; i++ {
		o, err := r.Open(ctx, e, plain)
		if err != nil {
			t.Fatal(err)
		}
		out.Reset()
		if err := o.WriteRange(ctx, &out, 0, e.Size); err != nil || !bytes.Equal(out.Bytes(), content) {
			t.Fatalf("plain local read %d: %v", i, err)
		}
	}
	if len(plain.m) != 0 {
		t.Fatal("a plain Local was written to")
	}
	if hits.Load() != 6 {
		t.Fatalf("two reads through a plain Local asked the advertiser %d time(s), want 6", hits.Load())
	}
}
