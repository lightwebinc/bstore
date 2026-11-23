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
	// failClosure is how many tellings fail before one is taken.
	failClosure int
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

func (l *keepLocal) Held(h [32]byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.m[h]
	return ok
}

func (l *keepLocal) Closure(root [32]byte, size uint64, branches, blobs [][32]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failClosure > 0 {
		l.failClosure--
		return errors.New("no room for the manifest")
	}
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
	// A read that finds the closure whole tells it again: that is how a
	// Keeper learns it is in use.
	if len(l.closures) != 2 || l.closures[1].root != e.Root {
		t.Fatalf("a read served locally told %d closure(s)", len(l.closures))
	}
}

// Ranged reads, each of one blob, fill a closure between them: no read
// fetched it all, and the one that holds the last object tells it.
func TestKeeperRangesCompleteAClosure(t *testing.T) {
	content := randomContent(2*storerec.BlobSize + 999)
	r, e, objects, _ := advertised(t, content)
	l := &keepLocal{m: map[[32]byte][]byte{}}
	ctx := context.Background()
	for i := uint64(0); i < 3; i++ {
		o, err := r.Open(ctx, e, l)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		off := i*storerec.BlobSize + 7
		if err := o.WriteRange(ctx, &out, off, 50); err != nil || !bytes.Equal(out.Bytes(), content[off:off+50]) {
			t.Fatalf("range %d: %v", i, err)
		}
		if want := map[uint64]int{0: 0, 1: 0, 2: 1}[i]; len(l.closures) != want {
			t.Fatalf("after range %d the closure was told %d time(s), want %d", i, len(l.closures), want)
		}
	}
	if len(l.m) != len(objects) {
		t.Fatalf("%d of %d object(s) held", len(l.m), len(objects))
	}
}

// A telling that fails is tried again; a closure that is refused keeps
// nothing, its nodes included.
func TestKeeperRetriesAndRefusals(t *testing.T) {
	content := randomContent(storerec.BlobSize + 5)
	ctx := context.Background()
	r, e, _, _ := advertised(t, content)
	l := &keepLocal{m: map[[32]byte][]byte{}, failClosure: 1}
	o, err := r.Open(ctx, e, l)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := o.WriteRange(ctx, &out, 0, e.Size); err != nil {
		t.Fatal(err)
	}
	if len(l.closures) != 0 {
		t.Fatal("a telling that failed was counted")
	}
	// The next read finds the closure whole and tells it.
	if _, err := r.Open(ctx, e, l); err != nil {
		t.Fatal(err)
	}
	if len(l.closures) != 1 {
		t.Fatalf("after one failed telling the closure was told %d time(s)", len(l.closures))
	}

	r, e, _, hits := advertised(t, content)
	bad := *e
	bad.Size++ // the root's length is not the entry's: CheckRoot refuses it
	l = &keepLocal{m: map[[32]byte][]byte{}}
	if _, err := r.Open(ctx, &bad, l); !errors.Is(err, ErrRefused) {
		t.Fatalf("a closure whose size is not the entry's opened: %v", err)
	}
	if hits.Load() == 0 || len(l.m) != 0 || l.keeps != 0 {
		t.Fatalf("a refused closure kept %d object(s) (%d fetched)", len(l.m), hits.Load())
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
