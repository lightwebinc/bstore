package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	bpub "github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

const testBucket = "example_photos_hmqzvtkwrx"

// target is a submit route that records what it is sent, and can be made
// to fail.
type target struct {
	mu   sync.Mutex
	got  map[string][][32]byte // topic -> hashes of the bodies
	down atomic.Bool
	srv  *httptest.Server
}

func newTarget(t *testing.T) *target {
	tg := &target{got: map[string][][32]byte{}}
	tg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tg.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		b, _ := io.ReadAll(r.Body)
		tg.mu.Lock()
		tg.got[r.Header.Get("x-topics")] = append(tg.got[r.Header.Get("x-topics")], sha256.Sum256(b))
		tg.mu.Unlock()
		_, _ = w.Write([]byte(`{"` + r.Header.Get("x-topics") + `":{"outputsToAdmit":[0],"coinsToRetain":[]}}`))
	}))
	t.Cleanup(tg.srv.Close)
	return tg
}

func (tg *target) count(topic string) int {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return len(tg.got[topic])
}

func open(t *testing.T, ctx context.Context, dir string, cs *httptest.Server, store *Store, d *Delivery) *Publisher {
	t.Helper()
	p, err := Open(ctx, Config{Dir: dir, Chain: nodeChain(cs.URL), Tip: nodeTip(cs.URL), Settler: &bpub.RPCSettler{RPC: &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}},
		Fees: mint.DefaultFees, PieceTree: 8, EntryTree: 4, HoldAhead: 4, Store: store, Delivery: d, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPutDeliversAndRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cs := httptest.NewServer(testchain.New(700))
	defer cs.Close()
	dir := t.TempDir()
	if _, err := Init(filepath.Join(dir, "id")); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	host, prov := newTarget(t), newTarget(t)
	d := &Delivery{Hosts: []string{host.srv.URL}, Providers: []string{prov.srv.URL}}
	p := open(t, ctx, filepath.Join(dir, "id"), cs, store, d)
	w := p.Wallet()
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		t.Fatal(err)
	}

	// 9 MiB: three blobs and the root, four pieces; a bounty first.
	content := bytes.Repeat([]byte("bstore publish test, "), 9<<20/21+1)[:9<<20]
	st, err := store.Stage(ctx, bytes.NewReader(content), uint64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	want, _ := storerec.Build(content)
	if st.Root != want.RootHash || st.ProofRoot != want.ProofRoot || st.ContentHash != want.ContentHash || st.Size != want.Size {
		t.Fatal("the streamed closure is not the one built in memory")
	}
	f := Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: "a/b", ContentType: "application/octet-stream"}
	obj := Object{Root: st.Root, Size: st.Size, ContentHash: st.ContentHash, ProofRoot: st.ProofRoot, Closure: StoreClosure{Store: store, Manifest: st.Manifest}}
	r, err := p.Put(ctx, f, obj, &Bounty{Satoshis: 1000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 10, CheckC: 30})
	if err == nil {
		err = r.Wait(ctx)
	}
	if err != nil || !r.Sent || r.Pieces != 4 || r.Entry.Hold == nil {
		t.Fatalf("put: %v %+v", err, r)
	}
	blob, records := storerec.BlobTopic(testBucket), storerec.RecordsTopic(testBucket)
	if prov.count(blob) != 4 || prov.count(records) != 1 || host.count(records) != 2 || host.count(blob) != 0 {
		t.Fatalf("delivered: provider %d pieces %d entries; host %d records, %d pieces", prov.count(blob), prov.count(records), host.count(records), host.count(blob))
	}
	if p.Outbox().Pending() != 0 {
		t.Fatal("delivered objects left in the outbox")
	}
	// The hold outputs are minted ahead; exactly one is taken, by this
	// entry, and the rest stay ready.
	used, ready := 0, 0
	for _, h := range p.Holds() {
		switch {
		case h.Used && h.Entry == r.Txid && h.BumpHex != "":
			used++
		case !h.Used && h.BumpHex != "":
			ready++
		}
	}
	if used != 1 || ready < 1 {
		t.Fatalf("hold records: %d used by the entry, %d ready", used, ready)
	}

	// The provider is down: the put answers all the same, the host takes
	// the entry, and the outbox owes the provider the pieces and the entry.
	prov.down.Store(true)
	r1, err := p.Put(ctx, Fields{Bucket: testBucket, ID: f.ID, Key: "a/c", ContentType: "text/plain"}, obj, nil)
	if err == nil {
		err = r1.Wait(ctx)
	}
	if err != nil || !r1.Sent {
		t.Fatalf("put with the provider down: %v", err)
	}
	if n := p.Outbox().Pending(); n != 5 {
		t.Fatalf("outbox holds %d carrier(s) owed to the provider, want 5", n)
	}
	// The host is down: the entry is minted, not sent; the outbox keeps it.
	prov.down.Store(false)
	host.down.Store(true)
	r2, err := p.Delete(ctx, testBucket, f.ID, "a/b", time.Time{})
	if !errors.Is(err, ErrNotSent) || r2 == nil {
		t.Fatalf("delete with the host down: %v", err)
	}
	host.down.Store(false)
	// Reopen, as after a restart: the outbox and the trees are kept.
	p.Close()
	p2 := open(t, ctx, filepath.Join(dir, "id"), cs, store, d)
	time.Sleep(10 * time.Millisecond)
	p2.Outbox().now = func() time.Time { return time.Now().Add(time.Hour) }
	p2.Outbox().Retry(ctx)
	if p2.Outbox().Pending() != 0 {
		t.Fatalf("after the retry the outbox holds %d", p2.Outbox().Pending())
	}
	if host.count(records) != 4 {
		t.Fatalf("the delete entry reached the host %d times", host.count(records)-3)
	}
	if prov.count(blob) != 8 || prov.count(records) != 3 {
		t.Fatalf("the provider took %d pieces and %d records, want 8 and 3", prov.count(blob), prov.count(records))
	}
	// The entry tree's outputs were recorded used before their carriers:
	// the next entry spends the next output.
	p2.mu.Lock()
	next := p2.st.Entry.Tree.Next
	p2.mu.Unlock()
	if next != 3 {
		t.Fatalf("entry tree next output %d, want 3", next)
	}
}

func TestStageBound(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stage(context.Background(), bytes.NewReader(make([]byte, 101)), 100); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over the bound: %v", err)
	}
	st, err := store.Stage(context.Background(), bytes.NewReader(nil), 0)
	if err != nil || st.Root != storerec.EmptyRoot || st.ProofRoot != storerec.EmptyHash {
		t.Fatalf("empty: %v", err)
	}
	st.Release()
	if n, err := store.Trim(0); err != nil || n != 0 {
		t.Fatalf("trim: %d %v", n, err)
	}
}

// A put cut short leaves blobs no closure names; Trim removes them once
// they are older than orphanAge, and never a pinned one.
func TestTrimSweepsOrphans(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The body fails only once its first blob is on disk: a stage aborted
	// earlier stores nothing, which would leave no orphan to sweep.
	r := io.MultiReader(bytes.NewReader(make([]byte, 5<<20)), &failing{after: filepath.Join(dir, "objects")})
	if _, err := store.Stage(context.Background(), r, 10<<20); err == nil {
		t.Fatal("a body that failed was staged")
	}
	n := 0
	_ = filepath.Walk(filepath.Join(dir, "objects"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			n++
			old := time.Now().Add(-2 * orphanAge)
			_ = os.Chtimes(p, old, old)
		}
		return nil
	})
	if n != 1 {
		t.Fatalf("%d objects after a failed stage", n)
	}
	if got, err := store.Trim(1 << 40); err != nil || got != 0 {
		t.Fatalf("after the sweep %d bytes are held (%v)", got, err)
	}
}

// failing is a body that goes away. With after set, it first waits (up to
// ten seconds) for a file to be written under that directory.
type failing struct{ after string }

func (f *failing) Read([]byte) (int, error) {
	for end := time.Now().Add(10 * time.Second); f.after != "" && time.Now().Before(end); time.Sleep(time.Millisecond) {
		found := false
		_ = filepath.Walk(f.after, func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				found = true
			}
			return nil
		})
		if found {
			break
		}
	}
	return 0, errors.New("the client went away")
}

// Grown counts what was written since the last Trim began, so a caller can
// trim a store that a heavy ingest fills between two trims on the clock.
func TestStoreGrownCountsWritesSinceTheLastTrim(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if g := store.Grown(); g != 0 {
		t.Fatalf("a new store has grown %d", g)
	}
	body := make([]byte, 5<<20)
	body[0] = 1
	st, err := store.Stage(context.Background(), bytes.NewReader(body), 10<<20)
	if err != nil {
		t.Fatal(err)
	}
	st.Release()
	if g := store.Grown(); g < int64(len(body)) {
		t.Fatalf("grown %d after staging %d bytes", g, len(body))
	}
	if _, err := store.Trim(1 << 40); err != nil {
		t.Fatal(err)
	}
	if g := store.Grown(); g != 0 {
		t.Fatalf("grown %d after a trim", g)
	}
	// The same bytes again are already held: nothing is written.
	st, err = store.Stage(context.Background(), bytes.NewReader(body), 10<<20)
	if err != nil {
		t.Fatal(err)
	}
	st.Release()
	if g := store.Grown(); g != 0 {
		t.Fatalf("grown %d after staging bytes already held", g)
	}
}
