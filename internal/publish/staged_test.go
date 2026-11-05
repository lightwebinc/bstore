package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

// orderTarget is a submit route that records the topic of each carrier
// taken, in the order taken, and the txid of each; pieces can be held
// (stall) until released, and the whole target can be down.
type orderTarget struct {
	mu    sync.Mutex
	order []string       // topics, in the order taken
	ids   map[string]int // carrier txid -> times taken
	stall chan struct{}
	down  atomic.Bool
	srv   *httptest.Server
}

func newOrderTarget(t *testing.T) *orderTarget {
	tg := &orderTarget{ids: map[string]int{}, stall: make(chan struct{})}
	tg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tg.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		b, _ := io.ReadAll(r.Body)
		topic := r.Header.Get("x-topics")
		if topic == storerec.BlobTopic(testBucket) {
			select {
			case <-tg.stall:
			case <-r.Context().Done():
				return
			}
		}
		tx, err := transaction.NewTransactionFromBEEF(b)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tg.mu.Lock()
		tg.order = append(tg.order, topic)
		tg.ids[tx.TxID().String()]++
		tg.mu.Unlock()
		_, _ = w.Write([]byte(`{"` + topic + `":{"outputsToAdmit":[0],"coinsToRetain":[]}}`))
	}))
	t.Cleanup(tg.srv.Close)
	return tg
}

func (tg *orderTarget) taken() []string {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return append([]string(nil), tg.order...)
}

// piecesThenEntry checks that, over order[from:], every piece came before
// any record, and counts each.
func piecesThenEntry(t *testing.T, order []string, from int, pieces, records int) {
	t.Helper()
	blob, recs := storerec.BlobTopic(testBucket), storerec.RecordsTopic(testBucket)
	np, nr := 0, 0
	for _, topic := range order[from:] {
		switch topic {
		case blob:
			if nr > 0 {
				t.Fatalf("a piece was taken after the entry: %v", order[from:])
			}
			np++
		case recs:
			nr++
		}
	}
	if np != pieces || nr != records {
		t.Fatalf("taken %d pieces and %d records, want %d and %d: %v", np, nr, pieces, records, order[from:])
	}
}

// A put answers once its carriers are in the outbox: before any target has
// taken a piece. The pieces then arrive, and the entry after every one of
// them. A put whose target is down answers the same, and a restart sends
// what the outbox holds, each carrier once, the pieces before the entry.
func TestPutAnswersFromTheOutbox(t *testing.T) {
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
	tg := newOrderTarget(t)
	d := &Delivery{Hosts: []string{tg.srv.URL}, Providers: []string{tg.srv.URL}}
	p := open(t, ctx, filepath.Join(dir, "id"), cs, store, d)
	w := p.Wallet()
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		t.Fatal(err)
	}
	// 9 MiB: three blobs and the root, four pieces.
	content := bytes.Repeat([]byte("bstore staged put test, "), 9<<20/24+1)[:9<<20]
	st, err := store.Stage(ctx, bytes.NewReader(content), uint64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	f := Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: "a/b", ContentType: "application/octet-stream"}
	obj := Object{Root: st.Root, Size: st.Size, ContentHash: st.ContentHash, ProofRoot: st.ProofRoot, Closure: StoreClosure{Store: store, Manifest: st.Manifest, Staged: true}}

	// The target holds every piece: the put answers anyway.
	t0 := time.Now()
	r, err := p.Put(ctx, f, obj, nil)
	if err != nil || r.Pieces != 4 {
		t.Fatalf("put: %v %+v", err, r)
	}
	if got := tg.taken(); len(got) != 0 {
		t.Fatalf("the put answered after the target took %v", got)
	}
	select {
	case <-r.Delivered:
		t.Fatal("delivered while the target holds the pieces")
	default:
	}
	// What the put committed: the pieces (a live batch, by reference), the
	// entry and its log.
	if _, err := p.EntryBEEF(r.Txid); err != nil {
		t.Fatalf("the entry is not logged: %v", err)
	}
	if batches, _ := filepath.Glob(filepath.Join(dir, "id", "outbox", "*.batch")); len(batches) != 1 {
		t.Fatalf("%d batch files while the put is sending", len(batches))
	}
	close(tg.stall)
	if err := r.Wait(ctx); err != nil {
		t.Fatalf("after %s: %v", time.Since(t0), err)
	}
	piecesThenEntry(t, tg.taken(), 0, 4, 2)
	if n := p.Outbox().Pending(); n != 0 {
		t.Fatalf("%d owed after the sends", n)
	}
	if batches, _ := filepath.Glob(filepath.Join(dir, "id", "outbox", "*.batch")); len(batches) != 0 {
		t.Fatalf("batch files left: %v", batches)
	}

	// The target is down: the put answers the same, owes everything, and
	// the next start sends it, each carrier once, the pieces first.
	tg.down.Store(true)
	before := len(tg.taken())
	r2, err := p.Put(ctx, Fields{Bucket: testBucket, ID: f.ID, Key: "a/c", ContentType: "text/plain"}, obj, nil)
	if err != nil || r2.Pieces != 4 {
		t.Fatalf("put with the target down: %v", err)
	}
	if err := r2.Wait(ctx); err != ErrNotSent {
		t.Fatalf("the entry counts as sent with the target down: %v", err)
	}
	if n := p.Outbox().Pending(); n != 5 {
		t.Fatalf("%d owed with the target down, want 5", n)
	}
	p.Close()
	tg.down.Store(false)
	p2 := open(t, ctx, filepath.Join(dir, "id"), cs, store, d)
	defer p2.Close()
	if n := p2.Outbox().Pending(); n != 5 {
		t.Fatalf("%d owed after the restart, want 5", n)
	}
	p2.Outbox().now = func() time.Time { return time.Now().Add(time.Hour) }
	p2.Outbox().Retry(ctx)
	if n := p2.Outbox().Pending(); n != 0 {
		t.Fatalf("%d owed after the retry", n)
	}
	piecesThenEntry(t, tg.taken(), before, 4, 2)
	tg.mu.Lock()
	defer tg.mu.Unlock()
	for id, n := range tg.ids {
		// An entry goes to the host and the provider, one server here.
		want := 1
		if id == r.Txid || id == r2.Txid {
			want = 2
		}
		if n != want {
			t.Errorf("carrier %s taken %d time(s), want %d", short(id), n, want)
		}
	}
}
