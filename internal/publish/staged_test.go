package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
	order []string          // topics, in the order taken
	ids   map[string]int    // carrier txid -> times taken
	spent map[string]string // a piece carrier's funding outpoint -> its txid
	stall chan struct{}
	down  atomic.Bool
	srv   *httptest.Server
}

func newOrderTarget(t *testing.T) *orderTarget {
	tg := &orderTarget{ids: map[string]int{}, spent: map[string]string{}, stall: make(chan struct{})}
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
		if topic == storerec.BlobTopic(testBucket) && len(tx.Inputs) == 1 && tx.Inputs[0].SourceTXID != nil {
			out := mintID(tx.Inputs[0].SourceTXID.String(), tx.Inputs[0].SourceTxOutIndex)
			if prev, ok := tg.spent[out]; ok && prev != tx.TxID().String() {
				t.Errorf("funding output %s spent by two carriers", out)
			}
			tg.spent[out] = tx.TxID().String()
		}
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

	// The pieces' mints wait at the gate (their read is observed just
	// before the mint) and the target holds every piece: the put answers
	// anyway, with no piece minted.
	gate := make(chan struct{})
	var mints atomic.Int32
	d.Observe = func(name string, _ float64) {
		switch name {
		case pieceStage("read"):
			<-gate
		case pieceStage("mint"):
			mints.Add(1)
		}
	}
	t0 := time.Now()
	r, err := p.Put(ctx, f, obj, nil)
	if err != nil || r.Pieces != 4 {
		t.Fatalf("put: %v %+v", err, r)
	}
	if n := mints.Load(); n != 0 {
		t.Fatalf("%d piece(s) minted before the answer", n)
	}
	if got := tg.taken(); len(got) != 0 {
		t.Fatalf("the put answered after the target took %v", got)
	}
	select {
	case <-r.Delivered:
		t.Fatal("delivered while the target holds the pieces")
	default:
	}
	// What the put committed: a record to mint each piece on its reserved
	// output (a live batch), the entry (minted) and its log.
	if _, err := p.EntryBEEF(r.Txid); err != nil {
		t.Fatalf("the entry is not logged: %v", err)
	}
	if batches, _ := filepath.Glob(filepath.Join(dir, "id", "outbox", "*.batch")); len(batches) != 1 {
		t.Fatalf("%d batch files while the put is sending", len(batches))
	}
	first, _ := outboxLines(t, filepath.Join(dir, "id", "outbox"))
	pieces, entries := 0, 0
	for _, it := range first {
		switch it.What {
		case whatMint:
			pieces++
		case "entry":
			entries++
		default:
			t.Fatalf("the put committed a %q: %+v", it.What, it)
		}
	}
	if pieces != 4 || entries != 1 {
		t.Fatalf("the put committed %d pieces to mint and %d entries, want 4 and 1", pieces, entries)
	}
	close(gate)
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

// outboxLines reads the outbox's batch files and own files: the first
// line written for each ID (what a put committed) and the last (the item
// now), lines marked done left out.
func outboxLines(t *testing.T, dir string) (first, last map[string]*outItem) {
	t.Helper()
	first, last = map[string]*outItem{}, map[string]*outItem{}
	batches, _ := filepath.Glob(filepath.Join(dir, "*.batch"))
	for _, path := range batches {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range bytes.Split(raw, []byte("\n")) {
			if len(bytes.TrimSpace(l)) == 0 {
				continue
			}
			var it outItem
			if err := json.Unmarshal(l, &it); err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if it.Done != "" {
				delete(first, it.Done)
				delete(last, it.Done)
				continue
			}
			if _, ok := first[it.ID]; !ok {
				first[it.ID] = &it
			}
			last[it.ID] = &it
		}
	}
	own, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, path := range own {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var it outItem
		if err := json.Unmarshal(raw, &it); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if _, ok := first[it.ID]; !ok {
			first[it.ID] = &it
		}
		last[it.ID] = &it
	}
	return first, last
}

// stagedRig is a funded publisher over an order target with 9 MiB staged
// (three blobs and the root: four pieces).
type stagedRig struct {
	ctx   context.Context
	cs    *httptest.Server
	dir   string
	store *Store
	tg    *orderTarget
	d     *Delivery
	p     *Publisher
	st    *Staged
	obj   Object
}

func newStagedRig(t *testing.T) *stagedRig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cs := httptest.NewServer(testchain.New(700))
	t.Cleanup(cs.Close)
	dir := t.TempDir()
	if _, err := Init(filepath.Join(dir, "id")); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	tg := newOrderTarget(t)
	close(tg.stall)
	d := &Delivery{Hosts: []string{tg.srv.URL}, Providers: []string{tg.srv.URL}}
	p := open(t, ctx, filepath.Join(dir, "id"), cs, store, d)
	w := p.Wallet()
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("bstore mint later test, "), 9<<20/24+1)[:9<<20]
	st, err := store.Stage(ctx, bytes.NewReader(content), uint64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Release)
	obj := Object{Root: st.Root, Size: st.Size, ContentHash: st.ContentHash, ProofRoot: st.ProofRoot, Closure: StoreClosure{Store: store, Manifest: st.Manifest, Staged: true}}
	return &stagedRig{ctx: ctx, cs: cs, dir: dir, store: store, tg: tg, d: d, p: p, st: st, obj: obj}
}

// commit is a put up to its answer and no further (as a crash right after
// the answer leaves it): the pieces' records and the entry in the outbox,
// synced, the batch closed, nothing sent.
func (r *stagedRig) commit(t *testing.T, key string) (*pieceBatch, *Receipt) {
	t.Helper()
	e := &storerec.Entry{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Writer: r.p.Identity(), Key: key, Kind: storerec.KindPut,
		Created: uint64(time.Now().UnixMilli()), Size: r.obj.Size, Root: r.obj.Root, ContentHash: r.obj.ContentHash,
		ProofRoot: r.obj.ProofRoot, ProofProfile: 1, ContentType: "x/y"}
	rec, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	pb, err := r.p.mintPieces(r.ctx, testBucket, r.obj.Root, r.obj.Closure)
	if err != nil {
		t.Fatal(err)
	}
	rcpt, _, _, err := r.p.mintEntry(r.ctx, e, rec, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range pb.items {
		if it.What != whatMint || it.Tree == "" {
			t.Fatalf("committed a %q, not a piece to mint later: %+v", it.What, it)
		}
	}
	return pb, rcpt
}

// reopen closes the publisher and opens it again.
func (r *stagedRig) reopen(t *testing.T) {
	t.Helper()
	r.p.Close()
	r.p = open(t, r.ctx, filepath.Join(r.dir, "id"), r.cs, r.store, r.d)
}

// After a restart, the pieces a put committed and did not mint yet are
// minted by the outbox's retry, each once, on the output its record
// reserved (never a fresh one), and sent before the entry; so are the
// pieces it minted and did not send, as the bytes minted.
func TestMintLaterReplays(t *testing.T) {
	r := newStagedRig(t)
	defer func() { r.p.Close() }()
	tg := r.tg

	// Nothing minted.
	pb, rcpt := r.commit(t, "none")
	pb.b.close()
	outs := map[string]bool{}
	for _, it := range pb.items {
		outs[it.ID] = true
	}
	r.reopen(t)
	if n := r.p.Outbox().Pending(); n != 5 {
		t.Fatalf("%d owed after the restart, want 5", n)
	}
	r.p.Outbox().Retry(r.ctx)
	if n := r.p.Outbox().Pending(); n != 0 {
		t.Fatalf("%d owed after the retry", n)
	}
	piecesThenEntry(t, tg.taken(), 0, 4, 2)
	tg.mu.Lock()
	for out := range outs {
		if _, ok := tg.spent[out]; !ok {
			t.Errorf("reserved output %s was not the one a piece spent: %v", out, tg.spent)
		}
	}
	if len(tg.spent) != 4 || tg.ids[rcpt.Txid] != 2 {
		t.Errorf("%d outputs spent, the entry taken %d times", len(tg.spent), tg.ids[rcpt.Txid])
	}
	tg.mu.Unlock()

	// Two of four minted before the crash, none sent.
	before := len(tg.taken())
	pb, rcpt = r.commit(t, "some")
	var minted []string
	for _, i := range []int{0, 2} {
		beef, err := r.p.mintLater(r.ctx, pb.items[i], pb.c)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := transaction.NewTransactionFromBEEF(beef)
		if err != nil {
			t.Fatal(err)
		}
		if id := pb.items[i].Txid; id != tx.TxID().String() || pb.items[i].What != "piece" {
			t.Fatalf("minted item: %+v", pb.items[i])
		}
		minted = append(minted, pb.items[i].Txid)
	}
	pb.b.close()
	_, last := outboxLines(t, filepath.Join(r.dir, "id", "outbox"))
	mints, pieces := 0, 0
	for _, it := range last {
		switch it.What {
		case whatMint:
			mints++
		case "piece":
			pieces++
		}
	}
	if mints != 2 || pieces != 2 {
		t.Fatalf("%d records to mint and %d pieces minted in the outbox, want 2 and 2", mints, pieces)
	}
	r.reopen(t)
	if n := r.p.Outbox().Pending(); n != 5 {
		t.Fatalf("%d owed after the restart, want 5", n)
	}
	r.p.Outbox().Retry(r.ctx)
	if n := r.p.Outbox().Pending(); n != 0 {
		t.Fatalf("%d owed after the retry", n)
	}
	piecesThenEntry(t, tg.taken(), before, 4, 2)
	tg.mu.Lock()
	defer tg.mu.Unlock()
	for _, id := range minted {
		if tg.ids[id] != 1 {
			t.Errorf("carrier %s minted before the crash taken %d time(s)", short(id), tg.ids[id])
		}
	}
	for _, it := range pb.items {
		if _, ok := tg.spent[it.ID]; !ok {
			t.Errorf("reserved output %s was not the one a piece spent", it.ID)
		}
	}
	if len(tg.spent) != 8 || tg.ids[rcpt.Txid] != 2 {
		t.Errorf("%d outputs spent in all, the entry taken %d times", len(tg.spent), tg.ids[rcpt.Txid])
	}
}

// A piece that cannot be minted later (its tree is not one the identity
// keeps) stays owed: logged, counted, tried again after a backoff, and
// given up after a week like anything else owed.
func TestMintLaterFailureStaysOwed(t *testing.T) {
	r := newStagedRig(t)
	var mu sync.Mutex
	var logs []string
	r.p.cfg.Logf = func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	r.p.outbox.logf = r.p.cfg.Logf
	count := func(sub string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, l := range logs {
			if strings.Contains(l, sub) {
				n++
			}
		}
		return n
	}
	defer func() { r.p.Close() }()
	o := r.p.Outbox()
	b, err := o.newBatch()
	if err != nil {
		t.Fatal(err)
	}
	gone := strings.Repeat("ab", 32)
	blob, err := parseHash(r.st.Manifest.Blobs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.addMint(b, testBucket, r.obj.Root, [][32]byte{blob}, gone, 3, r.d.pieceTargets(testBucket)); err != nil {
		t.Fatal(err)
	}
	b.close()
	o.done(mintID(gone, 3))
	if n := o.Pending(); n != 1 {
		t.Fatalf("%d owed, want 1", n)
	}
	o.Retry(r.ctx)
	if n := count("not minted"); n != 1 {
		t.Fatalf("logged %d times, want once: %v", n, logs)
	}
	if n := o.Pending(); n != 1 {
		t.Fatalf("%d owed after the failed mint, want 1", n)
	}
	if total, _ := o.Owed(); total < storerec.BlobSize {
		t.Fatalf("%d bytes owed after the failed mint", total)
	}
	// Not due again yet; due to RetryAll.
	o.Retry(r.ctx)
	if n := count("not minted"); n != 1 {
		t.Fatalf("tried again before its backoff: %d", n)
	}
	o.RetryAll(r.ctx)
	if n := count("not minted"); n != 2 {
		t.Fatalf("not tried again by RetryAll: %d", n)
	}
	if len(r.tg.taken()) != 0 {
		t.Fatal("something was sent")
	}
	o.now = func() time.Time { return time.Now().Add(8 * 24 * time.Hour) }
	o.Retry(r.ctx)
	if n := o.Pending(); n != 0 || count("given up") != 1 {
		t.Fatalf("%d owed after a week, given up logged %d times", n, count("given up"))
	}
}
