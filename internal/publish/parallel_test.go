package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	bpub "github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/pushdrop"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

// A front with no challenge route answers the draw: the draw counts at once
// (row C23), the front is not asked again, and the next draw goes to the
// next front in the same step.
func TestNoRouteSpendsTheDrawAndMovesOn(t *testing.T) {
	r := newRaceRig(t)
	content := r.stageContent(64 << 10)
	race := r.put("k", Bounty{Satoshis: 4000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	honest := newFakeFront(t, content)
	gone := newFakeFront(t, content)
	gone.noRoute = true
	c := candAt(race, honest)
	blobs, _ := r.p.closureBlobs(r.ctx, race.entry)
	tip, _ := r.p.bestHeader(r.ctx)
	ch := &Challenge{Root: race.entry.Root, ProofRoot: race.entry.ProofRoot, Size: race.entry.Size, Provider: honest.identity(),
		Answerer: gone.identity(), C: 30}
	_, raw, err := r.p.NewRequest(r.ctx, ch)
	if err != nil {
		t.Fatal(err)
	}
	c.Pending = &PendingDraw{Request: hex.EncodeToString(raw), Front: gone.srv.URL + "/store/bstore/v1", Answerer: hex.EncodeToString(gone.identity())}
	challengeAlone(r.ctx, r.p, race, c, blobs, tip.Height)
	if gone.draws() != 1 || c.Draws != 2 || c.Status != CandPassed || !slices.Contains(c.BadFronts, gone.srv.URL+"/store/bstore/v1") {
		t.Fatalf("404 front asked %d times; %d draws spent; %s; bad %v; %+v", gone.draws(), c.Draws, c.Status, c.BadFronts, c.Attempts)
	}
	// The advertiser's own derived front answering 404 ERR_ROUTE_NOT_FOUND
	// is not a draw (row C31): asked once, nothing spent, not asked again,
	// and the reason kept in the draw log.
	dead := newFakeFront(t, content)
	dead.noRoute = true
	c2 := candAt(race, dead)
	challengeAlone(r.ctx, r.p, race, c2, blobs, tip.Height)
	if dead.draws() != 1 || c2.Draws != 0 || c2.Status == CandPassed || c2.Status == CandFailed ||
		len(c2.DrawLog) == 0 || c2.DrawLog[len(c2.DrawLog)-1].Outcome != OutcomeNotADraw {
		t.Fatalf("own 404 route-not-found front: asked %d, %d draws, %s, log %+v", dead.draws(), c2.Draws, c2.Status, c2.DrawLog)
	}
	// The draw log holds only the not-a-draw line, never a no-answer line
	// before it.
	for _, a := range c2.DrawLog {
		if a.Outcome != OutcomeNotADraw {
			t.Fatalf("C31 draw log has %q: %+v", a.Outcome, c2.DrawLog)
		}
	}
	// Any other 404 from the derived front spends the draw (C23).
	other := newFakeFront(t, content)
	other.notFound = true
	c3 := candAt(race, other)
	challengeAlone(r.ctx, r.p, race, c3, blobs, tip.Height)
	if other.draws() != 1 || c3.Draws != 1 || c3.Status == CandPassed {
		t.Fatalf("own 404 front: asked %d, %d draws, %s", other.draws(), c3.Draws, c3.Status)
	}
}

// Progress notes never push the reason a draw was spent out of the draw
// log: it is kept apart from the attempt notes.
func TestDrawLogKeepsTheReason(t *testing.T) {
	r := newRaceRig(t)
	content := r.stageContent(64 << 10)
	race := r.put("k", Bounty{Satoshis: 4000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	other := newFakeFront(t, content)
	other.notFound = true
	c := candAt(race, other)
	blobs, _ := r.p.closureBlobs(r.ctx, race.entry)
	tip, _ := r.p.bestHeader(r.ctx)
	challengeAlone(r.ctx, r.p, race, c, blobs, tip.Height)
	// The front is now bad and no other states it answers: every later step
	// notes "waiting".
	for range 100 {
		challengeAlone(r.ctx, r.p, race, c, blobs, tip.Height)
	}
	if len(c.Attempts) != maxNotes {
		t.Fatalf("%d attempt notes kept", len(c.Attempts))
	}
	for _, a := range c.Attempts {
		if a.Outcome != "waiting" {
			t.Fatalf("the spent draw is still among the notes: %+v", c.Attempts)
		}
	}
	if len(c.DrawLog) == 0 || c.DrawLog[len(c.DrawLog)-1].Outcome != "spent" || !strings.Contains(c.DrawLog[len(c.DrawLog)-1].Why, "404") {
		t.Fatalf("draw log %+v", c.DrawLog)
	}
}

// Candidates are challenged a wave at once: as many of the best ranked as
// passes are missing; a lower one is not challenged while K better ones
// can still pass.
func TestJudgeChallengesAWaveAtOnce(t *testing.T) {
	r := newRaceRig(t)
	content := r.stageContent(64 << 10)
	race := r.put("k", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	var fronts []*fakeFront
	for i := 0; i < 3; i++ {
		f := newFakeFront(t, content)
		f.delay = 1500 * time.Millisecond
		c := candAt(race, f)
		c.FirstSeen = int64(i + 1)
		fronts = append(fronts, f)
	}
	tip, _ := r.p.bestHeader(r.ctx)
	start := time.Now()
	if err := r.p.judge(r.ctx, race, tip.Height, RaceOptions{}); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	if len(race.Passed()) != 2 || fronts[0].draws() != 1 || fronts[1].draws() != 1 || fronts[2].draws() != 0 {
		t.Fatalf("passed %d; draws %d %d %d", len(race.Passed()), fronts[0].draws(), fronts[1].draws(), fronts[2].draws())
	}
	if took > 2900*time.Millisecond {
		t.Fatalf("two candidates of 1.5 s each took %v: not at once", took)
	}
	saved, err := r.p.Race(race.Entry)
	if err != nil || len(saved.Passed()) != 2 {
		t.Fatalf("the race saved: %v", err)
	}
}

// A refusal's not-taken note for this call's payment releases the coin at
// once; a note for another payment, or one that does not verify, leaves it
// held.
func TestNotTakenNoteReleasesTheCoin(t *testing.T) {
	r := newRaceRig(t)
	pay := r.p.NewPayer(r.ctx)
	in, err := pay.TakeAtLeast(r.ctx, 2000)
	if err != nil {
		t.Fatal(err)
	}
	coin := coinOf(in)
	dest, _ := r.p.signer.FundScript()
	tx, err := mint.Payment(r.ctx, dest, 1000, in, dest, mint.DefaultFees)
	if err != nil {
		t.Fatal(err)
	}
	pay.Change(tx, 0, nil)
	r.p.notePayment(tx, false, false)
	key, _ := ec.NewPrivateKey()
	answerer := key.PubKey().Compressed()
	request := []byte("the request")
	note := func(k *ec.PrivateKey, payment [32]byte) []byte {
		n := &storerec.NotTaken{Payment: payment, Request: sha256.Sum256(request), Reason: "busy", Time: uint64(time.Now().Unix()), Answerer: answerer}
		body, err := n.Encode()
		if err != nil {
			t.Fatal(err)
		}
		ck, _ := wallet.NewKeyDeriver(k).DerivePrivateKey(storerec.Protocol, storerec.KeyChallenge, pushdrop.Anyone())
		signed, err := storerec.SignNotTaken(ck, body)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	call := func(signed []byte) *Call {
		b, _ := json.Marshal(map[string]string{"error": "busy", "notTaken": base64.StdEncoding.EncodeToString(signed)})
		return &Call{Status: 503, Body: b, Payment: tx, Peer: hex.EncodeToString(answerer)}
	}
	txid := tx.TxID()
	other, _ := ec.NewPrivateKey()
	if r.p.notTaken(call(note(other, [32]byte(txid[:]))), request, answerer) != nil || r.p.notTaken(call(note(key, sha256.Sum256(nil))), request, answerer) != nil {
		t.Fatal("a note that does not hold released the payment")
	}
	if len(r.p.st.Payments) != 1 {
		t.Fatal("the payment record went")
	}
	// A good note over a session authenticated as someone else is ignored.
	wrong := call(note(key, [32]byte(txid[:])))
	wrong.Peer = hex.EncodeToString(other.PubKey().Compressed())
	if r.p.notTaken(wrong, request, answerer) != nil || len(r.p.st.Payments) != 1 {
		t.Fatal("a note from another peer released the payment")
	}
	if n := r.p.notTaken(call(note(key, [32]byte(txid[:]))), request, answerer); n == nil || n.Reason != "busy" {
		t.Fatal("a good note was not taken")
	}
	if len(r.p.st.Payments) != 0 {
		t.Fatal("the payment is still held")
	}
	back, changed := false, false
	for _, o := range r.p.wallet.Pool.Outputs() {
		back = back || o.Outpoint() == coin.Outpoint()
		changed = changed || o.TxID == txid.String()
	}
	if !back || changed {
		t.Fatalf("coin back %v, change still held %v", back, changed)
	}
}

// Payment coins: split off the pool into a pool of their own, a call takes
// the smallest alone, and gets it back unspent; a call that paid leaves its
// change in the identity's pool.
func TestPaymentCoins(t *testing.T) {
	r := newRaceRig(t)
	r.p.FillPayCoins(r.ctx, 100)
	// Split, not waited for: unproven until their proof is collected.
	if pool, done, _ := r.p.payPool(r.ctx, 100); pool != r.p.wallet.Pool {
		t.Fatal("an unproven payment coin was taken")
	} else {
		done()
	}
	r.p.CollectChange(r.ctx)
	n, sats := r.p.PayCoins()
	if n != payCoins || sats != uint64(payCoins)*r.p.payCoinValue(100) {
		t.Fatalf("%d payment coins, %d satoshis", n, sats)
	}
	// A tree or a fee never takes one: they are not in the identity's pool.
	for _, o := range r.p.wallet.Pool.Outputs() {
		if o.Satoshis == r.p.payCoinValue(100) {
			t.Fatal("a payment coin is in the identity's pool")
		}
	}
	pool, done, _ := r.p.payPool(r.ctx, 100)
	if pool == r.p.wallet.Pool || pool.Count() != 1 {
		t.Fatal("the call does not pay from one payment coin")
	}
	if n, _ := r.p.PayCoins(); n != payCoins-1 {
		t.Fatalf("%d payment coins while one is out", n)
	}
	done()
	if n, _ := r.p.PayCoins(); n != payCoins {
		t.Fatalf("%d payment coins after the call", n)
	}
	// A call over what a payment coin covers pays from the identity's pool.
	if pool, done, _ := r.p.payPool(r.ctx, 1_000_000); pool != r.p.wallet.Pool {
		t.Fatal("a large payment took a payment coin")
	} else {
		done()
	}
	// A call pool a stop left behind is merged on the next start.
	pool, _, _ = r.p.payPool(r.ctx, 100)
	if pool == r.p.wallet.Pool {
		t.Fatal("no payment coin")
	}
	r.p.mergePayPools()
	if n, _ := r.p.PayCoins(); n != payCoins {
		t.Fatalf("%d payment coins after the merge", n)
	}
}

// Trees ahead: once a put leaves the piece tree low the next is minted in
// the background, recorded with the trees ahead (where a sweep finds it),
// and the next put switches to it without minting one.
func TestPieceTreeMintedAhead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cs := httptest.NewServer(testchain.New(700))
	defer cs.Close()
	dir := t.TempDir()
	if _, err := Init(filepath.Join(dir, "id")); err != nil {
		t.Fatal(err)
	}
	store, _ := OpenStore(filepath.Join(dir, "store"))
	host := newTarget(t)
	var logs []string
	var mu sync.Mutex
	p, err := Open(ctx, Config{Dir: filepath.Join(dir, "id"), Chain: nodeChain(cs.URL), Tip: nodeTip(cs.URL), Settler: &bpub.RPCSettler{RPC: &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}},
		Fees: mint.DefaultFees, PieceTree: 8, EntryTree: 4, Store: store, Delivery: &Delivery{Hosts: []string{host.srv.URL}, Providers: []string{host.srv.URL}},
		Logf: func(f string, a ...any) {
			mu.Lock()
			logs = append(logs, f)
			mu.Unlock()
			t.Logf(f, a...)
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	w := p.Wallet()
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		t.Fatal(err)
	}
	put := func(key string) {
		content := make([]byte, 4*storerec.BlobSize) // 5 pieces
		_, _ = rand.Read(content)
		st, err := store.Stage(ctx, bytes.NewReader(content), 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Release()
		if _, err := p.Put(ctx, Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: key, ContentType: "x/y"},
			Object{Root: st.Root, Size: st.Size, ContentHash: st.ContentHash, ProofRoot: st.ProofRoot,
				Closure: StoreClosure{Store: store, Manifest: st.Manifest, Staged: true}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	put("a")
	if err := p.pieces.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	held := p.pieces.Held()
	if len(held) != 1 {
		t.Fatalf("%d trees held after the first put", len(held))
	}
	p.keepHeld(kindPiece)
	p.mu.Lock()
	ahead := slices.ContainsFunc(p.st.Piece.Ahead, func(a funding.Tree) bool { return a.Txid == held[0].Txid })
	pending := len(p.st.Pending)
	p.mu.Unlock()
	if !ahead || pending != 0 {
		t.Fatalf("the tree minted ahead is not with the trees ahead (%v), %d prepare records left", ahead, pending)
	}
	mu.Lock()
	logs = nil
	mu.Unlock()
	put("b") // 3 left on the first tree, then 2 from the tree minted ahead
	p.mu.Lock()
	cur := p.st.Piece.Tree
	left := slices.ContainsFunc(p.st.Piece.Ahead, func(a funding.Tree) bool { return a.Txid == held[0].Txid })
	trees := len(p.st.Piece.Trees)
	p.mu.Unlock()
	if cur.Txid != held[0].Txid || cur.Next != 2 || left || trees != 2 {
		t.Fatalf("current %s next %d (want the tree minted ahead, 2); still ahead %v; %d trees", short(cur.Txid), cur.Next, left, trees)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, l := range logs {
		if strings.Contains(l, "rather than") {
			t.Fatalf("a tree was minted on demand: %s", l)
		}
	}
}

// crashTarget records every carrier it is sent, and holds the answers to
// pieces once hold is closed... until released.
type crashTarget struct {
	mu      sync.Mutex
	got     map[string][32]byte // carrier txid -> SHA-256 of its BEEF
	ok      map[string]bool     // carriers answered 200
	funding map[string]string   // funding outpoint -> carrier txid
	pieces  int
	stall   chan struct{}
	stalled chan struct{}
	once    sync.Once
	after   int
	srv     *httptest.Server
	t       *testing.T
}

func newCrashTarget(t *testing.T, after int) *crashTarget {
	c := &crashTarget{got: map[string][32]byte{}, ok: map[string]bool{}, funding: map[string]string{}, stall: make(chan struct{}), stalled: make(chan struct{}), after: after, t: t}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		topic := r.Header.Get("x-topics")
		if topic == storerec.BlobTopic(testBucket) {
			pc, err := storerec.CheckPieceCarrier(r.Context(), b, testBucket, nil)
			if err != nil {
				_, _ = w.Write([]byte(`{"` + topic + `":{"refused":"` + err.Error() + `"}}`))
				return
			}
			tx, _ := transaction.NewTransactionFromBEEF(b)
			id := tx.TxID().String()
			c.mu.Lock()
			if prev, ok := c.got[id]; ok && prev != sha256.Sum256(b) {
				c.mu.Unlock()
				t.Errorf("carrier %s sent again with other bytes", id)
				http.Error(w, "other bytes", 500)
				return
			}
			if prev, ok := c.funding[pc.Funding.String()]; ok && prev != id {
				t.Errorf("funding output %s spent by two carriers", pc.Funding.String())
			}
			c.funding[pc.Funding.String()] = id
			first := false
			if _, ok := c.got[id]; !ok {
				c.got[id] = sha256.Sum256(b)
				c.pieces++
				first = true
			}
			n := c.pieces
			c.mu.Unlock()
			released := false
			select {
			case <-c.stall:
				released = true
			default:
			}
			if first && n > c.after && !released {
				c.once.Do(func() { close(c.stalled) })
				select {
				case <-c.stall:
				case <-r.Context().Done():
					return
				}
				http.Error(w, "down", 503)
				return
			}
			c.mu.Lock()
			c.ok[id] = true
			c.mu.Unlock()
		}
		_, _ = w.Write([]byte(`{"` + topic + `":{"outputsToAdmit":[0],"coinsToRetain":[]}}`))
	}))
	t.Cleanup(func() {
		select {
		case <-c.stall:
		default:
			close(c.stall)
		}
		c.srv.Close()
	})
	return c
}

// The child half of TestPutKilledMidwayResumes: a put of eleven pieces,
// killed by the parent while it is sending them.
func TestCrashChildPut(t *testing.T) {
	dir := os.Getenv("BSTORE_CRASH_DIR")
	if dir == "" {
		t.Skip("run by TestPutKilledMidwayResumes")
	}
	ctx := context.Background()
	store, err := OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	chain, target := os.Getenv("BSTORE_CRASH_CHAIN"), os.Getenv("BSTORE_CRASH_TARGET")
	p, err := Open(ctx, Config{Dir: filepath.Join(dir, "id"), Chain: nodeChain(chain), Tip: nodeTip(chain), Settler: &bpub.RPCSettler{RPC: &nodeapi.RPC{URL: chain + "/rpc", ID: "t"}},
		Fees: mint.DefaultFees, PieceTree: 64, EntryTree: 4, Store: store, Delivery: &Delivery{Hosts: []string{target}, Providers: []string{target}}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	content, _ := os.ReadFile(filepath.Join(dir, "content"))
	st, err := store.Stage(ctx, bytes.NewReader(content), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Put(ctx, Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: "crash", ContentType: "x/y"},
		Object{Root: st.Root, Size: st.Size, ContentHash: st.ContentHash, ProofRoot: st.ProofRoot,
			Closure: StoreClosure{Store: store, Manifest: st.Manifest, Staged: true}}, nil)
	t.Fatalf("the put was not killed: %v", err)
}

// Crash safety of the outbox by reference: a process killed while its
// put is sending pieces leaves every carrier it minted in the outbox,
// synced; the next start sends each again, the same bytes, and the next
// put spends fresh funding outputs.
func TestPutKilledMidwayResumes(t *testing.T) {
	if testing.Short() && os.Getenv("BSTORE_CRASH_FULL") == "" {
		t.Log("short mode: the crash test still runs; it takes a few seconds")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cs := httptest.NewServer(testchain.New(700))
	defer cs.Close()
	dir := t.TempDir()
	if _, err := Init(filepath.Join(dir, "id")); err != nil {
		t.Fatal(err)
	}
	w, err := bwallet.Open(WalletDir(filepath.Join(dir, "id")), Profile)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		t.Fatal(err)
	}
	content := make([]byte, 10*storerec.BlobSize) // 11 pieces
	_, _ = rand.Read(content)
	if err := os.WriteFile(filepath.Join(dir, "content"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	target := newCrashTarget(t, 3)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChildPut$", "-test.v")
	cmd.Env = append(os.Environ(), "BSTORE_CRASH_DIR="+dir, "BSTORE_CRASH_CHAIN="+cs.URL, "BSTORE_CRASH_TARGET="+target.srv.URL)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-target.stalled:
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		t.Fatalf("the child never reached the fourth piece:\n%s", out.String())
	}
	time.Sleep(200 * time.Millisecond) // the other workers' carriers are in flight too
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_ = cmd.Wait()
	close(target.stall)

	// What the killed put left: batch lines, synced, each naming its
	// carrier by reference.
	batches, _ := filepath.Glob(filepath.Join(dir, "id", "outbox", "*.batch"))
	if len(batches) != 1 {
		t.Fatalf("%d batch files after the kill\n%s", len(batches), out.String())
	}
	target.mu.Lock()
	before := len(target.got)
	target.mu.Unlock()
	store, _ := OpenStore(filepath.Join(dir, "store"))
	p, err := Open(ctx, Config{Dir: filepath.Join(dir, "id"), Chain: nodeChain(cs.URL), Tip: nodeTip(cs.URL), Settler: &bpub.RPCSettler{RPC: &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}},
		Fees: mint.DefaultFees, PieceTree: 64, EntryTree: 4, Store: store, Delivery: &Delivery{Hosts: []string{target.srv.URL}, Providers: []string{target.srv.URL}}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	owed := p.Outbox().Pending()
	if owed < 1 {
		t.Fatalf("nothing owed after the kill (%d carriers reached the target)", before)
	}
	var owedIDs []string
	for _, it := range p.Outbox().load() {
		if it.Ref == "" || len(it.Head) == 0 {
			t.Fatalf("carrier %s kept whole, not by reference", short(it.ID))
		}
		owedIDs = append(owedIDs, it.ID)
	}
	// Trim cannot take an object the outbox still owes.
	if _, err := store.Trim(0); err != nil {
		t.Fatal(err)
	}
	p.Outbox().RetryAll(ctx)
	if n := p.Outbox().Pending(); n != 0 {
		t.Fatalf("%d left after the retry", n)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "id", "outbox", "*.batch")); len(left) != 0 {
		t.Fatalf("batch files left: %v", left)
	}
	target.mu.Lock()
	for _, id := range owedIDs {
		if !target.ok[id] {
			t.Errorf("carrier %s owed after the kill was not delivered by the retry", short(id))
		}
	}
	target.mu.Unlock()
	// The next put spends fresh outputs: the target refuses a funding
	// output two carriers spend.
	st, err := store.Stage(ctx, bytes.NewReader(content), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	r, err := p.Put(ctx, Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: "after", ContentType: "x/y"},
		Object{Root: st.Root, Size: st.Size, ContentHash: st.ContentHash, ProofRoot: st.ProofRoot, Closure: StoreClosure{Store: store, Manifest: st.Manifest, Staged: true}}, nil)
	if err != nil || r.Pieces != 11 {
		t.Fatalf("the put after: %v", err)
	}
}

// RaceTick, the gateway's background pass: a race with K passed is settled
// by rank; one nobody won is reclaimed once its deadline comes; each saved.
func TestRaceTickSettlesAndReclaims(t *testing.T) {
	r := newRaceRig(t)
	won := r.put("won", Bounty{Satoshis: 10_000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	lost := r.put("lost", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 20, CheckC: 30})
	a, b := r.provider("a"), r.provider("b")
	passed(won, a, r.advertise(a, 2_000_000_000, nil), 1)
	passed(won, b, r.advertise(b, 2_000_000_000, nil), 2)
	if err := r.p.saveRace(won); err != nil {
		t.Fatal(err)
	}
	o := RaceOptions{Window: time.Hour}
	// A pass sends the settlement and does not wait for its block: the
	// next pass, once a block holds it, sees it mined.
	r.p.RaceTick(r.ctx, o)
	got, _ := r.p.Race(won.Entry)
	if got.State != RaceSettling || got.Settlement == nil {
		t.Fatalf("the won race after one pass: %s %s", got.State, got.Note)
	}
	r.blocks(1)
	r.p.RaceTick(r.ctx, o)
	if got, _ = r.p.Race(won.Entry); got.State != RaceSettled || got.Settlement == nil {
		t.Fatalf("the won race after its block: %s %s", got.State, got.Note)
	}
	if l, _ := r.p.Race(lost.Entry); l.State != RaceOpen {
		t.Fatalf("the race nobody won before its deadline: %s", l.State)
	}
	r.blocks(25)
	r.p.RaceTick(r.ctx, o)
	if l, _ := r.p.Race(lost.Entry); l.State != RaceReclaim || l.Reclaim == nil {
		t.Fatalf("the race nobody won after its deadline, after one pass: %s %s", l.State, l.Note)
	}
	r.blocks(1)
	r.p.RaceTick(r.ctx, o)
	if l, _ := r.p.Race(lost.Entry); l.State != RaceReclaimd || l.Reclaim == nil {
		t.Fatalf("the race nobody won after its deadline: %s %s", l.State, l.Note)
	}
	if g, _ := r.p.Race(won.Entry); g.State != RaceSettled || g.Reclaim != nil {
		t.Fatal("a settled race was touched again")
	}
}

// Review fixes: the answer names the peer its session authenticated; a
// reconcile keeps records noted while it ran; a split cut short keeps its
// coins with the payment coins; an object found on disk is read verified;
// a coin taken for a call a stop cut short comes back.
func TestReviewFixes(t *testing.T) {
	r := newRaceRig(t)
	content := r.stageContent(64 << 10)
	f := newFakeFront(t, content)
	call, err := r.p.Post(r.ctx, f.srv.URL+"/store/bstore/v1/challenge", []byte(`{"request":""}`), 0)
	if err != nil {
		t.Fatal(err)
	}
	if call.Peer != hex.EncodeToString(f.identity()) {
		t.Fatalf("peer %q, want %x", call.Peer, f.identity())
	}

	// Reconcile against notes appended meanwhile.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		tx := transaction.NewTransaction()
		tx.LockTime = uint32(i)
		src := transaction.NewTransaction()
		src.LockTime = uint32(1000 + i)
		src.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: must(r.p.signer.FundScript())})
		tx.AddInputFromTx(src, 0, nil)
		wg.Add(2)
		go func() { defer wg.Done(); r.p.notePayment(tx, false, false) }()
		go func() { defer wg.Done(); r.p.ReconcilePayments(r.ctx) }()
	}
	wg.Wait()
	if n := len(r.p.st.Payments); n != 20 {
		t.Fatalf("%d of 20 payment records kept", n)
	}

	// A payment coin also in the identity's pool stays a payment coin.
	r.p.FillPayCoins(r.ctx, 100)
	r.p.CollectChange(r.ctx)
	coins, _ := r.p.payCoinPool()
	c := coins.Outputs()[0]
	_, _ = r.p.wallet.Pool.Add(c)
	r.p.mergePayPools()
	for _, o := range r.p.wallet.Pool.Outputs() {
		if o.Outpoint() == c.Outpoint() {
			t.Fatal("a payment coin left in the identity's pool")
		}
	}

	// An intent left by a stop: the coin comes back an hour on.
	took, _ := coins.Remove(c)
	if !took {
		t.Fatal("no coin to take")
	}
	r.p.noteIntent(c)
	r.p.reconcileIntents(r.ctx)
	if len(r.p.st.Intents) != 1 {
		t.Fatal("an intent settled before the hour")
	}
	r.p.cfg.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	r.p.reconcileIntents(r.ctx)
	r.p.cfg.Now = time.Now
	back := slices.ContainsFunc(coins.Outputs(), func(o bwallet.Output) bool { return o.Outpoint() == c.Outpoint() })
	if len(r.p.st.Intents) != 0 || !back {
		t.Fatalf("intent left %d, coin back %v", len(r.p.st.Intents), back)
	}

	// An object a stage found on disk, not written, is read verified.
	b := []byte("an object already held")
	h, _ := r.store.Put(b)
	if err := os.WriteFile(r.store.objectPath(h), []byte("rotted bytes on disk!!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, wrote, _ := r.store.put(b, true); wrote {
		t.Fatal("an object held was written again")
	}
	if _, err := r.store.readStaged(h); err == nil {
		t.Fatal("a rotted object found by a stage was read unverified")
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
