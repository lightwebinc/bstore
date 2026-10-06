package publish

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	bpub "github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

// switchSettler forwards to the chain's RPC, or drops what it is sent
// (answering as if it took it) while drop is set.
type switchSettler struct {
	inner bpub.Settler
	drop  atomic.Bool
	sent  atomic.Int64
}

func (s *switchSettler) Submit(ctx context.Context, tx *transaction.Transaction) error {
	s.sent.Add(1)
	if s.drop.Load() {
		return nil
	}
	return s.inner.Submit(ctx, tx)
}

func (s *switchSettler) Name() string { return "switch" }

type raceRig struct {
	t      *testing.T
	ctx    context.Context
	chain  *testchain.Chain
	cs     *httptest.Server
	rpc    *nodeapi.RPC
	p      *Publisher
	leg    *switchSettler
	store  *Store
	dir    string
	staged *Staged
}

func newRaceRig(t *testing.T) *raceRig {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	r := &raceRig{t: t, ctx: ctx, chain: testchain.New(700), dir: t.TempDir()}
	r.cs = httptest.NewServer(r.chain)
	t.Cleanup(r.cs.Close)
	r.rpc = &nodeapi.RPC{URL: r.cs.URL + "/rpc", ID: "t"}
	id := filepath.Join(r.dir, "id")
	if _, err := Init(id); err != nil {
		t.Fatal(err)
	}
	w, err := bwallet.Open(WalletDir(id), Profile)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, r.rpc, &nodeapi.Asset{Base: r.cs.URL}, 104, 50); err != nil {
		t.Fatal(err)
	}
	if r.store, err = OpenStore(filepath.Join(r.dir, "store")); err != nil {
		t.Fatal(err)
	}
	host := newTarget(t)
	r.leg = &switchSettler{inner: &bpub.RPCSettler{RPC: r.rpc}}
	r.p, err = Open(ctx, Config{Dir: id, Asset: &nodeapi.Asset{Base: r.cs.URL}, Settler: r.leg, Fees: mint.DefaultFees, PieceTree: 8, EntryTree: 4,
		HoldAhead: 2, Store: r.store, Delivery: &Delivery{Hosts: []string{host.srv.URL}, Providers: []string{host.srv.URL}}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.p.Close)
	if r.staged, err = r.store.Stage(ctx, strings.NewReader("an object with a bounty"), 1<<20); err != nil {
		t.Fatal(err)
	}
	return r
}

// put publishes the staged object under key with a bounty.
func (r *raceRig) put(key string, b Bounty) *Race {
	r.t.Helper()
	rc, err := r.p.Put(r.ctx, Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: key, ContentType: "text/plain"},
		Object{Root: r.staged.Root, Size: r.staged.Size, ContentHash: r.staged.ContentHash, ProofRoot: r.staged.ProofRoot,
			Closure: StoreClosure{Store: r.store, Manifest: r.staged.Manifest}}, &b)
	if err != nil {
		r.t.Fatal(err)
	}
	race, err := r.p.Race(rc.Txid)
	if err != nil {
		r.t.Fatal(err)
	}
	return race
}

// provider is an advertiser identity.
type provider struct {
	w *bwallet.Embedded
}

func (r *raceRig) provider(name string) *provider {
	w, err := bwallet.Create(filepath.Join(r.dir, "p-"+name), Profile)
	if err != nil {
		r.t.Fatal(err)
	}
	w.Originator = "test"
	return &provider{w: w}
}

func (pv *provider) key() []byte { return pv.w.Signer().Identity.Compressed() }

// advertise mints pv's advertisement of the staged root (spending prev, a
// renewal, when given), paid from the writer's pool.
func (r *raceRig) advertise(pv *provider, expiry uint64, prev *transaction.Transaction) *transaction.Transaction {
	r.t.Helper()
	rb, _ := r.store.Get(r.staged.Root)
	url := "https://provider.example/store" + storerec.CHIRPURLSuffix(r.staged.Root)
	lock, err := storerec.AdDerivation.Lock(r.ctx, pv.w, pv.w.Originator, [][]byte{pv.key(), r.staged.Root[:], []byte(url),
		storerec.CompactSize(expiry), storerec.CompactSize(uint64(len(rb)))}, true)
	if err != nil {
		r.t.Fatal(err)
	}
	var in *mint.Input
	if prev != nil {
		in = &mint.Input{Tx: prev, Vout: 0, Unlocker: storerec.AdDerivation.Unlocker(r.ctx, pv.w, pv.w.Originator)}
	}
	return r.mine(lock, in)
}

// cancel spends pv's advertisement into something that is not one.
func (r *raceRig) cancel(pv *provider, ad *transaction.Transaction) {
	r.t.Helper()
	change, _ := r.p.signer.FundScript()
	r.mine(change, &mint.Input{Tx: ad, Vout: 0, Unlocker: storerec.AdDerivation.Unlocker(r.ctx, pv.w, pv.w.Originator)})
}

func (r *raceRig) mine(lock *script.Script, in *mint.Input) *transaction.Transaction {
	r.t.Helper()
	pay := r.p.NewPayer(r.ctx)
	fee, err := pay.TakeAtLeast(r.ctx, 1000)
	if err != nil {
		r.t.Fatal(err)
	}
	change, _ := r.p.signer.FundScript()
	tx, err := mint.Transition(lock, 1, in, fee, change, mint.DefaultFees)
	if err != nil {
		r.t.Fatal(err)
	}
	mp, h, err := pay.SettleAndWait(r.ctx, "test", tx)
	if err != nil {
		r.t.Fatal(err)
	}
	tx.MerklePath = mp
	pay.Change(tx, h, mp)
	return tx
}

func (r *raceRig) blocks(n int) {
	r.t.Helper()
	addr, _ := r.p.signer.FundAddress(false)
	if _, err := r.chain.Generate(n, addr); err != nil {
		r.t.Fatal(err)
	}
}

// passed adds a candidate that passed, seen at seen.
func passed(r *Race, pv *provider, ad *transaction.Transaction, seen int64) {
	a, _ := storerec.DecodeAdvertisement(*ad.Outputs[0].LockingScript)
	r.Cands = append(r.Cands, &Cand{Advertiser: hex.EncodeToString(pv.key()), First: ad.TxID().String(), FirstVout: 0, FirstExpiry: a.Expiry, FirstSeen: seen,
		URL: "https://provider.example/store" + storerec.CHIRPURLSuffix(r.entry.Root), Expiry: 2_000_000_000, Status: CandPassed})
}

// Settlement by rank: the first seen passed advertiser whose advertisement
// was then cancelled is dropped and the ranks below move up; a renewed one
// counts as live; the payouts are the square-root split of exactly the
// hold output, each to the BRC-29 key of its advertiser for the entry and
// rank; and the hold output settles once, whatever is asked after.
func TestSettleByRankOnce(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 10_000, T: 2, K: 3, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a, b, c := r.provider("a"), r.provider("b"), r.provider("c")
	adA := r.advertise(a, 2_000_000_000, nil)
	adB := r.advertise(b, 1_900_000_000, nil)
	adC := r.advertise(c, 2_000_000_000, nil)
	r.advertise(b, 2_000_000_000, adB) // b renews
	r.cancel(c, adC)                   // c cancels
	passed(race, c, adC, 1)
	passed(race, a, adA, 2)
	passed(race, b, adB, 3)
	rec, err := r.p.Settle(r.ctx, race, false)
	if !errors.Is(err, ErrNotYet) || rec == nil || race.State != RaceSettled {
		// Mined; the remittances have no front to go to here.
		t.Fatalf("settle: %v %+v %s", err, rec, race.State)
	}
	if len(rec.Dropped) != 1 || rec.Dropped[0] != hex.EncodeToString(c.key()) {
		t.Fatalf("dropped %v", rec.Dropped)
	}
	pay := storerec.Payouts(10_000, 2)
	raw, _ := hex.DecodeString(rec.Raw)
	tx, _ := transaction.NewTransactionFromBytes(raw)
	prefix := storerec.DerivationPrefix(race.C())
	var sum uint64
	for i, pv := range []*provider{a, b} {
		py := rec.Payees[i]
		if py.Rank != i+1 || py.Advertiser != hex.EncodeToString(pv.key()) || py.Satoshis != pay[i] {
			t.Fatalf("payee %d: %+v", i, py)
		}
		want, err := r.p.signer.PaymentDestination(r.ctx, py.Advertiser, prefix, storerec.DerivationSuffix(py.Rank))
		if err != nil {
			t.Fatal(err)
		}
		if tx.Outputs[py.Vout].LockingScript.String() != want.String() || tx.Outputs[py.Vout].Satoshis != pay[i] {
			t.Fatalf("output %d does not pay rank %d's BRC-29 key", py.Vout, py.Rank)
		}
		sum += tx.Outputs[py.Vout].Satoshis
	}
	if sum != 10_000 {
		t.Fatalf("payouts sum to %d", sum)
	}
	if by, _ := r.p.cfg.Asset.Spender(r.ctx, strings.Split(race.Hold, ".")[0], 0); by != rec.Txid {
		t.Fatalf("the hold output is spent by %s", by)
	}
	// Settle again: the same settlement, nothing new sent.
	sent := r.leg.sent.Load()
	rec2, _ := r.p.Settle(r.ctx, race, true)
	if rec2 == nil || rec2.Txid != rec.Txid || r.leg.sent.Load() != sent {
		t.Fatal("a second settlement")
	}
	// A settled hold output is never reclaimed.
	r.blocks(600)
	if _, err := r.p.Reclaim(r.ctx, race); err == nil {
		t.Fatal("a settled hold output was reclaimed")
	}
}

// A settlement stopped before the network took it is sent again, the same
// bytes, by the next settle; a reclaim meanwhile refuses.
func TestSettleResumesTheRecordedBytes(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 5000, T: 1, K: 1, Retention: 3600, DeadlineBlocks: 500, CheckC: 30})
	a := r.provider("a")
	adA := r.advertise(a, 2_000_000_000, nil)
	passed(race, a, adA, 1)
	r.leg.drop.Store(true)
	short, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	rec, err := r.p.Settle(short, race, false)
	cancel()
	if !errors.Is(err, ErrNotYet) || rec == nil || race.State != RaceSettling {
		t.Fatalf("a settlement the leg dropped: %v %s", err, race.State)
	}
	// Reloaded, as a new process would.
	race, _ = r.p.Race(race.Entry)
	r.blocks(600)
	if _, err := r.p.Reclaim(r.ctx, race); err == nil {
		t.Fatal("a hold output with a recorded settlement was reclaimed")
	}
	r.leg.drop.Store(false)
	rec2, _ := r.p.Settle(r.ctx, race, false)
	if rec2 == nil || rec2.Txid != rec.Txid || race.State != RaceSettled {
		t.Fatalf("resumed: %+v %s", rec2, race.State)
	}
}

// A race nobody won: settle before the deadline pays nothing and closes;
// reclaim waits for the deadline; after it settle refuses and reclaim
// spends the hold output back, once.
func TestReclaimAfterTheDeadline(t *testing.T) {
	r := newRaceRig(t)
	race := r.put("k", Bounty{Satoshis: 4000, T: 2, K: 2, Retention: 3600, DeadlineBlocks: 20, CheckC: 30})
	if _, err := r.p.Reclaim(r.ctx, race); !errors.Is(err, ErrBeforeDeadline) {
		t.Fatalf("a reclaim before the deadline: %v", err)
	}
	if rec, err := r.p.Settle(r.ctx, race, true); err != nil || rec != nil || race.State != RaceClosed {
		t.Fatalf("settle with nobody passed: %v %+v %s", err, rec, race.State)
	}
	r.blocks(25)
	if _, err := r.p.Settle(r.ctx, race, true); err != nil && !errors.Is(err, ErrPastDeadline) {
		t.Fatalf("settle after the deadline: %v", err)
	}
	rc, err := r.p.Reclaim(r.ctx, race)
	if err != nil || rc == nil || race.State != RaceReclaimd {
		t.Fatalf("reclaim: %v %s", err, race.State)
	}
	if by, _ := r.p.cfg.Asset.Spender(r.ctx, strings.Split(race.Hold, ".")[0], 0); by != rc.Txid {
		t.Fatal("the hold output is not spent by the reclaim")
	}
	rc2, err := r.p.Reclaim(r.ctx, race)
	if err != nil || rc2.Txid != rc.Txid {
		t.Fatalf("a second reclaim: %v", err)
	}
	// Hold outputs minted ahead and never taken come back too.
	done, err := r.p.ReclaimUnused(r.ctx)
	if err != nil || len(done) == 0 {
		t.Fatalf("unused: %v %v", err, done)
	}
	for _, h := range r.p.Holds() {
		if !h.Used {
			t.Fatal("an unused hold output is still offered to puts")
		}
	}
}

// The writer's ranking: by first sight, ties to the lower txid; a renewal
// keeps the original's sight.
func TestRankingByFirstSight(t *testing.T) {
	race := &Race{Cands: []*Cand{
		{Advertiser: "b", First: "bb", FirstSeen: 5},
		{Advertiser: "a", First: "cc", FirstSeen: 5},
		{Advertiser: "c", First: "aa", FirstSeen: 9},
		{Advertiser: "d", First: "ff", FirstSeen: 1},
	}}
	var got []string
	for _, c := range race.Ranked() {
		got = append(got, c.Advertiser)
	}
	if strings.Join(got, "") != "dbac" {
		t.Fatalf("ranked %v", got)
	}
	p := &Publisher{cfg: Config{Logf: t.Logf, Now: time.Now}}
	r := &Race{}
	ad := &storerec.Advertisement{Host: []byte{2, 1}, Expiry: 100, URL: "u1"}
	p.sight(r, ad, "22", 0, 10)
	p.sight(r, &storerec.Advertisement{Host: []byte{2, 1}, Expiry: 200, URL: "u2"}, "33", 0, 20)
	if len(r.Cands) != 1 || r.Cands[0].FirstSeen != 10 || r.Cands[0].First != "22" || r.Cands[0].URL != "u2" {
		t.Fatalf("a renewal: %+v", r.Cands[0])
	}
}
