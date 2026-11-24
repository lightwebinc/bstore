package module

import (
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

const otherBucket = "archive_qzvmkwerty"

// twoBuckets is a host serving two buckets, each with a served chain whose
// token names a writer, entries anchored and not, a hold, a sweep that
// retracts an entry, an advertisement, an outcome and a front statement.
type twoBuckets struct {
	r      *rig
	owners map[string]*ownerSim
	served map[string]string
	probes map[string][]map[string]string
}

func newTwoBuckets(t *testing.T) *twoBuckets {
	t.Helper()
	ch := newTestChain(80000, nil)
	cl := newClock(T0 + uint64(dayMs))
	tb := &twoBuckets{owners: map[string]*ownerSim{}, served: map[string]string{}, probes: map[string][]map[string]string{}}
	for _, b := range []string{bucket, otherBucket} {
		tb.owners[b] = newOwner(b, b+" owner", ch)
		tb.served[b] = tb.owners[b].served()
	}
	tb.r = setup(t, setupOpts{chain: ch, clock: cl, config: func(c *Config) {
		c.Buckets = []string{bucket, otherBucket}
		c.Served = []string{tb.served[bucket], tb.served[otherBucket]}
	}})
	for _, b := range []string{bucket, otherBucket} {
		tb.fill(t, b)
	}
	// One sweep admitted in both buckets: the same txid, outpoints and
	// carrier funding in two buckets' indexes.
	there := newAuthor(otherBucket, "shared alice", ch)
	e := there.put(tb.owners[otherBucket].id, "shared", putOpts{})
	one(t, tb.r.submitTx(e.tx, storerec.RecordsPrefix+otherBucket), "shared carrier")
	sw := mintSweep(e.tree, []uint32{e.vout}, there.party, storerec.EntryDerivation, ch)
	one(t, tb.r.submitTx(sw, storerec.RecordsPrefix+bucket), "shared sweep here")
	one(t, tb.r.submitTx(sw, storerec.RecordsPrefix+otherBucket), "shared sweep there")
	tb.probes[otherBucket] = append(tb.probes[otherBucket], map[string]string{"bucket": otherBucket, "spent": e.op})
	tb.probes[bucket] = append(tb.probes[bucket], map[string]string{"bucket": bucket, "spent": e.op})
	return tb
}

func (tb *twoBuckets) fill(t *testing.T, b string) {
	t.Helper()
	r, ch, owner, tp := tb.r, tb.r.chain, tb.owners[b], storerec.RecordsPrefix+b
	alice := newAuthor(b, b+" alice", ch)
	prov := newProvider(b+" provider", ch)
	buyer := newAuthor(b, b+" buyer", ch)
	front := newAuthor(b, b+" front", ch)
	one(t, r.submit(owner.token(tokenOpts{}).beef, tp), "token 0")
	e1 := alice.put(owner.id, "k1", putOpts{})
	e2 := alice.put(owner.id, "k2", putOpts{})
	h := alice.hold(1000)
	e3 := alice.put(owner.id, "k3", putOpts{hold: h.terms})
	for _, tx := range [][]byte{atomicOf(t, e1.tx), atomicOf(t, e2.tx), atomicOf(t, h.tx), atomicOf(t, e3.tx)} {
		one(t, r.submit(tx, tp), "carrier")
	}
	one(t, r.submit(owner.token(tokenOpts{writers: [][]byte{alice.id()}, anchor: []*posted{e1}}).beef, tp), "token 1")
	sw := mintSweep(e2.tree, []uint32{e2.vout}, alice.party, storerec.EntryDerivation, ch)
	one(t, r.submitTx(sw, tp), "sweep")
	root := sha(b + " root")
	one(t, r.submitTx(prov.advertise([][32]byte{root}, uint64((r.clock.ms()+400*dayMs)/1000)), tp), "ad")
	one(t, r.submitTx(buyer.outcome(outcomeOpts{root: root, provider: prov.party, answerer: prov.party, outcome: 1}).tx, tp), "outcome")
	one(t, r.submitTx(front.statement(prov.party.id, 0).tx, tp), "statement")
	tb.probes[b] = []map[string]string{
		{"bucket": b, "owner": owner.hex(), "id": owner.idHex()},
		{"bucket": b, "entry": e1.txid},
		{"bucket": b, "entry": e2.txid},
		{"bucket": b, "entry": e3.txid},
		{"bucket": b, "hold": h.op},
		{"bucket": b, "spent": e2.op},
		{"bucket": b, "root": hex.EncodeToString(root[:]), "after": ""},
	}
}

// restartWith is the host restarted over the same storage and journal with
// these buckets: the index a full restore builds.
func (tb *twoBuckets) restartWith(t *testing.T, buckets ...string) *rig {
	t.Helper()
	r := tb.r
	return setup(t, setupOpts{chain: r.chain, storage: r.storage, journal: r.journal, clock: r.clock, config: func(c *Config) {
		*c = *r.config
		c.Buckets = buckets
		c.Served = nil
		for _, b := range buckets {
			c.Served = append(c.Served, tb.served[b])
		}
	}})
}

// rows are a bucket's records topic's unspent rows, as a host hands them.
func (tb *twoBuckets) rows(b string) []bmodule.Output {
	return tb.r.storage.utxos(storerec.RecordsPrefix + b)
}

// answers are the probes' answers on r, for the buckets r serves.
func (tb *twoBuckets) answers(t *testing.T, r *rig) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for b, qs := range tb.probes {
		for i, q := range qs {
			k := b + "#" + string(rune('0'+i))
			if !r.ls.buckets[b] {
				out[k] = []string{"refused"}
				continue
			}
			out[k] = r.answer(q)
		}
	}
	return out
}

// indexView is the service's state as plain maps: what a full restore and
// a bucket change must agree on. Insertion order across buckets is left out
// (it follows the order rows arrive in, which no answer depends on), and so
// is when a drop was queued.
func indexView(s *Service) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	drops := map[string]string{}
	for k, d := range s.drops {
		drops[k] = d.b + "|" + d.txid + "|" + d.topic
	}
	served := slices.Clone(s.servedList)
	sort.Strings(served)
	rereads := make([]string, 0, len(s.rereads))
	for _, r := range s.rereads {
		rereads = append(rereads, r.b+"|"+r.txid)
	}
	sort.Strings(rereads)
	return map[string]any{
		"buckets": s.buckets, "served": s.served, "servedList": served, "servedBuckets": s.servedBuckets, "servedIdx": s.servedIdx,
		"rows": plainOf(s.rows), "marks": plainOf(s.marks), "table": plainOf(s.table), "seen": plainOf(s.seen), "tokenSeen": plainOf(s.tokenSeen),
		"minedSeen": plainOf(s.minedSeen), "spentHeld": s.spentHeld,
		"chains": s.chains, "tokenChain": s.tokenChain, "carriers": plainOf(s.carriers), "byOp": s.byOp, "outcomesBy": s.outcomesBy,
		"outcomeHistory": s.outcomeHistory, "frontsByAdvertiser": s.frontsByAdvertiser, "frontsByPair": s.frontsByPair,
		"ads": plainOf(s.ads), "adsLive": s.adsLive, "since": s.since, "holds": plainOf(s.holds), "sweeps": plainOf(s.sweeps),
		"sweepsByOp": s.sweepsByOp, "heldByTx": s.heldByTx, "rereads": rereads, "drops": drops, "relisted": s.relisted, "reached": s.reached,
		"ready": s.ready,
	}
}

func sameIndex(t *testing.T, got, want *Service, what string) {
	t.Helper()
	g, w := indexView(got), indexView(want)
	for k := range w {
		if !reflect.DeepEqual(g[k], w[k]) {
			t.Errorf("%s: %s differs from a full restore\n got %#v\nwant %#v", what, k, g[k], w[k])
		}
	}
}

func TestBucketsFilled(t *testing.T) {
	tb := newTwoBuckets(t)
	c := tb.r.ls.Counts()
	if c.Tokens == 0 || c.Entries == 0 || c.Holds == 0 || c.Sweeps == 0 || c.Ads == 0 || c.Outcomes == 0 || c.Fronts == 0 {
		t.Fatalf("the scenario leaves a kind of the index empty: %+v", c)
	}
	for _, b := range []string{bucket, otherBucket} {
		for _, q := range tb.probes[b] {
			if len(tb.r.answer(q)) == 0 && q["spent"] == "" && q["entry"] == "" {
				t.Fatalf("%v answers nothing", q)
			}
		}
	}
}

// Adding a bucket indexes it as a restart with both buckets would, and
// leaves the other bucket's index as it was.
func TestAddBucketsIndexesAsARestart(t *testing.T) {
	tb := newTwoBuckets(t)
	full := tb.restartWith(t, bucket, otherBucket)
	one := tb.restartWith(t, bucket)
	before := len(one.ls.chains)
	n, err := one.ls.AddBuckets(context.Background(), []string{otherBucket}, []string{tb.served[otherBucket]}, tb.rows(otherBucket), one.storage)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("nothing indexed")
	}
	sameIndex(t, one.ls, full.ls, "added")
	if len(one.ls.chains) <= before {
		t.Fatal("the added bucket's chain is not indexed")
	}
	if !reflect.DeepEqual(tb.answers(t, one), tb.answers(t, full)) {
		t.Fatal("answers differ from a full restore")
	}
	if _, err := one.ls.AddBuckets(context.Background(), []string{otherBucket}, nil, nil, nil); err == nil {
		t.Fatal("a bucket added twice")
	}
	if _, err := one.ls.AddBuckets(context.Background(), []string{"third_qzvmkwerty"}, []string{tb.served[bucket]}, nil, nil); err == nil {
		t.Fatal("a served chain of another bucket")
	}
}

// Forgetting a bucket leaves the index a restart without it builds; adding
// it back gives the index with both again.
func TestForgetBucketsAsARestart(t *testing.T) {
	tb := newTwoBuckets(t)
	both := tb.restartWith(t, bucket, otherBucket)
	if err := both.ls.ForgetBuckets([]string{otherBucket}); err != nil {
		t.Fatal(err)
	}
	sameIndex(t, both.ls, tb.restartWith(t, bucket).ls, "forgotten")
	if both.ls.BucketCount() != 1 {
		t.Fatal(both.ls.BucketCount())
	}
	if _, err := both.ls.AddBuckets(context.Background(), []string{otherBucket}, []string{tb.served[otherBucket]}, tb.rows(otherBucket), nil); err != nil {
		t.Fatal(err)
	}
	full := tb.restartWith(t, bucket, otherBucket)
	sameIndex(t, both.ls, full.ls, "forgotten and added again")
	if !reflect.DeepEqual(tb.answers(t, both), tb.answers(t, full)) {
		t.Fatal("answers differ from a full restore")
	}
	if err := both.ls.ForgetBuckets([]string{"third_qzvmkwerty"}); err == nil {
		t.Fatal("a bucket not configured")
	}
}

// Change takes a change of buckets in place, keeping the running managers,
// and returns nil for any other change, so the host builds the module
// afresh.
func TestChangeTakesBucketsOnly(t *testing.T) {
	tb := newTwoBuckets(t)
	r := tb.restartWith(t, bucket)
	r.m.headers = r.chain
	envServed := func(svs ...string) string {
		out := make([]string, len(svs))
		for i, sv := range svs {
			out[i] = strings.ReplaceAll(sv, "|", ":")
		}
		return strings.Join(out, ",")
	}
	env := map[string]string{
		"BSTORE_BUCKETS": bucket + "," + otherBucket, "BSTORE_SERVED": envServed(tb.served[bucket], tb.served[otherBucket]),
		"BSTORE_STATE_DIR": r.config.StateDir, "BSTORE_HEADERS_URL": r.config.HeadersURL, "BSTORE_KEEP_DAYS": "31", "BSTORE_PENDING_DAYS": "7",
		"BSTORE_OTHER_HOURS": "24",
		"OVERLAY_TOPICS":     strings.Join([]string{"tm_x", storerec.RecordsPrefix + bucket, storerec.RecordsPrefix + otherBucket, storerec.BlobPrefix + otherBucket}, ","),
	}
	get := func(k string) string { return env[k] }
	// The parsed settings of this rig, as Change compares them.
	parsed, err := ParseConfig(get)
	if err != nil {
		t.Fatal(err)
	}
	cfg := *r.config
	cfg.Sessions, cfg.Budget, cfg.Response, cfg.Accept, cfg.Prices, cfg.Positions = parsed.Sessions, parsed.Budget, parsed.Response, parsed.Accept, parsed.Prices, parsed.Positions
	cfg.JournalSync, cfg.MaxBEEF, cfg.MaxSweepInputs, cfg.Listen, cfg.PayeeKey = parsed.JournalSync, parsed.MaxBEEF, parsed.MaxSweepInputs, parsed.Listen, parsed.PayeeKey
	r.m.config = &cfg
	kept := r.m.Module.Topics[storerec.RecordsPrefix+bucket]

	ch, err := r.m.Change(get)
	if err != nil || ch == nil {
		t.Fatalf("a bucket added: %v %v", ch, err)
	}
	if !slices.Equal(ch.Added(), []string{otherBucket}) || len(ch.Removed()) != 0 || !slices.Equal(ch.Topics(), []string{storerec.RecordsPrefix + otherBucket}) {
		t.Fatalf("added %v removed %v topics %v", ch.Added(), ch.Removed(), ch.Topics())
	}
	mod := ch.Module()
	if mod.Topics[storerec.RecordsPrefix+bucket] != kept {
		t.Fatal("the running manager was replaced")
	}
	if mod.Topics[storerec.RecordsPrefix+otherBucket] == nil || mod.Topics[storerec.BlobPrefix+otherBucket] == nil || len(mod.Topics) != 3 {
		t.Fatalf("topics %v", reflect.ValueOf(mod.Topics).MapKeys())
	}
	if r.ls.BucketCount() != 1 {
		t.Fatal("Change changed the index before Apply")
	}
	if _, err := ch.Apply(context.Background(), tb.rows(otherBucket), r.storage); err != nil {
		t.Fatal(err)
	}
	sameIndex(t, r.ls, tb.restartWith(t, bucket, otherBucket).ls, "changed")
	if len(r.m.Module.Topics) != 3 || r.ls.BucketCount() != 2 {
		t.Fatal("the module was not changed")
	}

	// Dropping it again.
	env["BSTORE_BUCKETS"], env["BSTORE_SERVED"], env["OVERLAY_TOPICS"] = bucket, envServed(tb.served[bucket]), "tm_x,"+storerec.RecordsPrefix+bucket
	ch, err = r.m.Change(get)
	if err != nil || ch == nil || !slices.Equal(ch.Removed(), []string{otherBucket}) || len(ch.Topics()) != 0 {
		t.Fatalf("a bucket dropped: %v %v", ch, err)
	}
	if _, err := ch.Apply(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	sameIndex(t, r.ls, tb.restartWith(t, bucket).ls, "dropped")

	// A blob topic carried later, the buckets unchanged: taken in place,
	// with nothing to index.
	env["OVERLAY_TOPICS"] = "tm_x," + storerec.RecordsPrefix + bucket + "," + storerec.BlobPrefix + bucket
	ch, err = r.m.Change(get)
	if err != nil || ch == nil || len(ch.Added()) != 0 || len(ch.Removed()) != 0 || ch.Module().Topics[storerec.BlobPrefix+bucket] == nil {
		t.Fatalf("a blob topic added later: %v %v", ch, err)
	}
	if _, err := ch.Apply(context.Background(), nil, nil); err != nil || r.m.Module.Topics[storerec.BlobPrefix+bucket] == nil {
		t.Fatalf("apply: %v", err)
	}
	env["OVERLAY_TOPICS"] = "tm_x," + storerec.RecordsPrefix + bucket

	// Anything else is a rebuild.
	env["BSTORE_KEEP_DAYS"] = "40"
	if ch, err := r.m.Change(get); err != nil || ch != nil {
		t.Fatalf("another setting: %v %v", ch, err)
	}
	env["BSTORE_KEEP_DAYS"] = "31"
	other := newOwner(bucket, "another owner", r.chain)
	env["BSTORE_SERVED"] = envServed(tb.served[bucket], other.served())
	if ch, err := r.m.Change(get); err != nil || ch != nil {
		t.Fatalf("a chain served in a bucket kept: %v %v", ch, err)
	}
	env["BSTORE_SERVED"] = envServed(tb.served[bucket])
	// Settings a new module would refuse are an error.
	env["OVERLAY_TOPICS"] = "tm_x," + storerec.RecordsPrefix + bucket + "," + storerec.BlobPrefix + otherBucket
	if _, err := r.m.Change(get); err == nil {
		t.Fatal("a blob topic of a bucket not configured")
	}
}

// plainOf is an ordered map's entries, without their order.
func plainOf[V any](o *omap[V]) map[string]V {
	out := make(map[string]V, len(o.m))
	for k, x := range o.m {
		out[k] = x.v
	}
	return out
}

// failingStorage fails every read, as a store that went away mid-change.
type failingStorage struct{ *memStorage }

func (failingStorage) FindOutput(context.Context, *transaction.Outpoint, string) (*bmodule.Output, error) {
	return nil, errors.New("storage went away")
}

// A failed AddBuckets leaves the buckets as they were: the added bucket is
// neither configured nor half indexed.
func TestAddBucketsFailedLeavesNothing(t *testing.T) {
	tb := newTwoBuckets(t)
	one := tb.restartWith(t, bucket)
	seam := one.ls.storage
	if _, err := one.ls.AddBuckets(context.Background(), []string{otherBucket}, []string{tb.served[otherBucket]}, tb.rows(otherBucket), failingStorage{one.storage}); err == nil {
		t.Fatal("a storage failure was not reported")
	}
	if one.ls.storage != seam {
		t.Fatal("a failed add left its storage as the restore seam")
	}
	sameIndex(t, one.ls, tb.restartWith(t, bucket).ls, "after a failed add")
	if one.ls.BucketCount() != 1 || one.ls.sets.Load().buckets[otherBucket] {
		t.Fatal("the failed bucket is still configured")
	}
}

// Lookups, admissions' first checks and replays read the buckets without
// the index's lock while buckets come and go (run with -race).
func TestBucketsChangeUnderLookups(t *testing.T) {
	tb := newTwoBuckets(t)
	r := tb.restartWith(t, bucket, otherBucket)
	q := question(tb.probes[otherBucket][0])
	admitted := tb.rows(otherBucket)[0] // admitted again: an admission's first check, then the index's
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = r.ls.Lookup(context.Background(), q)
				_, _ = r.ls.Classify(storerec.LookupService, q.Query)
				_ = r.ls.OutputNoLongerRetainedInHistory(context.Background(), &transaction.Outpoint{}, storerec.RecordsPrefix+otherBucket)
				_ = r.ls.OutputAdmittedByTopic(context.Background(), &bmodule.OutputAdmittedByTopic{Mode: bmodule.AdmitWholeTx, Topic: storerec.RecordsPrefix + otherBucket,
					Outpoint: admitted.Outpoint, AtomicBEEF: admitted.Beef})
				_, _ = r.ls.Replay(context.Background(), &bmodule.ReplayRequest{Topic: storerec.RecordsPrefix + otherBucket, Peer: "peer", Ask: replayPeer(r),
					MaxQuestions: 4, MaxObjects: 4, Applied: func(context.Context, chainhash.Hash) (bool, error) { return true, nil }})
			}
		}()
	}
	for range 50 {
		if err := r.ls.ForgetBuckets([]string{otherBucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ls.AddBuckets(context.Background(), []string{otherBucket}, []string{tb.served[otherBucket]}, tb.rows(otherBucket), nil); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	// Lookups and walks update what they cache in the index (heads, since
	// lists), so the index is compared by its answers here; the quiet churn
	// below is compared whole.
	full := tb.restartWith(t, bucket, otherBucket)
	if !reflect.DeepEqual(tb.answers(t, r), tb.answers(t, full)) {
		t.Fatal("answers after the churn differ from a full restore")
	}
	quiet := tb.restartWith(t, bucket, otherBucket)
	for range 5 {
		if err := quiet.ls.ForgetBuckets([]string{otherBucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := quiet.ls.AddBuckets(context.Background(), []string{otherBucket}, []string{tb.served[otherBucket]}, tb.rows(otherBucket), nil); err != nil {
			t.Fatal(err)
		}
	}
	sameIndex(t, quiet.ls, tb.restartWith(t, bucket, otherBucket).ls, "after a quiet churn")
}

// A dropped bucket's walk tries go, as at a restart, and a walk that began
// before the drop moves no position and counts no try: neither while the
// bucket is dropped nor once it is added again (the rebuilt index never saw
// what that walk saw). A walk begun after the re-add counts afresh.
func TestForgetBucketsFencesWalks(t *testing.T) {
	tb := newTwoBuckets(t)
	r := tb.restartWith(t, bucket, otherBucket)
	key := otherBucket + "|peer"
	var txid chainhash.Hash
	stale := r.ls.genOf(otherBucket) // a walk begins
	for range SinceTries {
		r.ls.tried(key, txid, "p1", stale)
	}
	if !r.ls.tried(key, txid, "p1", stale) {
		t.Fatal("not counted")
	}
	if err := r.ls.ForgetBuckets([]string{otherBucket}); err != nil {
		t.Fatal(err)
	}
	if len(r.ls.sinceTries[key]) != 0 {
		t.Fatal("the dropped bucket's tries stay")
	}
	before := r.ls.reachedAt(key)
	r.ls.reach(key, "p9", stale)
	if r.ls.tried(key, txid, "p1", stale) || r.ls.reachedAt(key) != before || len(r.ls.sinceTries[key]) != 0 {
		t.Fatal("a walk for a dropped bucket changed its state")
	}
	if _, err := r.ls.AddBuckets(context.Background(), []string{otherBucket}, []string{tb.served[otherBucket]}, tb.rows(otherBucket), nil); err != nil {
		t.Fatal(err)
	}
	// The walk that spanned the drop and the re-add still moves nothing.
	r.ls.reach(key, "p9", stale)
	if r.ls.tried(key, txid, "p1", stale) || r.ls.reachedAt(key) != before || len(r.ls.sinceTries[key]) != 0 {
		t.Fatal("a walk begun before the drop changed the re-added bucket's state")
	}
	fresh := r.ls.genOf(otherBucket)
	if fresh == stale {
		t.Fatal("the re-added bucket is in the same generation")
	}
	if r.ls.tried(key, txid, "p1", fresh) || len(r.ls.sinceTries[key]) != 1 {
		t.Fatal("a walk begun after the re-add does not count afresh")
	}
	r.ls.reach(key, "p9", fresh)
	if r.ls.reachedAt(key) != "p9" {
		t.Fatal("a walk begun after the re-add moves no position")
	}
}
