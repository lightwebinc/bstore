package module

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/guard"
	bmodule "github.com/lightwebinc/bcommon/module"
	"github.com/lightwebinc/bcommon/pushdrop"
	"github.com/lightwebinc/bcommon/replay"

	"github.com/lightwebinc/bstore/storerec"
)

// The index's bounds (spec section 12.2 to 12.4).
const (
	// PageLimit is the outputs of a page of ads, unanchored, outcomes,
	// outcome-history, history and chain (tokens, for history and chain).
	PageLimit = 64
	// PerSeq is the tokens answered at one seq and kept at one seq of a
	// chain; PerSmall the classes answered eight at a time (pending, ad,
	// fronts, front, sweep).
	PerSeq   = 8
	PerSmall = 8
	// UnanchoredDays is how far back unanchored answers by admission;
	// OutcomeWindowDays the same for outcomes and its kin.
	UnanchoredDays    = 7
	OutcomeWindowDays = 31
	// FrontsPerAdvertiser is the front statements kept the long floor per
	// advertiser of a root the map names, by first sight.
	FrontsPerAdvertiser = 16
	// ReorgDepth is how many seqs below the highest the latest tokens are
	// looked for among those whose proof verifies, before the highest is
	// taken.
	ReorgDepth = 64
	// ChoiceTTL is how long the latest tokens found against the headers
	// are reused.
	ChoiceTTL = time.Minute
	// SettleMs is how long a drop waits for the engine to record the
	// admission it drops.
	SettleMs = time.Minute
)

const (
	day  = int64(24 * time.Hour / time.Millisecond)
	hour = int64(time.Hour / time.Millisecond)
)

// TokenStatuses are the token statuses the index reports, each counted.
var TokenStatuses = []string{"pending", "rooted", "consistent", "map-mismatch"}

// DropReasons are why the index drops an object.
var DropReasons = []string{"retracted", "retention", "second-carrier", "unmatched-sweep"}

// Deleter is a storage that can delete an output: what drops need beyond
// the contract's RestoreStorage. A host whose storage cannot delete keeps
// what the index dropped, unanswered.
type Deleter interface {
	DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint, topic string) error
}

// Evicter is a storage that can forget that a topic applied a
// transaction, so that the same object offered again is decided afresh.
// Without it a dropped object offered again is a duplicate the engine
// never shows the manager.
type Evicter interface {
	DeleteAppliedTransaction(ctx context.Context, txid *chainhash.Hash, topic string) error
}

// AppliedChecker is a storage that says whether a topic applied a
// transaction: a drop decided while the engine admits the object waits
// until the engine has recorded the admission.
type AppliedChecker interface {
	DoesAppliedTransactionExist(ctx context.Context, txid *chainhash.Hash, topic string) (bool, error)
}

// tokenEntry is a bucket token the host holds.
type tokenEntry struct {
	txid  string
	vout  uint32
	b     string
	chain string
	seq   uint64
	// prev is the predecessor's txid in display order; empty at seq 0.
	prev string
	rec  *storerec.Bucket
	// inputs is every outpoint the transaction's inputs spend.
	inputs []string
	// first: at seq 0, an input's outpoint gives the bucket id.
	first  bool
	height uint32
	root   *chainhash.Hash
	rooted bool
	status string
	reason string
	// at is the first sight, Unix milliseconds.
	at int64
}

type headChoice struct {
	ts []*tokenEntry
	at int64
}

type histEntry struct {
	t *tokenEntry
	c string
}

// chainIdx is one chain: a (bucket, owner, id) triple's tokens.
type chainIdx struct {
	key, b, owner, id string
	served            bool
	bySeq             map[uint64][]*tokenEntry
	seqs              sortedNumbers
	byTxid            map[string]*tokenEntry
	// children: prev txid -> the txids of tokens naming it.
	children map[string]map[string]bool
	// branches: a served chain's maps, by the txid of the last consistent
	// token of each branch.
	branches map[string]*mapState
	// top is the highest seq of a rooted token, -1 for none.
	top int64
	// turned are tokens a full seq turned away while pending, by the txid
	// they name as predecessor.
	turned map[string]map[string]uint32
	// rootedSeqs and consistentSeqs are the seqs that hold a rooted token
	// and (a served chain) a consistent one.
	rootedSeqs, consistentSeqs *seqIndex
	head                       *headChoice
	// named: a served chain's commitments its held tokens name, with how
	// many name each.
	named map[string]int
	// hist: a served chain's rooted tokens whose changes set or remove each
	// key; built when first asked.
	hist     map[string][]histEntry
	latest   []*tokenEntry
	latestOK bool
}

// servedIdx is a served chain's own indexes of entries no token of it
// names (pending, unanchored).
type servedIdx struct {
	chain, b, owner, id string
	// pendingByKey: key -> writer -> time keys.
	pendingByKey map[string]map[string]*sortedStrings
	// unanchored: writer -> txids, admitted within UnanchoredDays as of the
	// last retention pass.
	unanchored map[string]*sortedStrings
}

type carrierEntry struct {
	c, txid, b, kind, op string
	at                   int64
	// owner is the record's owner: its writer, buyer or front.
	owner   string
	created uint64
	tkey    string
	// entry
	id, key string
	ekind   uint64
	root    string
	hold    string
	// outcome
	answerer, provider, oroot string
	// front
	advertiser string
}

type adEntry struct {
	b, txid          string
	vout             uint32
	root, advertiser string
	expiry           uint64
	at               int64
	spent, anyone    bool
}

type holdEntry struct {
	b, txid string
	vout    uint32
	at      int64
	spent   bool
}

type sweepEntry struct {
	b, txid string
	spent   []string
	height  uint32
	offset  uint64
	at      int64
}

type rowEntry struct {
	c  string
	at int64
}

type markEntry struct {
	key  string
	at   int64
	life bool
	b    string
}

type tableEntry struct {
	s  string
	at int64
}

type seenEntry struct {
	b  string
	at int64
}

type tokenSeenEntry struct {
	b    string
	vout uint32
	at   int64
}

type minedSeenEntry struct {
	b, txid string
	vout    uint32
	at      int64
}

type reread struct {
	b, txid string
	vout    uint32
}

type dropEntry struct {
	b, txid, topic string
	vout           uint32
	since          time.Time
}

// tally is the last retention pass's counts, for the gauges.
type tally struct {
	MapsComplete, MapsIncomplete, ChainsBroken, Anchored int
}

// Options build a Service.
type Options struct {
	Buckets []string
	// Served are the chains this host serves: <bucket>|<owner hex>|<id hex>.
	Served  []string
	Host    bmodule.Host
	Journal Journal
	// Priced are the priceable classes this host prices: asked through the
	// engine, they are refused.
	Priced map[string]bool
	// The retention floors; their floors when zero.
	KeepDays, PendingDays, OtherHours int
	Headers                           chaintracker.ChainTracker
	// Now is the host's clock; time.Now when nil.
	Now func() time.Time
	// Trees is the chain view a replay reads a chain's first token's parent
	// from (Replay); nil when the host has none.
	Trees replay.TreeSource
	// MaxBEEF and MaxSweepInputs are the topic managers' bounds, which a
	// replay holds a token it assembles to; storerec's defaults when zero.
	MaxBEEF, MaxSweepInputs int
}

// Service is ls_bstore: one lookup service for every bucket a host carries
// (spec sections 11 and 12.2 to 12.6).
//
// Two stores back it. The engine's storage holds the outputs the host
// keeps: bucket tokens, entry, outcome and front carriers, advertisements,
// hold outputs and sweeps' output 0, which this service indexes in memory
// and rebuilds from storage on start. The journal holds what storage
// cannot: the first carrier of each funding outpoint, the input outpoints
// of every admitted sweep, the retraction marks, first sights, and which
// held outputs a later transaction spent.
//
// Every status is a function of what the host holds, never of arrival
// order, except where the contract makes it one (the first carrier per
// funding outpoint, the first 16 front statements, first sights):
//
//   - a token is rooted when the host holds every token from it back to its
//     chain's first by spends, pending until then; a rooted token of a
//     served chain takes the status its branch's fold gives it (consistent,
//     or map-mismatch, sticky). Only rooted, consistent tokens are answered
//     as latest;
//   - an entry is anchored while a served chain's current map holds it,
//     replaced once a consistent token named it and the map no longer does,
//     pending while no token of the chain names it, and retracted when an
//     admitted sweep spends its funding outpoint or the outpoint carries a
//     retraction mark. A retracted carrier is dropped at once and never
//     answered;
//   - an advertisement is live until a renewal (or any admitted
//     transaction) spends it or its expiry passes the host's clock.
//
// To drop an object is to delete its output from the topic and evict the
// engine's record that its transaction was applied, so the same object
// offered again is decided afresh, through the storage a restore is handed
// (Deleter, Evicter).
type Service struct {
	mu sync.Mutex
	// admitting counts the host's calls waiting for mu (lockAdmission): each
	// holds its topic's lock in the host meanwhile, so a Flush gives mu up
	// at its next drop while one waits.
	admitting atomic.Int32

	buckets       map[string]bool
	served        map[string]bool
	servedList    []string
	servedBuckets map[string]bool
	host          bmodule.Host
	journal       Journal
	priced        map[string]bool
	keep, pending int64
	other         int64
	headers       chaintracker.ChainTracker
	now           func() time.Time
	// What a replay needs beyond the index (Replay).
	trees             replay.TreeSource
	maxBEEF, maxSweep int

	// What the journal holds.
	rows      *omap[rowEntry]
	marks     *omap[*markEntry]
	table     *omap[tableEntry]
	seen      *omap[seenEntry]
	tokenSeen *omap[tokenSeenEntry]
	minedSeen *omap[minedSeenEntry]
	spentHeld map[string]bool

	// What the host holds, from storage.
	chains             map[string]*chainIdx
	tokenChain         map[string]string
	servedIdx          map[string]*servedIdx
	carriers           *omap[*carrierEntry]
	byOp               map[string]map[string]bool
	outcomesBy         map[string]*sortedStrings
	outcomeHistory     map[string]*sortedStrings
	frontsByAdvertiser map[string]*sortedStrings
	frontsByPair       map[string]*sortedStrings
	ads                *omap[*adEntry]
	adsLive            map[string]*sortedStrings
	holds              *omap[*holdEntry]
	sweeps             *omap[*sweepEntry]
	sweepsByOp         map[string]map[string]bool
	// heldByTx: by <bucket>|<txid>, the output indexes the index holds, so
	// a transaction is evicted only once none is left.
	heldByTx map[string]map[uint32]bool

	rereads []reread
	// drops: outputs to delete from storage, with the engine's record of
	// their admission.
	drops map[string]*dropEntry
	// recheck: later carriers on an outpoint a restore read, decided once
	// every chain is folded.
	recheck   []string
	storage   bmodule.RestoreStorage
	restoring bool
	ready     bool
	warned    map[string]bool
	// holdDrops keeps drops queued, for tests.
	holdDrops bool
	// betweenChunks, when set, runs between two chunks of a Flush, with the
	// lock released (tests).
	betweenChunks func()

	last tally
}

var (
	_ bmodule.LookupService     = (*Service)(nil)
	_ bmodule.RetentionListener = (*Service)(nil)
	_ bmodule.Restorer          = (*Service)(nil)
)

// NewService builds ls_bstore and loads the journal.
func NewService(o Options) (*Service, error) {
	floor := func(name string, v, def int) (int64, error) {
		if v == 0 {
			v = def
		}
		if v < def {
			return 0, fmt.Errorf("ls_bstore: %s of %d is under its floor of %d", name, v, def)
		}
		return int64(v), nil
	}
	keep, err := floor("keep days", o.KeepDays, FloorKeepDays)
	if err != nil {
		return nil, err
	}
	pend, err := floor("pending days", o.PendingDays, FloorPendingDays)
	if err != nil {
		return nil, err
	}
	other, err := floor("other hours", o.OtherHours, FloorOtherHours)
	if err != nil {
		return nil, err
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Journal == nil {
		o.Journal = &MemoryJournal{}
	}
	s := &Service{buckets: map[string]bool{}, served: map[string]bool{}, servedBuckets: map[string]bool{}, host: o.Host, journal: o.Journal,
		priced: o.Priced, keep: keep * day, pending: pend * day, other: other * hour, headers: o.Headers, now: o.Now, warned: map[string]bool{},
		trees: o.Trees, maxBEEF: o.MaxBEEF, maxSweep: o.MaxSweepInputs}
	for _, b := range o.Buckets {
		s.buckets[b] = true
	}
	for _, sv := range o.Served {
		if s.served[sv] {
			continue
		}
		s.served[sv] = true
		s.servedList = append(s.servedList, sv)
		s.servedBuckets[strings.SplitN(sv, "|", 2)[0]] = true
	}
	if s.priced == nil {
		s.priced = map[string]bool{}
	}
	s.clear()
	lines, err := s.journal.Load()
	if err != nil {
		return nil, err
	}
	for i := range lines {
		s.apply(&lines[i])
	}
	return s, nil
}

func (s *Service) clear() {
	s.rows = newOmap[rowEntry]()
	s.marks = newOmap[*markEntry]()
	s.table = newOmap[tableEntry]()
	s.seen = newOmap[seenEntry]()
	s.tokenSeen = newOmap[tokenSeenEntry]()
	s.minedSeen = newOmap[minedSeenEntry]()
	s.spentHeld = map[string]bool{}
	s.chains = map[string]*chainIdx{}
	s.tokenChain = map[string]string{}
	s.carriers = newOmap[*carrierEntry]()
	s.byOp = map[string]map[string]bool{}
	s.outcomesBy = map[string]*sortedStrings{}
	s.outcomeHistory = map[string]*sortedStrings{}
	s.frontsByAdvertiser = map[string]*sortedStrings{}
	s.frontsByPair = map[string]*sortedStrings{}
	s.ads = newOmap[*adEntry]()
	s.adsLive = map[string]*sortedStrings{}
	s.holds = newOmap[*holdEntry]()
	s.sweeps = newOmap[*sweepEntry]()
	s.sweepsByOp = map[string]map[string]bool{}
	s.heldByTx = map[string]map[uint32]bool{}
	s.rereads = nil
	s.drops = map[string]*dropEntry{}
	s.recheck = nil
	s.servedIndexes()
}

// servedIndexes makes an empty index of unnamed entries for each served
// chain.
func (s *Service) servedIndexes() {
	s.servedIdx = map[string]*servedIdx{}
	for _, sv := range s.servedList {
		p := strings.Split(sv, "|")
		s.servedIdx[sv] = &servedIdx{chain: sv, b: p[0], owner: p[1], id: p[2], pendingByKey: map[string]map[string]*sortedStrings{}, unanchored: map[string]*sortedStrings{}}
	}
}

func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

// ---- gauges

// Counts are the gauges' values.
type Counts struct {
	Tokens, PendingTokens, Entries, Outcomes, Fronts, Ads, Holds, Sweeps, SweepOutpoints, Marks, Rows, PendingDrops int
	Tally                                                                                                           tally
}

// Counts are the gauges' values now.
func (s *Service) Counts() Counts {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := Counts{Tokens: len(s.tokenChain), Ads: s.ads.len(), Holds: s.holds.len(), Sweeps: s.sweeps.len(), SweepOutpoints: s.table.len(),
		Marks: s.marks.len(), Rows: s.rows.len(), PendingDrops: len(s.drops), Tally: s.last}
	for _, v := range s.chains {
		for _, t := range v.byTxid {
			if !t.rooted {
				c.PendingTokens++
			}
		}
	}
	for _, e := range s.carriers.values() {
		switch e.kind {
		case "entry":
			c.Entries++
		case "outcome":
			c.Outcomes++
		case "front":
			c.Fronts++
		}
	}
	return c
}

// Restored is whether the index is restored and ready to answer.
func (s *Service) Restored() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready
}

// TokenStatus is the status of the token with txid (display order), when
// the host holds it.
func (s *Service) TokenStatus(txid string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.tokenChain[txid]
	if !ok {
		return "", false
	}
	t := s.chains[k].byTxid[txid]
	if t == nil {
		return "", false
	}
	return t.status, true
}

// HoldsCarrier is whether the host holds the carrier with commitment c
// (hash byte order hex).
func (s *Service) HoldsCarrier(c string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.carriers.has(c)
}

// Mark is the retraction mark of outpoint op, when the host keeps one:
// its owner, its time and whether it is kept for the host's life.
func (s *Service) Mark(op string) (key string, at int64, life bool, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.marks.get(op)
	if !ok {
		return "", 0, false, false
	}
	return m.key, m.at, m.life, true
}

// Swept is whether the host keeps an admitted sweep's spend of outpoint op.
func (s *Service) Swept(op string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.table.has(op)
}

// FirstOn is the first carrier the host admitted on funding outpoint op.
func (s *Service) FirstOn(op string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows.get(op)
	return r.c, ok
}

// ---- the journal

func (s *Service) apply(l *Line) {
	switch l.K {
	case "c":
		if !s.rows.has(l.Op) {
			s.rows.set(l.Op, rowEntry{c: l.C, at: l.At})
		}
	case "cf":
		s.rows.del(l.Op)
	case "a":
		if !s.seen.has(l.C) {
			s.seen.set(l.C, seenEntry{b: l.B, at: l.At})
		}
	case "af":
		s.seen.del(l.C)
	case "x":
		if !s.marks.has(l.Op) {
			s.marks.set(l.Op, &markEntry{key: l.Key, at: l.At, life: l.Life, b: l.B})
		}
	case "xl":
		if m, ok := s.marks.get(l.Op); ok {
			m.life = true
		}
	case "xf":
		s.marks.del(l.Op)
	case "p":
		if !s.table.has(l.Op) {
			s.table.set(l.Op, tableEntry{s: l.S, at: l.At})
		}
	case "pf":
		s.table.del(l.Op)
	case "t":
		if !s.tokenSeen.has(l.Txid) {
			s.tokenSeen.set(l.Txid, tokenSeenEntry{b: l.B, vout: uint32(l.Vout), at: l.At}) //nolint:gosec // a journal's vout
		}
	case "tf":
		s.tokenSeen.del(l.Txid)
	case "o":
		k := heldKey(l.B, l.Txid, uint32(l.Vout)) //nolint:gosec // a journal's vout
		if !s.minedSeen.has(k) {
			s.minedSeen.set(k, minedSeenEntry{b: l.B, txid: l.Txid, vout: uint32(l.Vout), at: l.At}) //nolint:gosec // a journal's vout
		}
	case "w":
		s.spentHeld[heldKey(l.B, l.Txid, uint32(l.Vout))] = true //nolint:gosec // a journal's vout
	case "of":
		k := heldKey(l.B, l.Txid, uint32(l.Vout)) //nolint:gosec // a journal's vout
		s.minedSeen.del(k)
		delete(s.spentHeld, k)
	}
}

// write appends a journal line and applies it. The time Append takes is
// spent under the service's lock (bstore_journal_wait_seconds_total): the
// write, and the fsync too when the journal syncs every line.
func (s *Service) write(l Line) {
	start := time.Now()
	err := s.journal.Append(l)
	m := s.host.Metrics()
	m.Inc("bstore_journal_wait_seconds_total", nil, time.Since(start).Seconds())
	m.Inc("bstore_journal_lines_total", nil, 1)
	if err != nil {
		s.host.Log("ls_bstore could not write the journal", map[string]any{"kind": l.K, "err": err.Error()})
	}
	s.apply(&l)
}

// compact rewrites the journal as the state it holds: what the host keeps,
// and nothing it dropped.
func (s *Service) compact() {
	var out []Line
	for _, op := range s.rows.keys() {
		r, _ := s.rows.get(op)
		out = append(out, Line{K: "c", Op: op, C: r.c, At: r.at})
	}
	for _, op := range s.marks.keys() {
		m, _ := s.marks.get(op)
		out = append(out, Line{K: "x", Op: op, Key: m.key, At: m.at, Life: m.life, B: m.b})
	}
	for _, op := range s.table.keys() {
		p, _ := s.table.get(op)
		out = append(out, Line{K: "p", Op: op, S: p.s, At: p.at})
	}
	for _, c := range s.seen.keys() {
		if sc, _ := s.seen.get(c); s.carriers.has(c) {
			out = append(out, Line{K: "a", B: sc.b, C: c, At: sc.at})
		}
	}
	for _, txid := range s.tokenSeen.keys() {
		t, _ := s.tokenSeen.get(txid)
		out = append(out, Line{K: "t", B: t.b, Txid: txid, Vout: int64(t.vout), At: t.at})
	}
	for _, k := range s.minedSeen.keys() {
		o, _ := s.minedSeen.get(k)
		out = append(out, Line{K: "o", B: o.b, Txid: o.txid, Vout: int64(o.vout), At: o.at})
		if s.spentHeld[k] {
			out = append(out, Line{K: "w", B: o.b, Txid: o.txid, Vout: int64(o.vout)})
		}
	}
	if err := s.journal.Rewrite(out); err != nil {
		s.host.Log("ls_bstore could not compact the journal", map[string]any{"err": err.Error()})
		return
	}
	for _, c := range s.seen.keys() {
		if !s.carriers.has(c) {
			s.seen.del(c)
		}
	}
	for k := range s.spentHeld {
		if !s.minedSeen.has(k) {
			delete(s.spentHeld, k)
		}
	}
}

func (s *Service) warnOnce(key, msg string) {
	if s.warned[key] {
		return
	}
	s.warned[key] = true
	s.host.Log(msg, nil)
}

// ---- helpers

func heldKey(b, txid string, vout uint32) string { return b + "|" + outpoint(txid, vout) }

func outpoint(txid string, vout uint32) string {
	return txid + "." + strconv.FormatUint(uint64(vout), 10)
}

// timeKey is the sort key of a time cursor and of a dated record:
// created, then txid in display order.
func timeKey(created uint64, txid string) string {
	return fmt.Sprintf("%016d:%s", created, txid)
}

// keyOfCursor is the sort key of a time cursor <created>:<txid> as a
// question gives it.
func keyOfCursor(q string) string {
	c, t, _ := strings.Cut(q, ":")
	n, _ := strconv.ParseUint(c, 10, 64)
	return timeKey(n, t)
}

// txidOfKey is the txid of a time key.
func txidOfKey(k string) string { return k[17:] }

// outKey is the sort key of an output of a transaction among others: txid,
// then output index.
func outKey(txid string, vout uint32) string { return fmt.Sprintf("%s:%010d", txid, vout) }

// afterTxid is the first outKey of a transaction whose txid sorts after
// txid ("" for every one).
func afterTxid(txid string) string {
	if txid == "" {
		return ""
	}
	return txid + ";"
}

// cOf is a carrier's commitment from its txid in display order: the txid
// in hash byte order, as hex.
func cOf(txid string) string {
	h, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(h[:])
}

func hexOf(b []byte) string { return hex.EncodeToString(b) }

func srcTxid(in *transaction.TransactionInput) string {
	if in.SourceTXID != nil {
		return in.SourceTXID.String()
	}
	if in.SourceTransaction != nil {
		return in.SourceTransaction.TxID().String()
	}
	return ""
}

// parse is a stored or admitted output's transaction: the subject of its
// BEEF, with its proof.
func parse(raw []byte) (*transaction.Transaction, error) {
	_, tx, _, err := guard.ParseBEEF(raw, guard.DefaultBound)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, errors.New("no subject transaction")
	}
	return tx, nil
}

// place is a proof's block height, the txid's position in its block, and
// the root the path gives (nil when it gives none).
func place(tx *transaction.Transaction) (uint32, uint64, *chainhash.Hash) {
	mp := tx.MerklePath
	if mp == nil {
		return 0, 0, nil
	}
	txid := tx.TxID()
	var offset uint64
	if len(mp.Path) > 0 {
		for _, l := range mp.Path[0] {
			if l != nil && l.Txid != nil && *l.Txid && l.Hash != nil && *l.Hash == *txid {
				offset = l.Offset
				break
			}
		}
	}
	root, err := mp.ComputeRoot(txid)
	if err != nil {
		root = nil
	}
	return mp.BlockHeight, offset, root
}

func (s *Service) bucketOf(topic string) (string, bool) {
	if !strings.HasPrefix(topic, storerec.RecordsPrefix) || strings.HasPrefix(topic, storerec.BlobPrefix) {
		return "", false
	}
	b := topic[len(storerec.RecordsPrefix):]
	return b, s.buckets[b]
}

// ---- held outputs and drops

func (s *Service) holdOutput(b, txid string, vout uint32) {
	k := b + "|" + txid
	m := s.heldByTx[k]
	if m == nil {
		m = map[uint32]bool{}
		s.heldByTx[k] = m
	}
	m[vout] = true
}

func (s *Service) releaseOutput(b, txid string, vout uint32) {
	k := b + "|" + txid
	if m := s.heldByTx[k]; m != nil {
		delete(m, vout)
		if len(m) == 0 {
			delete(s.heldByTx, k)
		}
	}
}

// queueDrop queues output vout of txid in bucket b to be deleted, with the
// engine's record of its transaction once no output of it is held.
func (s *Service) queueDrop(b, txid string, vout uint32) {
	s.releaseOutput(b, txid, vout)
	topic := storerec.RecordsPrefix + b
	k := topic + "|" + outpoint(txid, vout)
	if _, ok := s.drops[k]; !ok {
		s.drops[k] = &dropEntry{b: b, txid: txid, vout: vout, topic: topic, since: time.Now()}
	}
}

// Flush deletes dropped outputs from storage and evicts the engine's
// record that their transactions were applied once the index holds no
// output of them, so the same object offered again is decided afresh. A
// drop decided while the engine admits the object waits until the engine
// has recorded the admission (it records it after the lookup services are
// told), for at most SettleMs. A store that cannot delete keeps what was
// dropped, unanswered; a store that cannot evict leaves a dropped object a
// duplicate when it is offered again. Each is said once in the log.
//
// Flush takes the drops waiting when it starts in chunks of FlushChunk,
// releasing the lock between chunks: under a put load nearly every object
// leaves a drop (the entry it supersedes), so one pass every FlushEvery can
// carry thousands of storage deletes, and admissions (OutputAdmittedByTopic)
// and lookups wait on the lock while a chunk runs, never for the whole pass.
// Each drop is decided under the lock when its chunk runs, as before.
func (s *Service) Flush(ctx context.Context) {
	s.mu.Lock()
	keys := s.dropKeys()
	s.mu.Unlock()
	for len(keys) > 0 {
		s.mu.Lock()
		n := s.flushKeys(ctx, keys[:min(FlushChunk, len(keys))], true)
		s.mu.Unlock()
		keys = keys[n:]
		if len(keys) > 0 {
			if s.betweenChunks != nil {
				s.betweenChunks()
			}
			s.giveWay()
		}
	}
}

// giveWay lets the host's calls waiting for mu take it before the next
// chunk (a bounded number of yields, so a Flush always moves on).
func (s *Service) giveWay() {
	for i := 0; i < 64 && s.admitting.Load() > 0; i++ {
		runtime.Gosched()
	}
}

// FlushChunk is how many drops one hold of the lock deletes in a Flush.
const FlushChunk = 64

// flush deletes every waiting drop at once, the lock held throughout (the
// callers that already hold it: a retention pass and a restore).
func (s *Service) flush(ctx context.Context) { s.flushKeys(ctx, s.dropKeys(), false) }

// dropKeys are the waiting drops' keys in order (the lock held).
func (s *Service) dropKeys() []string {
	keys := make([]string, 0, len(s.drops))
	for k := range s.drops {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// flushKeys decides the drops named by keys (the lock held); a key no longer
// waiting is skipped. With giveWay it stops before the next key once a call
// of the host's waits for the lock (lockAdmission), having decided at least
// one. It answers how many keys it took.
func (s *Service) flushKeys(ctx context.Context, keys []string, giveWay bool) int {
	st := s.storage
	if st == nil || len(s.drops) == 0 || s.holdDrops {
		return len(keys)
	}
	del, ok := st.(Deleter)
	if !ok {
		s.warnOnce("delete", "ls_bstore cannot delete dropped outputs: the storage has no DeleteOutput")
		s.drops = map[string]*dropEntry{}
		return len(keys)
	}
	ev, canEvict := st.(Evicter)
	if !canEvict {
		s.warnOnce("evict", "ls_bstore cannot evict a dropped transaction: the storage has no DeleteAppliedTransaction")
	}
	ap, canAsk := st.(AppliedChecker)
	for i, k := range keys {
		if giveWay && i > 0 && s.admitting.Load() > 0 {
			return i
		}
		d := s.drops[k]
		if d == nil {
			continue // decided meanwhile
		}
		// Offered again and held again while the drop waited: nothing to drop.
		if s.heldByTx[d.b+"|"+d.txid][d.vout] {
			delete(s.drops, k)
			continue
		}
		h, err := chainhash.NewHashFromHex(d.txid)
		if err != nil {
			delete(s.drops, k)
			continue
		}
		if canAsk && time.Since(d.since) < SettleMs {
			applied, err := ap.DoesAppliedTransactionExist(ctx, h, d.topic)
			if err != nil {
				s.host.Log("ls_bstore could not drop an output", map[string]any{"txid": d.txid, "err": err.Error()})
				continue
			}
			if !applied {
				continue
			}
		}
		if err := del.DeleteOutput(ctx, &transaction.Outpoint{Txid: *h, Index: d.vout}, d.topic); err != nil {
			s.host.Log("ls_bstore could not drop an output", map[string]any{"txid": d.txid, "err": err.Error()})
			continue
		}
		// Another output of the transaction is still held: its record stays.
		if _, held := s.heldByTx[d.b+"|"+d.txid]; !held && canEvict {
			if err := ev.DeleteAppliedTransaction(ctx, h, d.topic); err != nil {
				s.host.Log("ls_bstore could not drop an output", map[string]any{"txid": d.txid, "err": err.Error()})
				continue
			}
		}
		delete(s.drops, k)
	}
	return len(keys)
}

// ---- admission

// OutputAdmittedByTopic implements module.LookupService: it indexes an
// admitted output, then reads back tokens whose predecessor is rooted now.
// What it drops is deleted by the next Flush, once the engine has recorded
// the admission.
func (s *Service) OutputAdmittedByTopic(ctx context.Context, p *bmodule.OutputAdmittedByTopic) error {
	if p.Mode != bmodule.AdmitWholeTx {
		return nil
	}
	b, ok := s.bucketOf(p.Topic)
	if !ok {
		return nil
	}
	tx, err := parse(p.AtomicBEEF)
	if err != nil {
		s.host.Log("ls_bstore ignored an admission whose BEEF did not parse", map[string]any{"topic": p.Topic})
		return nil
	}
	s.lockAdmission()
	defer s.mu.Unlock()
	s.index(b, tx, p.Outpoint.Index, s.nowMs())
	s.drain(ctx)
	return nil
}

// lockAdmission takes mu for a call the host makes while it holds the
// topic's lock: a Flush running gives way to it at its next drop.
func (s *Service) lockAdmission() {
	s.admitting.Add(1)
	s.mu.Lock()
	s.admitting.Add(-1)
}

// drain reads back the tokens a full seq turned away whose predecessor
// rooted since.
func (s *Service) drain(ctx context.Context) {
	if s.storage == nil {
		return
	}
	for len(s.rereads) > 0 {
		r := s.rereads[0]
		s.rereads = s.rereads[1:]
		if _, held := s.tokenChain[r.txid]; held {
			continue
		}
		h, err := chainhash.NewHashFromHex(r.txid)
		if err != nil {
			continue
		}
		o, err := s.storage.FindOutput(ctx, &transaction.Outpoint{Txid: *h, Index: r.vout}, storerec.RecordsPrefix+r.b)
		if err != nil || o == nil || o.Beef == nil {
			continue
		}
		tx, err := parse(o.Beef)
		if err != nil {
			s.host.Log("ls_bstore could not read a token again", map[string]any{"txid": r.txid, "err": err.Error()})
			continue
		}
		at := s.nowMs()
		if t, ok := s.tokenSeen.get(r.txid); ok {
			at = t.at
		}
		s.index(r.b, tx, r.vout, at)
	}
}

// OutputEvicted implements module.LookupService.
func (s *Service) OutputEvicted(_ context.Context, op *transaction.Outpoint) error {
	s.lockAdmission()
	defer s.mu.Unlock()
	s.forget(op.Txid.String(), op.Index, "")
	return nil
}

// OutputNoLongerRetainedInHistory implements module.RetentionListener.
func (s *Service) OutputNoLongerRetainedInHistory(_ context.Context, op *transaction.Outpoint, topic string) error {
	b, ok := s.bucketOf(topic)
	if !ok {
		return nil
	}
	s.lockAdmission()
	defer s.mu.Unlock()
	s.forget(op.Txid.String(), op.Index, b)
	return nil
}

// index indexes output vout of an admitted transaction, first seen at at;
// it answers whether it holds it now.
func (s *Service) index(b string, tx *transaction.Transaction, vout uint32, at int64) bool {
	if int(vout) >= len(tx.Outputs) || tx.Outputs[vout] == nil || tx.Outputs[vout].LockingScript == nil {
		return false
	}
	s.spends(b, tx)
	script := []byte(*tx.Outputs[vout].LockingScript)
	switch claim := storerec.Classify(script); claim {
	case storerec.ClaimEntry, storerec.ClaimOutcome, storerec.ClaimFront:
		return s.indexCarrier(b, tx, string(claim), script, at)
	case storerec.ClaimToken:
		return s.indexToken(b, tx, vout, at)
	case storerec.ClaimHold:
		return s.indexHold(b, tx, vout, at)
	case storerec.ClaimFunding:
		if vout == 0 {
			return s.indexSweep(b, tx, at)
		}
	}
	ad, err := storerec.DecodeAdvertisement(script)
	if err != nil {
		s.host.Log("ls_bstore indexed an output of no known kind", map[string]any{"bucket": b, "txid": tx.TxID().String(), "vout": vout})
		return false
	}
	return s.indexAd(b, tx, vout, ad, at)
}

// spends records what an admitted transaction spends among the outputs the
// index holds: an advertisement or hold output spent, a sweep's output
// spent.
func (s *Service) spends(b string, tx *transaction.Transaction) {
	for _, in := range tx.Inputs {
		src := srcTxid(in)
		if src == "" {
			continue
		}
		k := heldKey(b, src, in.SourceTxOutIndex)
		if s.spentHeld[k] {
			continue
		}
		ad, isAd := s.ads.get(k)
		if isAd {
			ad.spent = true
			s.adLive(ad, false)
		}
		h, isHold := s.holds.get(k)
		if isHold {
			h.spent = true
		}
		if isAd || isHold || (in.SourceTxOutIndex == 0 && s.sweeps.has(b+"|"+src)) {
			s.write(Line{K: "w", B: b, Txid: src, Vout: int64(in.SourceTxOutIndex)})
		}
	}
}

func (s *Service) indexCarrier(b string, tx *transaction.Transaction, kind string, script []byte, at int64) bool {
	if len(tx.Inputs) == 0 {
		return false
	}
	in := tx.Inputs[0]
	src := srcTxid(in)
	if src == "" {
		return false
	}
	txid := tx.TxID().String()
	c := hexOf(tx.TxID()[:])
	if s.carriers.has(c) {
		return true
	}
	raw, _ := pushdrop.FirstPush(script)
	op := outpoint(src, in.SourceTxOutIndex)
	first := at
	if sc, ok := s.seen.get(c); ok {
		first = sc.at
	}
	e, err := carrierOf(b, kind, raw, txid, c, op, first)
	if err != nil {
		s.host.Log("ls_bstore could not read a carrier", map[string]any{"bucket": b, "txid": txid, "err": err.Error()})
		return false
	}
	// A sweep that arrived before the carrier: the outpoint is marked now.
	if early, ok := s.table.get(op); ok && !s.marks.has(op) {
		s.write(Line{K: "x", Op: op, Key: e.owner, At: early.at, Life: s.isParty(b, e.owner), B: b})
	}
	// Retraction wins over every floor: a carrier on a marked outpoint is
	// retracted whenever it is offered.
	if mark, ok := s.marks.get(op); ok {
		if !mark.life && s.isParty(b, e.owner) {
			s.write(Line{K: "xl", Op: op})
		}
		s.queueDrop(b, txid, 0)
		s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": "retracted"}, 1)
		return false
	}
	if row, ok := s.rows.get(op); !ok {
		s.write(Line{K: "c", Op: op, C: c, At: e.at})
	} else if row.c != c {
		// A later carrier on the outpoint has no floor unless a served chain names it.
		if s.restoring {
			s.recheck = append(s.recheck, c)
		} else if !s.namedByServed(b, c) {
			s.queueDrop(b, txid, 0)
			s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": "second-carrier"}, 1)
			return false
		}
	}
	if !s.seen.has(c) {
		s.write(Line{K: "a", B: b, C: c, At: e.at})
	}
	s.holdCarrier(e)
	return true
}

// carrierOf is a carrier's index entry, read from its record.
func carrierOf(b, kind string, raw []byte, txid, c, op string, at int64) (*carrierEntry, error) {
	e := &carrierEntry{c: c, txid: txid, b: b, kind: kind, op: op, at: at}
	switch kind {
	case "entry":
		r, err := storerec.DecodeEntry(raw)
		if err != nil {
			return nil, err
		}
		e.owner, e.created, e.id, e.key, e.ekind = hexOf(r.Writer), r.Created, hexOf(r.ID[:]), r.Key, r.Kind
		if r.Kind == storerec.KindPut {
			e.root = hexOf(r.Root[:])
		}
		if r.Hold != nil {
			e.hold = outpoint(chainhash.Hash(r.Hold.Txid).String(), uint32(r.Hold.Vout)) //nolint:gosec // a hold's vout is below 2^32
		}
	case "outcome":
		r, err := storerec.DecodeOutcome(raw)
		if err != nil {
			return nil, err
		}
		e.owner, e.created = hexOf(r.Buyer), r.Created
		e.answerer, e.provider, e.oroot = hexOf(r.Req.Answerer), hexOf(r.Req.Provider), hexOf(r.Req.Root[:])
	default:
		r, err := storerec.DecodeFront(raw)
		if err != nil {
			return nil, err
		}
		e.owner, e.created, e.advertiser = hexOf(r.Front), r.Created, hexOf(r.Advertiser)
	}
	e.tkey = timeKey(e.created, txid)
	return e, nil
}

func (s *Service) holdCarrier(e *carrierEntry) {
	s.carriers.set(e.c, e)
	addTo(s.byOp, e.op, e.c)
	s.holdOutput(e.b, e.txid, 0)
	switch e.kind {
	case "entry":
		for _, sv := range s.servedFor(e.b, e.id) {
			if v := s.chains[sv.chain]; v == nil || v.named[e.c] == 0 {
				s.unnamedAdd(sv, e)
			}
		}
	case "outcome":
		free := s.nowMs()-e.at < OutcomeWindowDays*day
		for _, k := range outcomeKeys(e) {
			sortedIn(s.outcomeHistory, k).insert(e.tkey)
			if free {
				sortedIn(s.outcomesBy, k).insert(e.tkey)
			}
		}
		if free {
			for _, k := range outcomeBuyerKeys(e) {
				sortedIn(s.outcomesBy, k).insert(e.tkey)
			}
		}
	default:
		sortedIn(s.frontsByAdvertiser, e.b+"|"+e.advertiser).insert(e.txid)
		sortedIn(s.frontsByPair, e.b+"|"+e.advertiser+"|"+e.owner).insert(e.tkey)
	}
}

func outcomeKeys(e *carrierEntry) []string {
	return []string{e.b + "|a|" + e.answerer, e.b + "|p|" + e.provider}
}

func outcomeBuyerKeys(e *carrierEntry) []string {
	return []string{e.b + "|a|" + e.answerer + "|" + e.owner, e.b + "|p|" + e.provider + "|" + e.owner}
}

func (s *Service) forgetCarrier(e *carrierEntry) {
	s.carriers.del(e.c)
	removeFrom(s.byOp, e.op, e.c)
	s.releaseOutput(e.b, e.txid, 0)
	switch e.kind {
	case "entry":
		for _, sv := range s.servedFor(e.b, e.id) {
			s.unnamedRemove(sv, e)
		}
	case "outcome":
		for _, k := range outcomeKeys(e) {
			sortedOut(s.outcomeHistory, k, e.tkey)
			sortedOut(s.outcomesBy, k, e.tkey)
		}
		for _, k := range outcomeBuyerKeys(e) {
			sortedOut(s.outcomesBy, k, e.tkey)
		}
	default:
		sortedOut(s.frontsByAdvertiser, e.b+"|"+e.advertiser, e.txid)
		sortedOut(s.frontsByPair, e.b+"|"+e.advertiser+"|"+e.owner, e.tkey)
	}
}

// drop drops a held carrier: out of every answer, its output deleted and
// its admission evicted.
func (s *Service) drop(e *carrierEntry, why string) {
	s.forgetCarrier(e)
	if s.seen.has(e.c) {
		s.write(Line{K: "af", C: e.c})
	}
	s.queueDrop(e.b, e.txid, 0)
	s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": why}, 1)
	s.host.Log("ls_bstore dropped a carrier", map[string]any{"txid": e.txid, "outpoint": e.op, "kind": e.kind, "why": why})
}

// retract retracts a held carrier: its outpoint is marked, and the carrier
// goes.
func (s *Service) retract(e *carrierEntry, sweepAt int64) {
	// During a restore the chains are not folded yet, so a writer may not
	// count as a party here; the next retention pass makes the mark
	// lifelong once it does, well inside the shorter floor.
	if !s.marks.has(e.op) {
		s.write(Line{K: "x", Op: e.op, Key: e.owner, At: sweepAt, Life: s.isParty(e.b, e.owner), B: e.b})
	}
	s.host.Metrics().Inc("bstore_retractions_total", nil, 1)
	s.drop(e, "retracted")
}

func (s *Service) indexToken(b string, tx *transaction.Transaction, vout uint32, at int64) bool {
	txid := tx.TxID().String()
	tok, err := storerec.ReadToken(tx)
	if err != nil || tok.Vout != vout || tok.Bucket.Bucket != b {
		s.host.Log("ls_bstore could not read a token", map[string]any{"bucket": b, "txid": txid, "vout": vout})
		return false
	}
	rec := tok.Bucket
	owner, id := hexOf(rec.Owner), hexOf(rec.ID[:])
	key := b + "|" + owner + "|" + id
	v := s.chains[key]
	if v == nil {
		v = &chainIdx{key: key, b: b, owner: owner, id: id, served: s.served[key], bySeq: map[uint64][]*tokenEntry{}, byTxid: map[string]*tokenEntry{},
			children: map[string]map[string]bool{}, branches: map[string]*mapState{}, top: -1, turned: map[string]map[string]uint32{},
			rootedSeqs: newSeqIndex(), consistentSeqs: newSeqIndex(), named: map[string]int{}}
		s.chains[key] = v
	}
	if !s.tokenSeen.has(txid) {
		s.write(Line{K: "t", B: b, Txid: txid, Vout: int64(vout), At: at})
	}
	if _, held := v.byTxid[txid]; held {
		return true
	}
	var inputs []string
	first := false
	for _, in := range tx.Inputs {
		src := srcTxid(in)
		if src == "" {
			continue
		}
		inputs = append(inputs, outpoint(src, in.SourceTxOutIndex))
		if rec.Seq == 0 {
			if h, err := chainhash.NewHashFromHex(src); err == nil && storerec.BucketID([32]byte(*h), in.SourceTxOutIndex) == rec.ID {
				first = true
			}
		}
	}
	height, _, root := place(tx)
	e := &tokenEntry{txid: txid, vout: vout, b: b, chain: key, seq: rec.Seq, rec: rec, inputs: inputs, first: first, height: height, root: root, status: "pending", at: at}
	if t, ok := s.tokenSeen.get(txid); ok {
		e.at = t.at
	}
	if rec.Seq != 0 {
		e.prev = chainhash.Hash(rec.Prev).String()
	}
	list := append(append([]*tokenEntry(nil), v.bySeq[rec.Seq]...), e)
	sort.Slice(list, func(i, j int) bool { return list[i].txid < list[j].txid })
	v.bySeq[rec.Seq] = list
	v.seqs.insert(rec.Seq)
	v.byTxid[txid] = e
	s.tokenChain[txid] = key
	s.holdOutput(b, txid, vout)
	if e.prev != "" {
		addTo(v.children, e.prev, txid)
	}
	if v.served {
		for _, c := range namesOf(rec) {
			s.name(v, c, 1)
		}
	}
	if s.restoring {
		return true
	}
	s.rootFrom(v, e, true)
	if !e.rooted {
		s.counted(v, e)
	}
	s.bound(v, rec.Seq)
	_, held := v.byTxid[txid]
	return held
}

func (s *Service) forgetToken(v *chainIdx, t *tokenEntry, reroot bool) {
	s.moved(v)
	if t.rooted {
		v.rootedSeqs.remove(t.seq)
	}
	if t.status == "consistent" {
		v.consistentSeqs.remove(t.seq)
	}
	delete(v.byTxid, t.txid)
	delete(s.tokenChain, t.txid)
	s.releaseOutput(v.b, t.txid, t.vout)
	var list []*tokenEntry
	for _, x := range v.bySeq[t.seq] {
		if x.txid != t.txid {
			list = append(list, x)
		}
	}
	if len(list) == 0 {
		delete(v.bySeq, t.seq)
		v.seqs.remove(t.seq)
	} else {
		v.bySeq[t.seq] = list
	}
	if t.prev != "" {
		removeFrom(v.children, t.prev, t.txid)
	}
	if v.served {
		for _, c := range namesOf(t.rec) {
			s.name(v, c, -1)
		}
	}
	if reroot && t.rooted {
		s.reroot(v)
	}
}

// minedOutput registers a mined output of bucket b (an advertisement, a
// hold output, a sweep's output 0), first seen at at; it answers the first
// sight.
func (s *Service) minedOutput(b, txid string, vout uint32, at int64) int64 {
	k := heldKey(b, txid, vout)
	if seen, ok := s.minedSeen.get(k); ok {
		return seen.at
	}
	s.write(Line{K: "o", B: b, Txid: txid, Vout: int64(vout), At: at})
	return at
}

func (s *Service) indexHold(b string, tx *transaction.Transaction, vout uint32, at int64) bool {
	txid := tx.TxID().String()
	k := heldKey(b, txid, vout)
	if s.holds.has(k) {
		return true
	}
	s.holds.set(k, &holdEntry{b: b, txid: txid, vout: vout, at: s.minedOutput(b, txid, vout, at), spent: s.spentHeld[k]})
	s.holdOutput(b, txid, vout)
	return true
}

func (s *Service) indexAd(b string, tx *transaction.Transaction, vout uint32, ad *storerec.Advertisement, at int64) bool {
	txid := tx.TxID().String()
	k := heldKey(b, txid, vout)
	if s.ads.has(k) {
		return true
	}
	e := &adEntry{b: b, txid: txid, vout: vout, root: hexOf(ad.Hash[:]), advertiser: hexOf(ad.Host), expiry: ad.Expiry, at: s.minedOutput(b, txid, vout, at),
		spent: s.spentHeld[k], anyone: string(ad.Host) == string(storerec.AnyoneKey)}
	s.ads.set(k, e)
	s.holdOutput(b, txid, vout)
	s.adLive(e, true)
	return true
}

// adLive puts an advertisement in the indexes ads and ad answer from, or
// takes it out: only one unspent, not of the anyone key, unexpired at the
// last look.
func (s *Service) adLive(e *adEntry, live bool) {
	k := outKey(e.txid, e.vout)
	if live && !e.spent && !e.anyone && int64(e.expiry)*1000 > s.nowMs() { //nolint:gosec // an expiry is a 32-bit time
		sortedIn(s.adsLive, e.b+"|"+e.root).insert(k)
		sortedIn(s.adsLive, e.b+"|"+e.root+"|"+e.advertiser).insert(k)
		return
	}
	sortedOut(s.adsLive, e.b+"|"+e.root, k)
	sortedOut(s.adsLive, e.b+"|"+e.root+"|"+e.advertiser, k)
}

// indexSweep indexes a sweep (spec sections 12.1 and 12.4): its input
// outpoints are kept, once per host whatever bucket admitted it, and every
// carrier the host holds on one of them is retracted. The sweep's own
// transaction is kept only where an input matched a carrier of this
// bucket; otherwise it is dropped at once, and its outpoints still retract
// a carrier that comes later.
func (s *Service) indexSweep(b string, tx *transaction.Transaction, at int64) bool {
	txid := tx.TxID().String()
	k := b + "|" + txid
	if s.sweeps.has(k) {
		return true
	}
	var spent []string
	matched := false
	for _, in := range tx.Inputs {
		src := srcTxid(in)
		if src == "" {
			continue
		}
		op := outpoint(src, in.SourceTxOutIndex)
		spent = append(spent, op)
		if !s.table.has(op) {
			s.write(Line{K: "p", Op: op, S: txid, At: at})
		}
		// Every carrier on the outpoint is retracted, in whatever bucket; the
		// sweep is kept in this bucket only where it spends the outpoint of
		// a carrier of this bucket: one held now, or one a mark says was
		// retracted (by this sweep offered to another bucket first, or
		// before a restart).
		cs := make([]string, 0, len(s.byOp[op]))
		for c := range s.byOp[op] {
			cs = append(cs, c)
		}
		sort.Strings(cs)
		for _, c := range cs {
			e, _ := s.carriers.get(c)
			if e == nil {
				continue
			}
			matched = matched || e.b == b
			p, _ := s.table.get(op)
			s.retract(e, p.at)
		}
		if m, ok := s.marks.get(op); ok && m.b == b {
			matched = true
		}
	}
	if !matched {
		s.queueDrop(b, txid, 0)
		s.host.Metrics().Inc("bstore_dropped_total", map[string]string{"why": "unmatched-sweep"}, 1)
		return false
	}
	height, offset, _ := place(tx)
	s.sweeps.set(k, &sweepEntry{b: b, txid: txid, spent: spent, height: height, offset: offset, at: s.minedOutput(b, txid, 0, at)})
	for _, op := range spent {
		addTo(s.sweepsByOp, b+"|"+op, txid)
	}
	s.holdOutput(b, txid, 0)
	return true
}

func (s *Service) forgetSweep(sw *sweepEntry) {
	s.sweeps.del(sw.b + "|" + sw.txid)
	for _, op := range sw.spent {
		removeFrom(s.sweepsByOp, sw.b+"|"+op, sw.txid)
	}
	s.releaseOutput(sw.b, sw.txid, 0)
}

// forget removes a held output from the index, in one bucket or in all
// ("" for all); the journal's rows, marks and outpoints stay.
func (s *Service) forget(txid string, vout uint32, b string) {
	var bks []string
	if b == "" {
		for bk := range s.buckets {
			bks = append(bks, bk)
		}
		sort.Strings(bks)
	} else {
		bks = []string{b}
	}
	for _, bk := range bks {
		k := heldKey(bk, txid, vout)
		if ad, ok := s.ads.get(k); ok {
			s.adLive(ad, false)
			s.ads.del(k)
			s.releaseOutput(bk, txid, vout)
		}
		if s.holds.has(k) {
			s.holds.del(k)
			s.releaseOutput(bk, txid, vout)
		}
		if vout == 0 {
			if sw, ok := s.sweeps.get(bk + "|" + txid); ok {
				s.forgetSweep(sw)
			}
		}
	}
	if vout == 0 {
		if e, ok := s.carriers.get(cOf(txid)); ok && (b == "" || e.b == b) {
			s.forgetCarrier(e)
		}
	}
	if key, ok := s.tokenChain[txid]; ok {
		v := s.chains[key]
		if t := v.byTxid[txid]; t != nil && t.vout == vout && (b == "" || v.b == b) {
			s.forgetToken(v, t, true)
		}
	}
}

func namesOf(rec *storerec.Bucket) []string {
	out := make([]string, 0, len(rec.Changes)+len(rec.Consumed))
	for _, ch := range rec.Changes {
		out = append(out, hexOf(ch.C[:]))
	}
	for _, c := range rec.Consumed {
		out = append(out, hexOf(c[:]))
	}
	return out
}
