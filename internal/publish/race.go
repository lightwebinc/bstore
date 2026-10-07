package publish

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/storerec"
)

// The hold race, judged by the writer's own client (spec section 9.4, T2):
// it watches the hosts for eligible advertisements of the entry's root,
// records when it first sees each (its own clock, on an advertisement it
// verified mined against its own headers and held to section 9.3's
// eligibility), ranks them by that time with ties to the lower tie key
// (storerec.TieKey: the advertisement outpoint with the entry's
// commitment), and
// challenges each candidate in rank order with a settlement challenge of
// min(checkC, freeC) samples, within the candidate's terms (freeC, minAge,
// minBounty), answered free. A candidate that does not pass is skipped. The
// race closes when K distinct advertisers have passed, when the writer's
// window ends, or at the deadline; settlement then pays by rank (settle.go).
//
// A race is recorded under races/<entry txid>.json, written before
// anything it decides leaves the process.

// Race states.
const (
	RaceOpen     = "open"     // watching and judging
	RaceClosed   = "closed"   // the window ended with fewer than t passed: nothing is paid
	RaceSettling = "settling" // a settlement is signed and recorded, not yet seen mined
	RaceSettled  = "settled"  // the settlement mined
	RaceRemitted = "remitted" // the settlement taken by the network and remitted, not yet seen mined
	RaceLapsed   = "lapsed"   // the deadline came with nothing settled
	RaceSpent    = "spent"    // the hold output was spent by something this race did not make
	RaceReclaim  = "reclaiming"
	RaceReclaimd = "reclaimed"
)

// Candidate statuses.
const (
	CandSeen      = "seen"      // eligible, not yet challenged
	CandWaiting   = "waiting"   // the hold output is not yet minAge deep for this provider
	CandPriced    = "priced"    // the provider would not answer free and the budget did not cover it
	CandPassed    = "passed"    // passed its settlement challenge
	CandFailed    = "failed"    // did not pass: skipped
	CandCancelled = "cancelled" // passed, then its advertisement was spent by something other than a renewal
)

// Race is one entry's hold race.
type Race struct {
	Entry  string `json:"entry"` // txid, display order
	Bucket string `json:"bucket"`
	Record string `json:"record"` // the entry's encoding, hex
	// Hold is the hold output, "txid.vout"; HoldHeight the block it mined
	// in, when known.
	Hold       string `json:"hold"`
	HoldHeight uint32 `json:"holdHeight,omitempty"`
	Started    int64  `json:"started"`
	// RootLen is the root node's length, which an eligible advertisement's
	// fileSize must equal.
	RootLen uint64 `json:"rootLen,omitempty"`
	State   string `json:"state"`
	// Closing is set when the race has closed for settlement (the window
	// ended with at least t passed) and is waiting to be settled.
	Closing    bool          `json:"closing,omitempty"`
	Cands      []*Cand       `json:"candidates,omitempty"`
	Settlement *SettleRecord `json:"settlement,omitempty"`
	// Abandoned are settlements recorded that could never mine (a fee
	// input spent elsewhere), kept for the record.
	Abandoned []SettleRecord  `json:"abandoned,omitempty"`
	Reclaim   *ReclaimRecord  `json:"reclaim,omitempty"`
	Note      string          `json:"note,omitempty"`
	Outcomes  []PublishedNote `json:"outcomes,omitempty"`

	entry *storerec.Entry
}

// PublishedNote is an outcome record the race published.
type PublishedNote struct {
	Advertiser string `json:"advertiser"`
	Outcome    string `json:"outcome"`
	Txid       string `json:"txid"`
}

// Cand is one advertiser in the race.
type Cand struct {
	Advertiser string `json:"advertiser"` // 66 hex
	// First is the advertisement first seen (display txid and vout): the
	// rank keeps its first sight, and settlement checks it, or its renewal
	// successor, unspent.
	First       string `json:"first"`
	FirstVout   uint32 `json:"firstVout"`
	FirstExpiry uint64 `json:"firstExpiry"`
	FirstSeen   int64  `json:"firstSeen"` // Unix nanoseconds, the writer's clock
	// URL and Expiry are the live advertisement's with the latest expiry:
	// the tested URL (spec section 7.5).
	URL      string    `json:"url"`
	Expiry   uint64    `json:"expiry"`
	Status   string    `json:"status"`
	Attempts []Attempt `json:"attempts,omitempty"`
	// DrawLog is what became of each draw, apart from Attempts: progress
	// notes ("waiting" every step, "priced", "not asked") go to Attempts
	// only, so they never push out the reason a draw was spent.
	DrawLog []Attempt `json:"drawLog,omitempty"`
	// Draws are the settlement challenges spent; Pending is the one made
	// and not yet answered; BadFronts are fronts not asked again.
	Draws     int          `json:"draws,omitempty"`
	Pending   *PendingDraw `json:"pending,omitempty"`
	BadFronts []string     `json:"badFronts,omitempty"`
	// Front and Answerer are where it passed.
	Front    string `json:"front,omitempty"`
	Answerer string `json:"answerer,omitempty"`
	Paid     uint64 `json:"paid,omitempty"`
}

// Attempt is one settlement challenge of a candidate.
type Attempt struct {
	At      int64  `json:"at"`
	C       uint64 `json:"c"`
	Front   string `json:"front"`
	Outcome string `json:"outcome"`
	Why     string `json:"why,omitempty"`
	Paid    uint64 `json:"paid,omitempty"`
}

// RaceOptions are the writer's policy for a race step.
type RaceOptions struct {
	// Window is the writer's race window from the race's start; zero closes
	// the race only at K passes or the deadline.
	Window time.Duration
	// Budget is the most paid for one settlement challenge a provider does
	// not answer free; zero pays nothing.
	Budget uint64
	// Fetch reaches advertised URLs for by-reference samples.
	Fetch *http.Client
	// Publish publishes failed challenges as outcome records.
	Publish bool
}

func (p *Publisher) racesDir() string { return filepath.Join(p.cfg.Dir, "races") }

func (p *Publisher) saveRace(r *Race) error {
	if err := os.MkdirAll(p.racesDir(), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		return err
	}
	return writeFileSync(filepath.Join(p.cfg.Dir, "tmp"), filepath.Join(p.racesDir(), r.Entry+".json"), raw)
}

// Races are this identity's races, oldest first.
func (p *Publisher) Races() ([]*Race, error) {
	ents, err := os.ReadDir(p.racesDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []*Race
	for _, d := range ents {
		if !strings.HasSuffix(d.Name(), ".json") {
			continue
		}
		r, err := p.Race(strings.TrimSuffix(d.Name(), ".json"))
		if err != nil {
			p.cfg.Logf("race %s: %v", d.Name(), err)
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Started != out[j].Started {
			return out[i].Started < out[j].Started
		}
		return out[i].Entry < out[j].Entry
	})
	return out, nil
}

// Race is the race of one entry.
func (p *Publisher) Race(entry string) (*Race, error) {
	if !isHex64(entry) {
		return nil, errors.New("publish: an entry is named by its txid, 64 lowercase hex digits")
	}
	raw, err := os.ReadFile(filepath.Join(p.racesDir(), entry+".json"))
	if err != nil {
		return nil, err
	}
	var r Race
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	if r.Entry != entry {
		return nil, errors.New("publish: a race file names another entry")
	}
	if err := r.decode(); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *Race) decode() error {
	rec, err := hex.DecodeString(r.Record)
	if err != nil {
		return err
	}
	e, err := storerec.DecodeEntry(rec)
	if err != nil {
		return err
	}
	if e.Kind != storerec.KindPut || e.Hold == nil {
		return errors.New("publish: a race of an entry without hold terms")
	}
	r.entry = e
	return nil
}

// EntryOf is the race's entry.
func (r *Race) EntryOf() *storerec.Entry { return r.entry }

// C is the entry's commitment, hash byte order.
func (r *Race) C() [32]byte {
	c, _ := parseDisplay(r.Entry)
	return c
}

// Passed are the candidates that passed, in rank order.
func (r *Race) Passed() []*Cand {
	var out []*Cand
	for _, c := range r.Ranked() {
		if c.Status == CandPassed {
			out = append(out, c)
		}
	}
	return out
}

// Ranked are the candidates in the writer's ranking: by first sight, ties
// to the lower tie key of the advertisement first seen (spec section 9.4,
// storerec.TieKey).
func (r *Race) Ranked() []*Cand {
	out := slices.Clone(r.Cands)
	keys := make(map[*Cand][32]byte, len(out))
	for _, c := range out {
		keys[c] = r.tieKey(c.First, c.FirstVout)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FirstSeen != out[j].FirstSeen {
			return out[i].FirstSeen < out[j].FirstSeen
		}
		ki, kj := keys[out[i]], keys[out[j]]
		return bytes.Compare(ki[:], kj[:]) < 0
	})
	return out
}

// tieKey is storerec.TieKey of an advertisement outpoint (display txid)
// for the race's entry.
func (r *Race) tieKey(txid string, vout uint32) [32]byte {
	h, _ := parseDisplay(txid)
	return storerec.TieKey(h, vout, r.C())
}

// Open reports whether the race still watches and judges.
func (r *Race) Open() bool { return r.State == RaceOpen }

// registerRace records an entry's race before the entry is sent.
func (p *Publisher) registerRace(e *storerec.Entry, rec []byte, txid string, hold *HoldRecord) error {
	r := &Race{Entry: txid, Bucket: e.Bucket, Record: hex.EncodeToString(rec), Hold: fmt.Sprintf("%s.%d", hold.Txid, hold.Vout),
		HoldHeight: hold.Height, Started: p.cfg.Now().Unix(), State: RaceOpen, entry: e}
	if rb, err := p.cfg.Store.Get(e.Root); err == nil {
		r.RootLen = uint64(len(rb))
	}
	return p.saveRace(r)
}

// holdOf is the hold record a race's hold output is.
func (p *Publisher) holdOf(r *Race) (*HoldRecord, error) {
	txid, vout, ok := strings.Cut(r.Hold, ".")
	v, err := strconv.ParseUint(vout, 10, 32)
	if !ok || err != nil {
		return nil, errors.New("publish: a race names no hold outpoint")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Holds {
		h := p.st.Holds[i]
		if h.Txid == txid && h.Vout == uint32(v) {
			return &h, nil
		}
	}
	return nil, fmt.Errorf("publish: hold output %s is not this identity's", r.Hold)
}

// Step is one step of a race: it watches the hosts for advertisements,
// challenges the candidates that have not passed or failed, and closes the
// race when K have passed, the window has ended or the deadline has come.
// It saves the race before it returns.
func (p *Publisher) Step(ctx context.Context, r *Race, o RaceOptions) error {
	if !r.Open() {
		return nil
	}
	e := r.entry
	tip, err := p.cfg.Asset.BestHeader(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNoTip, err)
	}
	if uint64(tip.Height) >= e.Hold.Deadline {
		r.State, r.Note = RaceLapsed, fmt.Sprintf("the deadline %d came at tip %d with nothing settled", e.Hold.Deadline, tip.Height)
		return p.saveRace(r)
	}
	if r.HoldHeight == 0 {
		if h, err := p.holdOf(r); err == nil {
			r.HoldHeight = h.Height
		}
	}
	if err := p.watch(ctx, r); err != nil {
		p.cfg.Logf("race %s: %v", short(r.Entry), err)
	}
	if err := p.saveRace(r); err != nil {
		return err
	}
	if err := p.judge(ctx, r, tip.Height, o); err != nil {
		return err
	}
	passed := len(r.Passed())
	switch {
	case uint64(passed) >= e.Hold.K:
		r.Note = fmt.Sprintf("closed: %d advertisers passed (K = %d)", passed, e.Hold.K)
	case o.Window > 0 && p.cfg.Now().Sub(time.Unix(r.Started, 0)) >= o.Window:
		if uint64(passed) < e.Hold.T {
			r.State = RaceClosed
			r.Note = fmt.Sprintf("the window ended with %d passed, fewer than t = %d: nothing is paid", passed, e.Hold.T)
		} else {
			r.Closing = true
			r.Note = fmt.Sprintf("closed: the window ended with %d passed", passed)
		}
	}
	return p.saveRace(r)
}

// Closed reports whether the race is ready to settle: K passed, or the
// window ended with at least t.
func (r *Race) Closed(now time.Time, window time.Duration) bool {
	passed := uint64(len(r.Passed()))
	if passed >= r.entry.Hold.K || (r.Closing && passed >= r.entry.Hold.T) {
		return true
	}
	return window > 0 && now.Sub(time.Unix(r.Started, 0)) >= window && passed >= r.entry.Hold.T
}

// watch asks every host for the root's advertisements, every page, and
// records each eligible advertiser's first sight.
func (p *Publisher) watch(ctx context.Context, r *Race) error {
	rd := p.cfg.Reader
	if rd == nil {
		return errors.New("no hosts to watch")
	}
	e := r.entry
	if r.RootLen == 0 {
		if n, err := p.rootLen(ctx, e); err == nil {
			r.RootLen = n
		}
	}
	root := hex.EncodeToString(e.Root[:])
	// One sighting time per watch: advertisements first seen in the same
	// look tie on time, and the rank breaks the tie by the lower tie key.
	now := p.cfg.Now().UnixNano()
	var errs []string
	for _, h := range rd.Hosts {
		after := ""
		for page := 0; page < 64; page++ {
			a, err := rd.Ask(ctx, h, map[string]string{"bucket": r.Bucket, "root": root, "after": after})
			if err != nil {
				errs = append(errs, fmt.Sprintf("host %s: ads: %v", h, err))
				break
			}
			last := ""
			if len(a.Outputs) > 64 {
				errs = append(errs, fmt.Sprintf("host %s: %d outputs to one ads page", h, len(a.Outputs)))
				break
			}
			for _, out := range a.Outputs {
				tx, err := BEEFSubject(out.Beef, 1<<20)
				if err != nil {
					continue
				}
				txid := tx.TxID().String()
				if last != "" && txid <= last {
					errs = append(errs, fmt.Sprintf("host %s: ads out of order", h))
					break
				}
				last = txid
				if int(out.OutputIndex) >= len(tx.Outputs) || tx.Outputs[out.OutputIndex].LockingScript == nil {
					continue
				}
				ad, err := storerec.DecodeAdvertisement(*tx.Outputs[out.OutputIndex].LockingScript)
				if err != nil || r.RootLen == 0 || storerec.Eligibility(ad, e, r.RootLen) != "" {
					continue
				}
				if ad.Expiry <= uint64(p.cfg.Now().Unix()) || !strings.HasPrefix(ad.URL, "https://") {
					continue
				}
				if !chaintoken.Mined(ctx, tx, rd.Headers) {
					continue
				}
				p.sight(r, ad, txid, out.OutputIndex, now)
			}
			if len(a.Outputs) < 64 || last == "" || last <= after {
				break
			}
			after = last
		}
	}
	if len(errs) > 0 && len(errs) == len(rd.Hosts) {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// sight records an eligible advertisement: a new advertiser at the
// writer's clock now; a known one keeps its first sight and takes the
// advertisement with the latest expiry as its tested URL.
func (p *Publisher) sight(r *Race, ad *storerec.Advertisement, txid string, vout uint32, now int64) {
	k := hex.EncodeToString(ad.Host)
	for _, c := range r.Cands {
		if c.Advertiser != k {
			continue
		}
		if ad.Expiry > c.Expiry {
			c.URL, c.Expiry = ad.URL, ad.Expiry
		}
		// The same advertiser's other advertisement, seen in the same
		// look, keeps the rank of the lower tie key: an advertiser is
		// counted once, at its first place.
		if now == c.FirstSeen && r.tieLess(txid, vout, c.First, c.FirstVout) {
			c.First, c.FirstVout, c.FirstExpiry = txid, vout, ad.Expiry
		}
		return
	}
	r.Cands = append(r.Cands, &Cand{Advertiser: k, First: txid, FirstVout: vout, FirstExpiry: ad.Expiry, FirstSeen: now,
		URL: ad.URL, Expiry: ad.Expiry, Status: CandSeen})
	p.cfg.Logf("race %s: advertiser %s seen (%s)", short(r.Entry), short(k), short(txid))
}

func (r *Race) tieLess(txid string, vout uint32, other string, otherVout uint32) bool {
	a, b := r.tieKey(txid, vout), r.tieKey(other, otherVout)
	return bytes.Compare(a[:], b[:]) < 0
}

// rootLen is the root node's length: from the local store, else from an
// advertiser, held to the root's hash.
func (p *Publisher) rootLen(ctx context.Context, e *storerec.Entry) (uint64, error) {
	if b, err := p.cfg.Store.Get(e.Root); err == nil {
		return uint64(len(b)), nil
	}
	if p.cfg.Reader == nil {
		return 0, errors.New("no reader")
	}
	o, err := p.cfg.Reader.Open(ctx, e, p.cfg.Store)
	if err != nil {
		return 0, err
	}
	b, err := o.Object(ctx, e.Root)
	if err != nil {
		return 0, err
	}
	_, _ = p.cfg.Store.Put(b)
	return uint64(len(b)), nil
}

// providerTerms is the challenge half of a provider's terms document.
type providerTerms struct {
	FreeC     uint64
	MinAge    uint64
	MinBounty uint64
	Known     bool
}

func (p *Publisher) terms(ctx context.Context, front string) providerTerms {
	t := providerTerms{FreeC: storerec.MinFreeC}
	call, err := p.Get(ctx, strings.TrimRight(front, "/")+"/terms")
	if err != nil || call.Status != http.StatusOK {
		return t
	}
	var doc struct {
		Service   string `json:"service"`
		Challenge struct {
			FreeC     *uint64 `json:"freeC"`
			MinAge    *uint64 `json:"minAge"`
			MinBounty *uint64 `json:"minBounty"`
		} `json:"challenge"`
	}
	if json.Unmarshal(call.Body, &doc) != nil || doc.Service != "bstore" {
		return t
	}
	t.Known = true
	if v := doc.Challenge.FreeC; v != nil && *v >= storerec.MinFreeC && *v <= storerec.MaxC {
		t.FreeC = *v
	}
	if v := doc.Challenge.MinAge; v != nil && *v <= storerec.MaxMinAge {
		t.MinAge = *v
	}
	if v := doc.Challenge.MinBounty; v != nil {
		t.MinBounty = *v
	}
	return t
}

// fronts are where a candidate is challenged: its derived front, then the
// fronts that state they answer for it (spec sections 8.2 and 10.1), each
// statement verified as a carrier of the bucket.
func (p *Publisher) fronts(ctx context.Context, r *Race, c *Cand) []frontAt {
	adv, _ := hex.DecodeString(c.Advertiser)
	var out []frontAt
	bad := func(u string) bool { return slices.Contains(c.BadFronts, u) }
	if f, ok := storerec.DerivedFront(c.URL); ok && !bad(f) {
		out = append(out, frontAt{url: f, answerer: adv})
	}
	rd := p.cfg.Reader
	if rd == nil {
		return out
	}
	seen := map[string]bool{}
	for _, h := range rd.Hosts {
		if len(out) >= maxFronts {
			break
		}
		a, err := rd.Ask(ctx, h, map[string]string{"bucket": r.Bucket, "advertiser": c.Advertiser, "after": ""})
		if err != nil || len(a.Outputs) > 8 {
			continue
		}
		for _, o := range a.Outputs {
			if len(out) >= maxFronts {
				break
			}
			ad, err := storerec.Admit(ctx, o.Beef, nil, storerec.Host{Bucket: r.Bucket, Headers: rd.Headers})
			if err != nil || ad.Carrier == nil || ad.Carrier.Front == nil {
				continue
			}
			f := ad.Carrier.Front
			if f.Bucket != r.Bucket || !bytes.Equal(f.Advertiser, adv) || seen[f.URL+hex.EncodeToString(f.Front)] || bad(f.URL) {
				continue
			}
			seen[f.URL+hex.EncodeToString(f.Front)] = true
			out = append(out, frontAt{url: f.URL, answerer: f.Front})
		}
	}
	return out
}

// derived reports whether f is c's derived front (spec section 9.4): the
// front its advertisement's URL names, answering for the advertiser.
func derived(c *Cand, f frontAt) bool {
	u, ok := storerec.DerivedFront(c.URL)
	return ok && u == f.url
}

// OutcomeNotADraw is a candidate's note that a challenge did not count as
// a draw (row C31).
const OutcomeNotADraw = "not a draw"

// IsDrawOutcome reports whether an attempt's outcome says what became of a
// draw (an answer, a spent or uncounted draw, the candidate failing), as
// against a progress note: "waiting", "priced" and "not asked" spend
// nothing.
func IsDrawOutcome(o string) bool {
	switch o {
	case "waiting", "priced", "not asked", "":
		return false
	}
	return true
}

type frontAt struct {
	url      string
	answerer []byte
}

// judge challenges, in rank order, every candidate still to be judged,
// until K have passed. The candidates are taken in waves: as many of the
// best ranked still to be judged as passes are missing, challenged at
// once; a wave whose candidates do not all pass is followed by the next.
// No candidate is challenged while K better ranked ones could still pass,
// so the race pays no more challenges than one at a time would; it only
// waits less.
func (p *Publisher) judge(ctx context.Context, r *Race, tip uint32, o RaceOptions) error {
	e := r.entry
	var blobs []chirp.Child
	tried := map[*Cand]bool{}
	for {
		need := int(e.Hold.K) - len(r.Passed())
		if need <= 0 {
			return nil
		}
		var wave []*Cand
		for _, c := range r.Ranked() {
			switch c.Status {
			case CandSeen, CandWaiting, CandPriced:
			default:
				continue
			}
			if tried[c] {
				continue
			}
			wave = append(wave, c)
			if len(wave) == need {
				break
			}
		}
		if len(wave) == 0 {
			return nil
		}
		if blobs == nil {
			b, err := p.closureBlobs(ctx, e)
			if err != nil {
				return fmt.Errorf("race %s: the closure's blobs: %w", short(r.Entry), err)
			}
			blobs = b
		}
		var mu sync.Mutex
		var wg sync.WaitGroup
		var saveErr error
		for _, c := range wave {
			tried[c] = true
			wg.Add(1)
			go func(orig *Cand) {
				defer wg.Done()
				// The candidate is judged on a copy; the race is touched,
				// and saved, only under mu.
				mine := orig.clone()
				sync := func() error {
					mu.Lock()
					defer mu.Unlock()
					*orig = *mine.clone()
					return p.saveRace(r)
				}
				outcome := func(n PublishedNote) {
					mu.Lock()
					r.Outcomes = append(r.Outcomes, n)
					mu.Unlock()
				}
				p.challengeCand(ctx, r, mine, blobs, tip, o, sync, outcome)
				if err := sync(); err != nil {
					mu.Lock()
					saveErr = err
					mu.Unlock()
				}
			}(c)
		}
		wg.Wait()
		if saveErr != nil {
			return saveErr
		}
	}
}

// clone is a deep copy of a candidate.
func (c *Cand) clone() *Cand {
	d := *c
	d.Attempts = slices.Clone(c.Attempts)
	d.DrawLog = slices.Clone(c.DrawLog)
	d.BadFronts = slices.Clone(c.BadFronts)
	if c.Pending != nil {
		pd := *c.Pending
		d.Pending = &pd
	}
	return &d
}

// closureBlobs are the entry's blob references: from the local store's
// manifest, else from the closure as its advertisers serve it, verified.
func (p *Publisher) closureBlobs(ctx context.Context, e *storerec.Entry) ([]chirp.Child, error) {
	if m, err := p.cfg.Store.Manifest(e.Root); err == nil {
		var out []chirp.Child
		for i, b := range m.Blobs {
			h, err := parseHash(b)
			if err != nil {
				return nil, err
			}
			n := uint64(storerec.BlobSize)
			if uint64(i) == storerec.Blobs(e.Size)-1 {
				n = e.Size - uint64(i)*storerec.BlobSize
			}
			out = append(out, chirp.Child{Kind: chirp.ChildBlob, Length: n, Hash: h})
		}
		if uint64(len(out)) == storerec.Blobs(e.Size) {
			return out, nil
		}
	}
	if p.cfg.Reader == nil {
		return nil, errors.New("no reader")
	}
	o, err := p.cfg.Reader.Open(ctx, e, p.cfg.Store)
	if err != nil {
		return nil, err
	}
	return o.Blobs(), nil
}

// The settlement challenge's draws. A draw is one request: a nonce and a
// seed, bound to one front. A candidate gets maxDraws of them over the
// whole race, every front counted: a provider that could choose which
// draws it answers (refusing those that sample what it lacks, and waiting
// for a fresh nonce) would pass with a fraction of the bytes. A draw is
// spent by any answer the front gives to the request (a signed response, or
// a refusal of the request itself); one not answered (a timeout, a 5xx, a
// price) is sent again as it is, the same nonce, until its seed leaves the
// answerer's window or pendingTries sends have failed, and is then spent
// too. So refusing a draw gains a provider nothing but delay.
const (
	maxDraws     = 2
	maxFronts    = 4
	pendingTries = 3
	// maxNotes and maxDrawLog bound a candidate's Attempts and DrawLog.
	maxNotes   = 32
	maxDrawLog = 16
	// candBudget bounds the time one candidate takes in one step.
	candBudget = 4 * time.Minute
)

// PendingDraw is a request made and not yet answered.
type PendingDraw struct {
	Request  string `json:"request"` // hex
	Front    string `json:"front"`
	Answerer string `json:"answerer"`
	Tries    int    `json:"tries"`
}

// challengeCand gives a candidate its next draw. A front that answers the
// draw with no challenge route (404, 410) has answered it: the draw counts
// (row C23: only no response, a server error, a price or a seed refusal
// leaves a draw unanswered), the front is not asked again, and the next
// draw goes at once to the next front, within the same step. save records
// the candidate in the race, durably; outcome notes an outcome record
// published.
func (p *Publisher) challengeCand(ctx context.Context, r *Race, c *Cand, blobs []chirp.Child, tip uint32, o RaceOptions,
	save func() error, outcome func(PublishedNote)) {
	ctx, cancel := context.WithTimeout(ctx, candBudget)
	defer cancel()
	for range maxDraws + maxFronts {
		if !p.drawOnce(ctx, r, c, blobs, tip, o, save, outcome) {
			return
		}
	}
}

// drawOnce makes or resends one draw and reports whether the next front
// should be asked at once.
func (p *Publisher) drawOnce(ctx context.Context, r *Race, c *Cand, blobs []chirp.Child, tip uint32, o RaceOptions,
	save func() error, outcome func(PublishedNote)) bool {
	e := r.entry
	adv, _ := hex.DecodeString(c.Advertiser)
	note := func(a Attempt) {
		a.At = p.cfg.Now().Unix()
		c.Attempts = append(c.Attempts, a)
		if len(c.Attempts) > maxNotes {
			c.Attempts = c.Attempts[len(c.Attempts)-maxNotes:]
		}
		if IsDrawOutcome(a.Outcome) {
			c.DrawLog = append(c.DrawLog, a)
			if len(c.DrawLog) > maxDrawLog {
				c.DrawLog = c.DrawLog[len(c.DrawLog)-maxDrawLog:]
			}
		}
	}
	spend := func(why string) {
		c.Draws++
		c.Pending = nil
		note(Attempt{Outcome: "spent", Why: why})
	}
	if pd := c.Pending; pd != nil {
		raw, _ := hex.DecodeString(pd.Request)
		q, err := storerec.DecodeRequest(raw)
		if err != nil || uint64(tip) >= q.SeedHeight+storerec.SeedWindow {
			spend("the draw's seed left the answerer's window unanswered")
		}
	}
	if c.Draws >= maxDraws {
		c.Status = CandFailed
		note(Attempt{Outcome: "failed", Why: fmt.Sprintf("no pass in %d draws", maxDraws)})
		return false
	}
	var f frontAt
	if pd := c.Pending; pd != nil {
		ans, _ := hex.DecodeString(pd.Answerer)
		f = frontAt{url: pd.Front, answerer: ans}
	} else {
		fronts := p.fronts(ctx, r, c)
		if len(fronts) == 0 {
			note(Attempt{Outcome: "waiting", Why: "no front to answer for the advertiser"})
			return false
		}
		f = fronts[0]
	}
	own := bytes.Equal(f.answerer, adv)
	t := p.terms(ctx, f.url)
	depth := int64(0)
	if r.HoldHeight > 0 {
		depth = int64(tip) - int64(r.HoldHeight) + 1
	}
	if t.MinAge > 1 && depth < int64(t.MinAge) && o.Budget == 0 {
		c.Status = CandWaiting
		note(Attempt{Front: f.url, Outcome: "waiting", Why: fmt.Sprintf("the hold output is %d deep, the provider's minAge is %d", depth, t.MinAge)})
		return false
	}
	if t.Known && e.Hold.Satoshis < t.MinBounty && o.Budget == 0 {
		c.Status = CandPriced
		note(Attempt{Front: f.url, Outcome: "priced", Why: fmt.Sprintf("the bounty is below the provider's minimum of %d", t.MinBounty)})
		return false
	}
	n := min(e.Hold.CheckC, t.FreeC)
	ch := &Challenge{Bucket: r.Bucket, Root: e.Root, ProofRoot: e.ProofRoot, Size: e.Size, Entry: r.Entry,
		Provider: adv, Answerer: f.answerer, Front: f.url, URL: c.URL, Blobs: blobs, C: n, Budget: o.Budget}
	if c.Pending != nil {
		ch.Request, _ = hex.DecodeString(c.Pending.Request)
		if q, err := storerec.DecodeRequest(ch.Request); err == nil {
			ch.C = q.C
		}
	} else {
		_, raw, err := p.NewRequest(ctx, ch)
		if err != nil {
			note(Attempt{Front: f.url, Outcome: "not asked", Why: err.Error()})
			return false
		}
		ch.Request = raw
		// The draw is recorded before it is sent: a run that stops after
		// sending sends the same request again, never a fresh one.
		c.Pending = &PendingDraw{Request: hex.EncodeToString(raw), Front: f.url, Answerer: hex.EncodeToString(f.answerer)}
		if err := save(); err != nil {
			c.Pending = nil
			return false
		}
	}
	res, err := p.Challenge(ctx, ch, o.Fetch)
	at := Attempt{C: ch.C, Front: f.url}
	switch {
	case errors.Is(err, ErrOverBudget):
		at.Outcome, at.Why = "priced", "the provider's price is over the budget"
		note(at)
		c.Status = CandPriced
		return false
	case err != nil:
		at.Outcome, at.Why = "not asked", err.Error()
		note(at)
		return false
	}
	at.Outcome, at.Why, at.Paid = OutcomeName(res.Outcome), res.Why, res.Paid
	note(at)
	c.Paid += res.Paid
	answered := res.Status == http.StatusOK ||
		(res.Status == http.StatusUnprocessableEntity && res.Refusal != "seed")
	switch {
	case res.Status == http.StatusPaymentRequired:
		c.Status = CandPriced
		return false
	case res.Status == http.StatusNotFound && res.Code == "ERR_ROUTE_NOT_FOUND" && own && derived(c, f):
		// Row C31: the advertiser's own derived front has no bstore route
		// at all (an incumbent host behind a bridge). No route received the
		// challenge and nothing was signed or refused, so it was never a
		// draw. That front is not asked again, and both draws go to fronts
		// that state they answer for the advertiser.
		c.BadFronts = append(c.BadFronts, f.url)
		c.Pending = nil
		note(Attempt{C: ch.C, Front: f.url, Outcome: OutcomeNotADraw, Why: "the advertiser's derived front has no bstore route (404 ERR_ROUTE_NOT_FOUND, row C31)"})
		c.Status = CandSeen
		return true
	case res.Status == http.StatusNotFound || res.Status == http.StatusGone:
		// The front has no challenge route: an answer, final for this
		// front. The draw counts and the next front is asked now.
		c.BadFronts = append(c.BadFronts, f.url)
		spend(fmt.Sprintf("the front answered %d: it has no challenge route", res.Status))
		if c.Draws >= maxDraws {
			c.Status = CandFailed
			note(Attempt{Outcome: "failed", Why: fmt.Sprintf("no pass in %d draws", maxDraws)})
			return false
		}
		c.Status = CandSeen
		return true
	case !answered:
		// Not answered: the same request goes again later.
		c.Pending.Tries++
		if c.Pending.Tries >= pendingTries {
			// Not asked again in this race, the advertiser's own derived
			// front included: an incumbent host that never learned of
			// bstore has none, and its next draw goes to a front that
			// states it answers for it. The draw is spent either way.
			c.BadFronts = append(c.BadFronts, f.url)
			spend(fmt.Sprintf("the front gave no answer to the draw in %d tries", pendingTries))
		}
		if c.Status != CandFailed {
			c.Status = CandSeen
		}
		return false
	}
	c.Pending = nil
	c.Draws++
	switch {
	case res.Outcome == storerec.OutcomePass:
		c.Status, c.Front, c.Answerer = CandPassed, f.url, hex.EncodeToString(f.answerer)
		p.cfg.Logf("race %s: advertiser %s passed its settlement challenge (c = %d)", short(r.Entry), short(c.Advertiser), ch.C)
		return false
	case res.Outcome == storerec.OutcomeUnretrievable || own:
		// An unretrievable sample is about the advertiser's URL, whoever
		// answered; any other failure is the answerer's, here the
		// advertiser's own.
		c.Status = CandFailed
		p.cfg.Logf("race %s: advertiser %s did not pass: %s (%s)", short(r.Entry), short(c.Advertiser), at.Outcome, res.Why)
	default:
		// A front that is not the advertiser failed: the outcome is the
		// front's (spec section 7.4), and the front is not asked again.
		c.BadFronts = append(c.BadFronts, f.url)
		if c.Draws >= maxDraws {
			c.Status = CandFailed
		} else {
			c.Status = CandSeen
		}
		p.cfg.Logf("race %s: front %s for advertiser %s: %s (%s)", short(r.Entry), f.url, short(c.Advertiser), at.Outcome, res.Why)
	}
	if o.Publish && res.Status == http.StatusOK {
		if rec := res.OutcomeRecord(r.Bucket); rec != nil {
			if txid, err := p.PublishOutcome(ctx, rec); err == nil {
				outcome(PublishedNote{Advertiser: c.Advertiser, Outcome: at.Outcome, Txid: txid})
			} else {
				p.cfg.Logf("race %s: the outcome was not published: %v", short(r.Entry), err)
			}
		}
	}
	return false
}
