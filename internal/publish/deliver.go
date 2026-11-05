package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	bpub "github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/bstore/storerec"
)

// Delivery is how a publisher reaches the plane, the hosts and the
// providers (spec section 14). With a facade, pieces go to it once on the
// blob topic and records once on the records topic; mined objects also go
// to every host directly, because a host whose headers lagged refused them
// and the plane does not deliver them again. Without one (unicast), pieces
// go to each provider's submit route on the blob topic, and every record to
// each host and, for an entry, each provider. Either way an entry also goes
// to each provider named, so a provider that unicast pieces reached hears
// of the entry, and to the origin of each of Uploads.
type Delivery struct {
	Facade    string
	Hosts     []string
	Providers []string
	// Uploads are providers' public URLs (the base their /chirp/v1 routes
	// hang from). Without a facade, when any is set, a closure goes to each
	// of them by BRC-167 upload session instead of as piece carriers.
	Uploads []string
	HTTP    *http.Client
	// Quorum is how many hosts must take an entry for a put without a
	// facade to count as sent; zero is one.
	Quorum int
	// Observe, when set, is told how long each step took, in seconds, as a
	// histogram name with labels: `bstore_publish_submit_seconds{target}`
	// for each send (plane, host or provider), and
	// `bstore_publish_piece_seconds{stage}` for each piece carrier's read,
	// mint, outbox and send.
	Observe func(name string, seconds float64)
}

// observe reports a step that began at since.
func (d *Delivery) observe(name string, since time.Time) {
	if d != nil && d.Observe != nil {
		d.Observe(name, time.Since(since).Seconds())
	}
}

// kind is the target's label: the plane, a host or a provider.
func (t Target) kind() string {
	switch {
	case t.Plane:
		return "plane"
	case t.Host:
		return "host"
	}
	return "provider"
}

// Target is one place one object goes: a submit route and a topic.
type Target struct {
	Base  string `json:"base"`
	Topic string `json:"topic"`
	// Plane marks the facade; Host a host (not a provider).
	Plane bool `json:"plane,omitempty"`
	Host  bool `json:"host,omitempty"`
}

func trim(b string) string { return strings.TrimRight(b, "/") }

// uploading reports whether closures go by upload session.
func (d *Delivery) uploading() bool { return d.Facade == "" && len(d.Uploads) > 0 }

// pieceTargets are where a bucket's pieces go.
func (d *Delivery) pieceTargets(bucket string) []Target {
	topic := storerec.BlobTopic(bucket)
	var out []Target
	if d.Facade != "" {
		out = append(out, Target{Base: trim(d.Facade), Topic: topic, Plane: true})
	}
	for _, p := range d.Providers {
		out = append(out, Target{Base: trim(p), Topic: topic})
	}
	return out
}

// entryTargets are where a bucket's entries go; mined objects go to the
// facade and the hosts only.
func (d *Delivery) recordTargets(bucket string, entry bool) []Target {
	topic := storerec.RecordsTopic(bucket)
	var out []Target
	if d.Facade != "" {
		out = append(out, Target{Base: trim(d.Facade), Topic: topic, Plane: true})
	}
	for _, h := range d.Hosts {
		out = append(out, Target{Base: trim(h), Topic: topic, Host: true})
	}
	if entry {
		for _, p := range d.Providers {
			out = append(out, Target{Base: trim(p), Topic: topic})
		}
		if d.uploading() {
			// A closure sent by upload session waits at its provider for the
			// entry, and an unclaimed closure is dropped.
			for _, u := range d.Uploads {
				b := origin(u)
				if !slices.ContainsFunc(out, func(t Target) bool { return t.Base == b }) {
					out = append(out, Target{Base: b, Topic: topic})
				}
			}
		}
	}
	return out
}

// origin is the scheme and host of a provider's public URL: its submit
// route hangs from the origin, whatever path the public URL has.
func origin(public string) string {
	u, err := url.Parse(public)
	if err != nil || u.Host == "" {
		return trim(public)
	}
	return u.Scheme + "://" + u.Host
}

// sent reports whether an entry delivered to ok counts as sent: the facade
// took it, or, without one, a quorum of the hosts did.
func (d *Delivery) sent(ok []Target) bool {
	hosts := 0
	for _, t := range ok {
		if t.Plane {
			return true
		}
		if t.Host {
			hosts++
		}
	}
	if d.Facade != "" {
		return false
	}
	q := max(d.Quorum, 1)
	return hosts >= min(q, len(d.Hosts)) && hosts > 0
}

// errRefused is a target that answered and took nothing for a reason it
// gave: sending the same bytes again will not change its mind.
var errRefused = errors.New("publish: refused")

// submit posts a BEEF to a target's submit route, one topic, as the
// overlay engine takes it. A 200 is delivered, whatever the answer admits:
// an engine answers an object it already holds with nothing admitted, and a
// provider names why it took nothing.
func (d *Delivery) submit(ctx context.Context, t Target, beef []byte) error {
	defer d.observe(`bstore_publish_submit_seconds{target="`+t.kind()+`"}`, time.Now())
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for try := 0; ; try++ {
		wait, err := d.submitOnce(ctx, t, beef)
		if wait == 0 || try >= busyTries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// busyTries is how many times a target that answers "busy, retry" is asked
// again before the submission counts as not delivered: a facade's publish
// queue full is the plane draining slower than a put offers, a moment's
// backpressure, and a put that failed on it would mint every piece again.
// With the facade's own wait for room and Retry-After 1 this rides out about
// half a minute.
const busyTries = 10

// submitOnce makes one attempt. It returns how long to wait before another
// when the target asked for one, or zero: a 503 with Retry-After (a facade's
// queue full), or a 429 with Retry-After (a provider's per-address submit
// budget or in-flight limit). Without the 429, a gateway with more piece
// workers than a provider's default in-flight allowance failed a large put
// on the direct path: every piece "reached no target".
func (d *Delivery) submitOnce(ctx context.Context, t Target, beef []byte) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.Base+"/submit", bytes.NewReader(beef))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-topics", t.Topic)
	hc := d.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	res, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if res.StatusCode != http.StatusOK {
		err := fmt.Errorf("%s on %s: %d %s", t.Base, t.Topic, res.StatusCode, strings.TrimSpace(string(body)))
		if res.StatusCode == http.StatusServiceUnavailable || res.StatusCode == http.StatusTooManyRequests {
			if secs, e := strconv.Atoi(res.Header.Get("Retry-After")); e == nil && secs >= 0 {
				return max(time.Duration(secs)*time.Second, 100*time.Millisecond), err
			}
		}
		return 0, err
	}
	var ans map[string]struct {
		Refused string `json:"refused"`
	}
	if json.Unmarshal(body, &ans) == nil {
		if a, ok := ans[t.Topic]; ok && a.Refused != "" {
			return 0, fmt.Errorf("%w by %s on %s: %s", errRefused, t.Base, t.Topic, a.Refused)
		}
	}
	return 0, nil
}

// Outbox is what an identity minted and has not delivered everywhere: each
// object's BEEF and the targets still owed it, written and synced before
// the first send. The same bytes are sent again until every target has
// taken them, or a week has passed.
//
// A piece carrier is kept by reference: its BEEF less the stored object it
// carries (a head and a tail around it), the object's hash and a SHA-256
// binding the head and the tail, so the 4 MiB of an object are neither
// written nor hashed a second time: the store holds the object to its hash.
// A put's pieces are written to one batch file, a line each, and the
// workers minting them share each sync (a group commit): every line is
// synced before its carrier is sent. An item a target still owes after its
// first send moves to a file of its own; one every target took is marked
// done in the batch, and a batch whose every line is done is removed.
// The objects referred to are pinned in the store while they are owed.
type Outbox struct {
	dir   string
	d     *Delivery
	store *Store
	logf  func(string, ...any)
	now   func() time.Time
	mu    sync.Mutex
	// sending marks items a send is working on, so the background run
	// leaves them alone; refs are the store objects pinned for items.
	sending map[string]bool
	refs    map[string][32]byte
	// live are the batch files a put is writing: the background run
	// leaves them to it.
	live map[string]bool
	// Max bounds the bytes owed (0 is DefaultOutboxMax); owed is the
	// last count, and owedAt when it was taken.
	Max    int64
	owed   owedCount
	owedAt time.Time
}

// DefaultOutboxMax is the bytes an identity's outbox may owe before a put
// is refused: what targets have not taken (a provider that is down), kept
// as files of the outbox's own or pinned in the store.
const DefaultOutboxMax = 8 << 30

// ErrOutboxFull is a put refused because the outbox owes more than its
// bound: a target has not taken what was sent it. Nothing is spent.
var ErrOutboxFull = errors.New("publish: the outbox is full")

type owedCount struct {
	total  int64
	byDest map[string]int64
}

type outItem struct {
	ID      string    `json:"id"`
	What    string    `json:"what"`
	Targets []Target  `json:"targets"`
	Created time.Time `json:"created"`
	Tries   int       `json:"tries"`
	Next    time.Time `json:"next"`
	// Ref, Head, Tail and Sum keep a BEEF by reference: Head, the stored
	// object Ref names (hex), then Tail. At Ver refVersion, Sum (hex) is
	// refSum of the head and the tail; at Ver 0, an item an earlier build
	// wrote, it is the SHA-256 of the whole BEEF.
	Ref  string `json:"ref,omitempty"`
	Head []byte `json:"head,omitempty"`
	Tail []byte `json:"tail,omitempty"`
	Sum  string `json:"sum,omitempty"`
	Ver  int    `json:"ver,omitempty"`
	// Done marks a batch line whose item left the batch.
	Done string `json:"done,omitempty"`

	batch *outBatch
}

// outboxKeep is how long an undelivered object is tried.
const outboxKeep = 7 * 24 * time.Hour

func openOutbox(dir string, d *Delivery, store *Store, logf func(string, ...any), now func() time.Time) (*Outbox, error) {
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o700); err != nil {
		return nil, err
	}
	o := &Outbox{dir: dir, d: d, store: store, logf: logf, now: now, sending: map[string]bool{}, refs: map[string][32]byte{}, live: map[string]bool{}}
	// What is owed by reference keeps its object in the store.
	items := o.load()
	for _, it := range items {
		o.pinRef(it)
	}
	o.closeBatches(items)
	return o, nil
}

func (o *Outbox) paths(id string) (string, string) {
	return filepath.Join(o.dir, id+".beef"), filepath.Join(o.dir, id+".json")
}

func (o *Outbox) pinRef(it *outItem) {
	if it.Ref == "" || o.store == nil {
		return
	}
	h, err := parseHash(it.Ref)
	if err != nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.refs[it.ID]; ok {
		return
	}
	o.refs[it.ID] = h
	o.store.pin(h)
}

func (o *Outbox) unpinRef(id string) {
	o.mu.Lock()
	h, ok := o.refs[id]
	delete(o.refs, id)
	o.mu.Unlock()
	if ok && o.store != nil {
		o.store.unpinOne(h)
	}
}

func (o *Outbox) markSending(id string) {
	o.mu.Lock()
	o.sending[id] = true
	o.mu.Unlock()
}

// add records an object before it is sent.
func (o *Outbox) add(id, what string, beef []byte, targets []Target) (*outItem, error) {
	it := &outItem{ID: id, What: what, Targets: targets, Created: o.now(), Next: o.now()}
	o.markSending(id)
	bp, _ := o.paths(id)
	if err := writeFileSync(filepath.Join(o.dir, "tmp"), bp, beef); err != nil {
		o.done(id)
		return nil, err
	}
	if err := o.write(it); err != nil {
		o.done(id)
		return nil, err
	}
	return it, nil
}

// refVersion marks an item whose Sum binds its head and tail alone.
const refVersion = 1

// refSum binds an item's head and tail, and where they meet: the SHA-256
// of the head's length (8 bytes, little-endian), the head and the tail.
func refSum(head, tail []byte) string {
	h := sha256.New()
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(len(head)))
	h.Write(n[:])
	h.Write(head)
	h.Write(tail)
	return hex.EncodeToString(h.Sum(nil))
}

// addRef records a carrier of a stored object in batch b, by reference
// when the object's n bytes lie at offset at of the BEEF and there is a
// store to read them back from, and returns once the line is synced. The
// head and the tail are copied: the item outlives the BEEF, which a put
// holds for many pieces until their background send. A carrier whose
// object is not placed (at below zero) is kept whole.
func (o *Outbox) addRef(b *outBatch, id, what string, beef []byte, obj [32]byte, at, n int, targets []Target) (*outItem, error) {
	if at < 0 || n < 1 || at+n > len(beef) || o.store == nil {
		return o.add(id, what, beef, targets)
	}
	it := &outItem{ID: id, What: what, Targets: targets, Created: o.now(), Next: o.now(), batch: b,
		Ref: hex.EncodeToString(obj[:]), Head: bytes.Clone(beef[:at]), Tail: bytes.Clone(beef[at+n:]), Ver: refVersion}
	it.Sum = refSum(it.Head, it.Tail)
	o.markSending(id)
	o.pinRef(it)
	if err := b.append(it); err != nil {
		o.unpinRef(id)
		o.done(id)
		return nil, err
	}
	return it, nil
}

// beefOf is an item's BEEF: its own file, or rebuilt from its reference
// and held to its Sum (the store holds the object to its hash).
func (o *Outbox) beefOf(it *outItem) ([]byte, error) {
	if it.Ref == "" {
		bp, _ := o.paths(it.ID)
		return os.ReadFile(bp)
	}
	h, err := parseHash(it.Ref)
	if err != nil {
		return nil, err
	}
	if o.store == nil {
		return nil, errors.New("no store to read the carried object from")
	}
	if it.Ver >= refVersion && refSum(it.Head, it.Tail) != it.Sum {
		return nil, errors.New("the head and tail kept are not the ones recorded")
	}
	data, err := o.store.Get(h)
	if err != nil {
		return nil, err
	}
	beef := make([]byte, 0, len(it.Head)+len(data)+len(it.Tail))
	beef = append(append(append(beef, it.Head...), data...), it.Tail...)
	if it.Ver < refVersion {
		// An item an earlier build wrote: its Sum is over the whole BEEF.
		if sum := sha256.Sum256(beef); hex.EncodeToString(sum[:]) != it.Sum {
			return nil, errors.New("the rebuilt BEEF is not the one recorded")
		}
	}
	return beef, nil
}

func (o *Outbox) done(id string) {
	o.mu.Lock()
	delete(o.sending, id)
	o.mu.Unlock()
}

func (o *Outbox) write(it *outItem) error {
	c := *it
	c.batch = nil
	raw, err := json.Marshal(&c)
	if err != nil {
		return err
	}
	_, mp := o.paths(it.ID)
	return writeFileSync(filepath.Join(o.dir, "tmp"), mp, raw)
}

// remove takes an item out of the outbox: its own files, or its batch line.
func (o *Outbox) remove(it *outItem) {
	bp, mp := o.paths(it.ID)
	_ = os.Remove(mp)
	_ = os.Remove(bp)
	if it.batch != nil {
		it.batch.markDone(it.ID)
	}
	o.unpinRef(it.ID)
}

// send offers an item to every target it is still owed, at once, and
// returns those that took it. A target that refused it is neither counted
// nor asked again. An item no target is still owed leaves the outbox.
func (o *Outbox) send(ctx context.Context, it *outItem, beef []byte) []Target {
	ok, _ := o.sendAll(ctx, it, beef)
	return ok
}

// sendAll is send, and also returns the targets that refused the item.
func (o *Outbox) sendAll(ctx context.Context, it *outItem, beef []byte) (ok, refused []Target) {
	o.markSending(it.ID)
	defer o.done(it.ID)
	type res struct {
		t   Target
		err error
	}
	ch := make(chan res, len(it.Targets))
	for _, t := range it.Targets {
		go func(t Target) { ch <- res{t, o.d.submit(ctx, t, beef)} }(t)
	}
	var left []Target
	for range it.Targets {
		r := <-ch
		switch {
		case r.err == nil:
			ok = append(ok, r.t)
		case errors.Is(r.err, errRefused):
			// Taken nothing, for a reason the same bytes will not change:
			// neither delivered nor sent again.
			o.logf("%s %s: %v", it.What, short(it.ID), r.err)
			refused = append(refused, r.t)
		default:
			o.logf("%s %s: not delivered to %s (%v); it is sent again later", it.What, short(it.ID), r.t.Base, r.err)
			left = append(left, r.t)
		}
	}
	it.Tries++
	it.Targets = left
	if len(left) == 0 {
		o.remove(it)
		return ok, refused
	}
	back := time.Duration(1<<min(it.Tries, 10)) * 5 * time.Second
	it.Next = o.now().Add(min(back, time.Hour))
	// An item of a batch moves to a file of its own, then leaves the
	// batch: a crash between the two leaves both, and the file wins.
	if err := o.write(it); err != nil {
		o.logf("outbox %s: %v", short(it.ID), err)
		return ok, refused
	}
	if it.batch != nil {
		it.batch.markDone(it.ID)
		it.batch = nil
	}
	return ok, refused
}

// Drop removes every item of the given kinds ("piece", "entry",
// "outcome"): after a kill, what the sweeps retracted is not sent again.
func (o *Outbox) Drop(kinds ...string) int {
	n := 0
	items := o.load()
	for _, it := range items {
		if slices.Contains(kinds, it.What) {
			o.remove(it)
			n++
		}
	}
	o.closeBatches(items)
	return n
}

// Pending is the number of objects not yet delivered everywhere.
func (o *Outbox) Pending() int {
	items := o.load()
	o.closeBatches(items)
	return len(items)
}

// Retry sends every item that is due, oldest first; one a week old is
// given up, and said so.
func (o *Outbox) Retry(ctx context.Context) { o.retry(ctx, false) }

// RetryAll sends every item now, due or not.
func (o *Outbox) RetryAll(ctx context.Context) { o.retry(ctx, true) }

func (o *Outbox) retry(ctx context.Context, all bool) {
	items := o.load()
	defer o.closeBatches(items)
	defer o.count()
	sort.SliceStable(items, func(i, j int) bool { return items[i].Created.Before(items[j].Created) })
	for _, it := range items {
		if ctx.Err() != nil {
			return
		}
		o.mu.Lock()
		busy := o.sending[it.ID]
		o.mu.Unlock()
		if busy || (!all && o.now().Before(it.Next)) {
			continue
		}
		if o.now().Sub(it.Created) > outboxKeep {
			o.logf("%s %s: given up after a week; still owed to %d target(s)", it.What, short(it.ID), len(it.Targets))
			o.remove(it)
			continue
		}
		beef, err := o.beefOf(it)
		if err != nil {
			o.logf("outbox %s: %v; it cannot be sent again", short(it.ID), err)
			o.remove(it)
			continue
		}
		o.send(ctx, it, beef)
	}
}

// load reads every item owed: each file of its own, and each batch line
// not done and not moved to a file of its own.
func (o *Outbox) load() []*outItem {
	ents, err := os.ReadDir(o.dir)
	if err != nil {
		return nil
	}
	var items []*outItem
	own := map[string]bool{}
	var batches []string
	for _, e := range ents {
		switch {
		case strings.HasSuffix(e.Name(), ".json"):
			raw, err := os.ReadFile(filepath.Join(o.dir, e.Name()))
			if err != nil {
				continue
			}
			var it outItem
			if json.Unmarshal(raw, &it) != nil || it.ID+".json" != e.Name() {
				continue
			}
			own[it.ID] = true
			items = append(items, &it)
		case strings.HasSuffix(e.Name(), ".batch"):
			o.mu.Lock()
			live := o.live[e.Name()]
			o.mu.Unlock()
			if !live {
				batches = append(batches, e.Name())
			}
		}
	}
	for _, name := range batches {
		b, lines, err := o.openBatch(name)
		if err != nil {
			o.logf("outbox batch %s: %v", name, err)
			continue
		}
		for _, it := range lines {
			if own[it.ID] {
				b.markDone(it.ID)
				continue
			}
			it.batch = b
			items = append(items, it)
		}
	}
	return items
}

// closeBatches closes the batches items were read from, removing each
// whose every line is done.
func (o *Outbox) closeBatches(items []*outItem) {
	seen := map[*outBatch]bool{}
	for _, it := range items {
		if it.batch != nil && !seen[it.batch] {
			seen[it.batch] = true
			it.batch.close()
		}
	}
}

// outBatch is one batch file: item lines and done lines, appended.
type outBatch struct {
	o    *Outbox
	path string
	mu   sync.Mutex
	f    *os.File
	// written counts lines written; synced the lines a sync covers; open
	// the items not yet done.
	written, synced int
	open            map[string]bool
	syncMu          sync.Mutex
	// live is the file's name while the put that writes it runs.
	live string
	// ids is every item the put wrote, in the batch or in a file of its
	// own, for abandon.
	ids []string
}

// track records an item of the put, wherever it is kept.
func (b *outBatch) track(id string) {
	b.mu.Lock()
	b.ids = append(b.ids, id)
	b.mu.Unlock()
}

// newBatch starts a batch file.
func (o *Outbox) newBatch() (*outBatch, error) {
	var r [8]byte
	_, _ = rand.Read(r[:])
	name := fmt.Sprintf("%d-%s.batch", o.now().UnixNano(), hex.EncodeToString(r[:]))
	path := filepath.Join(o.dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	if err := commits.syncDir(o.dir); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	o.mu.Lock()
	o.live[name] = true
	o.mu.Unlock()
	return &outBatch{o: o, path: path, f: f, open: map[string]bool{}, live: name}, nil
}

// openBatch reads a batch file and opens it to mark lines done.
func (o *Outbox) openBatch(name string) (*outBatch, []*outItem, error) {
	path := filepath.Join(o.dir, name)
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var lines []*outItem
	done := map[string]bool{}
	for _, l := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(l)) == 0 {
			continue
		}
		var it outItem
		if json.Unmarshal(l, &it) != nil {
			continue // a line a crash cut short: it was never synced, so never sent
		}
		if it.Done != "" {
			done[it.Done] = true
			continue
		}
		if it.ID != "" {
			lines = append(lines, &it)
		}
	}
	b := &outBatch{o: o, path: path, open: map[string]bool{}}
	var out []*outItem
	for _, it := range lines {
		if !done[it.ID] && !b.open[it.ID] {
			b.open[it.ID] = true
			out = append(out, it)
		}
	}
	if len(out) == 0 {
		_ = os.Remove(path)
		return b, nil, nil
	}
	if b.f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600); err != nil {
		return nil, nil, err
	}
	return b, out, nil
}

// append writes an item's line and returns once a sync covers it; the
// workers of one put share each sync.
func (b *outBatch) append(it *outItem) error {
	c := *it
	c.batch = nil
	raw, err := json.Marshal(&c)
	if err != nil {
		return err
	}
	b.mu.Lock()
	if b.f == nil {
		b.mu.Unlock()
		return errors.New("outbox batch closed")
	}
	if _, err := b.f.Write(append(raw, '\n')); err != nil {
		b.mu.Unlock()
		return err
	}
	b.written++
	mine := b.written
	b.open[it.ID] = true
	b.mu.Unlock()
	b.syncMu.Lock()
	defer b.syncMu.Unlock()
	b.mu.Lock()
	if b.synced >= mine {
		b.mu.Unlock()
		return nil
	}
	upto, f := b.written, b.f
	b.mu.Unlock()
	if err := commits.syncFile(f); err != nil {
		return err
	}
	b.mu.Lock()
	b.synced = max(b.synced, upto)
	b.mu.Unlock()
	return nil
}

// markDone records that an item left the batch. It is not synced: a crash
// that loses it sends the same bytes again.
func (b *outBatch) markDone(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open[id] {
		return
	}
	delete(b.open, id)
	if b.f != nil {
		raw, _ := json.Marshal(outItem{Done: id})
		_, _ = b.f.Write(append(raw, '\n'))
	}
}

// abandon drops every item the put wrote and did not deliver, in the batch
// or moved to a file of its own: the put failed, so no entry names their
// carriers, and a retry of the put mints its own. Called once the put's
// senders have stopped.
func (b *outBatch) abandon() {
	b.mu.Lock()
	ids := slices.Clone(b.ids)
	b.mu.Unlock()
	for _, id := range ids {
		b.o.remove(&outItem{ID: id, batch: b})
	}
}

// close closes the file, removing it when every line is done.
func (b *outBatch) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f != nil {
		_ = b.f.Close()
		b.f = nil
	}
	if len(b.open) == 0 {
		_ = os.Remove(b.path)
	}
	if b.live != "" {
		b.o.mu.Lock()
		delete(b.o.live, b.live)
		b.o.mu.Unlock()
		b.live = ""
	}
}

// keptFacade is a facade that sends nothing: the library publishes each
// funding tree it adopts through one, and a bstore funding tree goes to no
// host (a host admits the carriers that spend it, never the tree).
func keptFacade() *bpub.Facade {
	return &bpub.Facade{Base: "http://kept.invalid", HTTP: keptClient}
}

var keptClient = &http.Client{Transport: keptTransport{}}

type keptTransport struct{}

func (keptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	raw, _ := json.Marshal(map[string]any{req.Header.Get("x-topics"): map[string]any{"outputsToAdmit": []int{}, "coinsToRetain": []int{}}})
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw)),
		ContentLength: int64(len(raw)), Request: req}, nil
}

// itemBytes is what an item keeps owed: its own BEEF file, or the stored
// object it refers to with its head and tail.
func (o *Outbox) itemBytes(it *outItem) int64 {
	if it.Ref == "" {
		bp, _ := o.paths(it.ID)
		if fi, err := os.Stat(bp); err == nil {
			return fi.Size()
		}
		return 0
	}
	n := int64(len(it.Head) + len(it.Tail))
	if h, err := parseHash(it.Ref); err == nil && o.store != nil {
		if fi, err := os.Stat(o.store.objectPath(h)); err == nil {
			n += fi.Size()
		}
	}
	return n
}

// count takes the bytes owed, in all and by target.
func (o *Outbox) count() owedCount {
	items := o.load()
	c := owedCount{byDest: map[string]int64{}}
	for _, it := range items {
		n := o.itemBytes(it)
		c.total += n
		for _, t := range it.Targets {
			c.byDest[t.Base] += n
		}
	}
	o.closeBatches(items)
	o.mu.Lock()
	o.owed, o.owedAt = c, o.now()
	o.mu.Unlock()
	return c
}

// Owed is the bytes the outbox owes, in all and by target URL, counted at
// most a few seconds ago.
func (o *Outbox) Owed() (int64, map[string]int64) {
	o.mu.Lock()
	c, at := o.owed, o.owedAt
	o.mu.Unlock()
	if at.IsZero() || o.now().Sub(at) > 5*time.Second {
		c = o.count()
	}
	return c.total, c.byDest
}

// room refuses a put of size more bytes when the outbox owes its bound
// already, naming the target that owes most.
func (o *Outbox) room(size int64) error {
	limit := o.Max
	if limit <= 0 {
		limit = DefaultOutboxMax
	}
	total, by := o.Owed()
	if total+size <= limit || total == 0 {
		return nil
	}
	worst, most := "", int64(0)
	for u, n := range by {
		if n > most || n == most && u < worst {
			worst, most = u, n
		}
	}
	return fmt.Errorf("%w: %d bytes owed against a bound of %d; %s has not taken %d bytes (is it down?); try again once it has", ErrOutboxFull, total, limit, worst, most)
}
