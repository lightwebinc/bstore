package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
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
// of the entry.
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
	}
	return out
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
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.Base+"/submit", bytes.NewReader(beef))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-topics", t.Topic)
	hc := d.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s on %s: %d %s", t.Base, t.Topic, res.StatusCode, strings.TrimSpace(string(body)))
	}
	var ans map[string]struct {
		Refused string `json:"refused"`
	}
	if json.Unmarshal(body, &ans) == nil {
		if a, ok := ans[t.Topic]; ok && a.Refused != "" {
			return fmt.Errorf("%w by %s on %s: %s", errRefused, t.Base, t.Topic, a.Refused)
		}
	}
	return nil
}

// Outbox is what an identity minted and has not delivered everywhere: each
// object's BEEF and the targets still owed it, written and synced before
// the first send. The same bytes are sent again until every target has
// taken them, or a week has passed.
//
// A piece carrier is kept by reference: its BEEF less the stored object it
// carries (a head and a tail around it), the object's hash and the whole
// BEEF's SHA-256, so the 4 MiB of an object are not written a second time.
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
}

type outItem struct {
	ID      string    `json:"id"`
	What    string    `json:"what"`
	Targets []Target  `json:"targets"`
	Created time.Time `json:"created"`
	Tries   int       `json:"tries"`
	Next    time.Time `json:"next"`
	// Ref, Head, Tail and Sum keep a BEEF by reference: Head, the stored
	// object Ref names (hex), then Tail, whose SHA-256 is Sum (hex).
	Ref  string `json:"ref,omitempty"`
	Head []byte `json:"head,omitempty"`
	Tail []byte `json:"tail,omitempty"`
	Sum  string `json:"sum,omitempty"`
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

// addRef records a carrier of a stored object in batch b, by reference
// when the object is found in the BEEF, and returns once the line is
// synced.
func (o *Outbox) addRef(b *outBatch, id, what string, beef []byte, obj [32]byte, data []byte, targets []Target) (*outItem, error) {
	it := &outItem{ID: id, What: what, Targets: targets, Created: o.now(), Next: o.now(), batch: b}
	if i := bytes.Index(beef, data); i >= 0 && o.store != nil && len(data) > 0 {
		sum := sha256.Sum256(beef)
		it.Ref, it.Head, it.Tail, it.Sum = hex.EncodeToString(obj[:]), beef[:i:i], beef[i+len(data):], hex.EncodeToString(sum[:])
	} else {
		return o.add(id, what, beef, targets)
	}
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
// and held to its hash.
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
	data, err := o.store.Get(h)
	if err != nil {
		return nil, err
	}
	beef := make([]byte, 0, len(it.Head)+len(data)+len(it.Tail))
	beef = append(append(append(beef, it.Head...), data...), it.Tail...)
	if sum := sha256.Sum256(beef); hex.EncodeToString(sum[:]) != it.Sum {
		return nil, errors.New("the rebuilt BEEF is not the one recorded")
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
	if d, err := os.Open(o.dir); err == nil {
		_ = d.Sync()
		d.Close()
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
	if err := f.Sync(); err != nil {
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
