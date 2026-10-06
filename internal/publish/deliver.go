package publish

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
type Outbox struct {
	dir  string
	d    *Delivery
	logf func(string, ...any)
	now  func() time.Time
	mu   sync.Mutex
	// sending marks items a send is working on, so the background run
	// leaves them alone.
	sending map[string]bool
}

type outItem struct {
	ID      string    `json:"id"`
	What    string    `json:"what"`
	Targets []Target  `json:"targets"`
	Created time.Time `json:"created"`
	Tries   int       `json:"tries"`
	Next    time.Time `json:"next"`
}

// outboxKeep is how long an undelivered object is tried.
const outboxKeep = 7 * 24 * time.Hour

func openOutbox(dir string, d *Delivery, logf func(string, ...any), now func() time.Time) (*Outbox, error) {
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o700); err != nil {
		return nil, err
	}
	return &Outbox{dir: dir, d: d, logf: logf, now: now, sending: map[string]bool{}}, nil
}

func (o *Outbox) paths(id string) (string, string) {
	return filepath.Join(o.dir, id+".beef"), filepath.Join(o.dir, id+".json")
}

// add records an object before it is sent.
func (o *Outbox) add(id, what string, beef []byte, targets []Target) (*outItem, error) {
	it := &outItem{ID: id, What: what, Targets: targets, Created: o.now(), Next: o.now()}
	bp, _ := o.paths(id)
	if err := writeFileSync(filepath.Join(o.dir, "tmp"), bp, beef); err != nil {
		return nil, err
	}
	if err := o.write(it); err != nil {
		return nil, err
	}
	return it, nil
}

func (o *Outbox) write(it *outItem) error {
	raw, err := json.Marshal(it)
	if err != nil {
		return err
	}
	_, mp := o.paths(it.ID)
	return writeFileSync(filepath.Join(o.dir, "tmp"), mp, raw)
}

func (o *Outbox) remove(id string) {
	bp, mp := o.paths(id)
	_ = os.Remove(mp)
	_ = os.Remove(bp)
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
	o.mu.Lock()
	o.sending[it.ID] = true
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		delete(o.sending, it.ID)
		o.mu.Unlock()
	}()
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
		o.remove(it.ID)
		return ok, refused
	}
	back := time.Duration(1<<min(it.Tries, 10)) * 5 * time.Second
	it.Next = o.now().Add(min(back, time.Hour))
	if err := o.write(it); err != nil {
		o.logf("outbox %s: %v", short(it.ID), err)
	}
	return ok, refused
}

// Drop removes every item of the given kinds ("piece", "entry",
// "outcome"): after a kill, what the sweeps retracted is not sent again.
func (o *Outbox) Drop(kinds ...string) int {
	ents, err := os.ReadDir(o.dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(o.dir, e.Name()))
		if err != nil {
			continue
		}
		var it outItem
		if json.Unmarshal(raw, &it) != nil || it.ID+".json" != e.Name() {
			continue
		}
		for _, k := range kinds {
			if it.What == k {
				o.remove(it.ID)
				n++
				break
			}
		}
	}
	return n
}

// Pending is the number of objects not yet delivered everywhere.
func (o *Outbox) Pending() int {
	ents, _ := os.ReadDir(o.dir)
	n := 0
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".json") {
			n++
		}
	}
	return n
}

// Retry sends every item that is due, oldest first; one a week old is
// given up, and said so.
func (o *Outbox) Retry(ctx context.Context) { o.retry(ctx, false) }

// RetryAll sends every item now, due or not.
func (o *Outbox) RetryAll(ctx context.Context) { o.retry(ctx, true) }

func (o *Outbox) retry(ctx context.Context, all bool) {
	ents, err := os.ReadDir(o.dir)
	if err != nil {
		return
	}
	var items []*outItem
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(o.dir, e.Name()))
		if err != nil {
			continue
		}
		var it outItem
		if json.Unmarshal(raw, &it) != nil || it.ID+".json" != e.Name() {
			continue
		}
		items = append(items, &it)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Created.Before(items[j].Created) })
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
			o.remove(it.ID)
			continue
		}
		bp, _ := o.paths(it.ID)
		beef, err := os.ReadFile(bp)
		if err != nil {
			o.logf("outbox %s: %v", short(it.ID), err)
			o.remove(it.ID)
			continue
		}
		o.send(ctx, it, beef)
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
