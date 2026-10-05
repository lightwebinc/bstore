package auth104

import (
	"container/list"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"
)

// Budget is a token bucket's rate and burst: Rate a second, at most Burst
// at once.
type Budget struct {
	Rate  float64
	Burst float64
}

func (b Budget) check(name string) error {
	if !(b.Rate > 0) || math.IsInf(b.Rate, 0) || !(b.Burst >= 1) || math.IsInf(b.Burst, 0) {
		return fmt.Errorf("auth104: %s: a rate above 0 and a burst of at least 1", name)
	}
	return nil
}

type bucket struct {
	tokens float64
	at     time.Time
}

func (b *bucket) refill(c Budget, now time.Time) {
	if now.After(b.at) {
		b.tokens = math.Min(c.Burst, b.tokens+now.Sub(b.at).Seconds()*c.Rate)
	}
	b.at = now
}

// wait is the seconds until one token is there, at least 1.
func (b *bucket) wait(c Budget) int {
	return max(1, int(math.Ceil((1-b.tokens)/c.Rate)))
}

// Keyed is token buckets by key, at most Max of them: past it the least
// recently used is forgotten, which only ever gives a forgotten key a full
// bucket again, never more than Burst at once.
type Keyed struct {
	c   Budget
	max int
	now func() time.Time

	mu    sync.Mutex
	order *list.List // of string, least recently used at the front
	byKey map[string]*keyedEntry
}

type keyedEntry struct {
	b  bucket
	el *list.Element
}

// NewKeyed is a keyed budget of at most max keys.
func NewKeyed(c Budget, max int, now func() time.Time) (*Keyed, error) {
	if err := c.check("keyed budget"); err != nil {
		return nil, err
	}
	if max < 1 {
		return nil, fmt.Errorf("auth104: a keyed budget keeps at least one key")
	}
	if now == nil {
		now = time.Now
	}
	return &Keyed{c: c, max: max, now: now, order: list.New(), byKey: map[string]*keyedEntry{}}, nil
}

// Take takes one token from key's bucket, or says how many seconds until
// one is there.
func (k *Keyed) Take(key string) (bool, int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	now := k.now()
	e := k.entry(key, now)
	e.b.refill(k.c, now)
	if e.b.tokens < 1 {
		return false, e.b.wait(k.c)
	}
	e.b.tokens--
	return true, 0
}

func (k *Keyed) entry(key string, now time.Time) *keyedEntry {
	if e, ok := k.byKey[key]; ok {
		k.order.MoveToBack(e.el)
		return e
	}
	e := &keyedEntry{b: bucket{tokens: k.c.Burst, at: now}}
	e.el = k.order.PushBack(key)
	k.byKey[key] = e
	for k.order.Len() > k.max {
		front := k.order.Front()
		k.order.Remove(front)
		delete(k.byKey, front.Value.(string))
	}
	return e
}

// Len is how many keys have a bucket.
func (k *Keyed) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.order.Len()
}

// Handshakes is the handshake budget: a bucket for the route and one for
// each remote address, an address's checked first so that one address that
// floods spends its own budget and not the route's.
type Handshakes struct {
	global Budget
	mu     sync.Mutex
	g      bucket
	addr   *Keyed
	now    func() time.Time
}

// MaxAddresses is the most addresses with a bucket of their own.
const MaxAddresses = 10000

// NewHandshakes is a handshake budget.
func NewHandshakes(global, perAddress Budget, now func() time.Time) (*Handshakes, error) {
	if err := global.check("handshake budget"); err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	a, err := NewKeyed(perAddress, MaxAddresses, now)
	if err != nil {
		return nil, err
	}
	return &Handshakes{global: global, g: bucket{tokens: global.Burst, at: now()}, addr: a, now: now}, nil
}

// Take takes one handshake for a remote address, or says which limit
// refused it ("address" or "global") and the seconds to wait.
func (h *Handshakes) Take(remote string) (ok bool, limit string, retry int) {
	if ok, r := h.addr.Take(AddressKey(remote)); !ok {
		return false, "address", r
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	h.g.refill(h.global, now)
	if h.g.tokens < 1 {
		return false, "global", h.g.wait(h.global)
	}
	h.g.tokens--
	return true, "", 0
}

// AddressKey is the key a remote address is budgeted under: an IPv4
// address as it is (an IPv4-mapped IPv6 address as its IPv4 address), an
// IPv6 address by its /64, which is what one host is usually given. A port
// is dropped.
func AddressKey(remote string) string {
	host := remote
	if h, _, err := net.SplitHostPort(remote); err == nil {
		host = h
	}
	host = strings.TrimSpace(host)
	if i := strings.IndexByte(host, '%'); i >= 0 {
		host = host[:i]
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return (&net.IPNet{IP: ip.Mask(net.CIDRMask(64, 128)), Mask: net.CIDRMask(64, 128)}).String()
}
