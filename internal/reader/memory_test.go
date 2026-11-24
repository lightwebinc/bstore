package reader

import (
	"context"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/storerec"
)

func tokenAtSeq(seq uint64, b byte) *Token {
	return &Token{Txid: [32]byte{b}, Bucket: &storerec.Bucket{Seq: seq}}
}

// A key is answered from memory only inside the time its token was proven
// the latest, and any answer of the chain renews that time for every key.
func TestMemoryWindow(t *testing.T) {
	var m memory
	t0 := time.Unix(1000, 0)
	d := time.Second
	a := &Found{Token: tokenAtSeq(3, 1)}
	m.put("c", "a", a, t0)
	if m.get("c", "a", t0.Add(999*time.Millisecond), d) != a {
		t.Fatal("a key proven inside the time is not remembered")
	}
	if m.get("c", "a", t0.Add(d), d) != nil {
		t.Fatal("a key is remembered past the time")
	}
	if m.get("c", "b", t0, d) != nil || m.get("other", "a", t0, d) != nil {
		t.Fatal("a key never proven is remembered")
	}
	// Another key proven against the same token later renews the chain.
	b := &Found{Token: tokenAtSeq(3, 1)}
	m.put("c", "b", b, t0.Add(1500*time.Millisecond))
	if m.get("c", "a", t0.Add(2*time.Second), d) != a {
		t.Fatal("an answer for the same token did not renew the chain's keys")
	}
	// A read that began earlier and finished later does not move the time back.
	m.put("c", "a", a, t0)
	if m.get("c", "a", t0.Add(2400*time.Millisecond), d) != a {
		t.Fatal("a slower read moved the chain's time back")
	}
	// A clock that stepped back remembers nothing.
	if m.get("c", "a", t0.Add(-time.Second), d) != nil {
		t.Fatal("remembered before the time it was proven")
	}
}

// A newer token forgets every key; an answer against an older token, or
// another token at the same seq, is not kept.
func TestMemoryNewToken(t *testing.T) {
	var m memory
	t0 := time.Unix(1000, 0)
	d := time.Minute
	old := &Found{Token: tokenAtSeq(3, 1)}
	m.put("c", "a", old, t0)
	m.put("c", "b", old, t0)
	next := &Found{Token: tokenAtSeq(4, 2)}
	m.put("c", "a", next, t0.Add(time.Second))
	if m.get("c", "a", t0.Add(time.Second), d) != next {
		t.Fatal("the newer token's answer is not remembered")
	}
	if m.get("c", "b", t0.Add(time.Second), d) != nil {
		t.Fatal("a key proven against the older token outlived it")
	}
	m.put("c", "b", old, t0.Add(2*time.Second))
	if m.get("c", "b", t0.Add(2*time.Second), d) != nil {
		t.Fatal("a late answer against the older token was kept")
	}
	m.put("c", "b", &Found{Token: tokenAtSeq(4, 9)}, t0.Add(2*time.Second))
	if m.get("c", "b", t0.Add(2*time.Second), d) != nil {
		t.Fatal("another token at the same seq was kept")
	}
	if m.get("c", "a", t0.Add(2*time.Second), d) != next {
		t.Fatal("a refused answer disturbed what was remembered")
	}
}

func TestMemoryBound(t *testing.T) {
	var m memory
	t0 := time.Unix(1000, 0)
	f := &Found{Token: tokenAtSeq(1, 1)}
	for i := 0; i < memoryKeys+10; i++ {
		m.put("c", string(rune('a'+i%26))+time.Duration(i).String(), f, t0)
	}
	if n := len(m.chains["c"].keys); n > memoryKeys {
		t.Fatalf("%d keys remembered, the bound is %d", n, memoryKeys)
	}
}

// Object answers a remembered key without a host, and counts it.
func TestObjectRemembered(t *testing.T) {
	now := time.Unix(1000, 0)
	r := &Reader{Memory: time.Second, Now: func() time.Time { return now }, Logf: t.Logf}
	c := Chain{Bucket: "b"}
	f := &Found{Token: tokenAtSeq(2, 7), Result: storerec.ObjectFound}
	r.mem.put(c.key(), "k", f, now)
	got, err := r.Object(context.Background(), c, "k")
	if err != nil || got != f {
		t.Fatalf("got %v, %v", got, err)
	}
	// No host is configured: a key not remembered, or one past its time,
	// has nobody to ask.
	if _, err := r.Object(context.Background(), c, "other"); err == nil {
		t.Fatal("a key never proven was answered")
	}
	now = now.Add(time.Second)
	if _, err := r.Object(context.Background(), c, "k"); err == nil {
		t.Fatal("a key past its time was answered without a host")
	}
	if n := r.Counts(); n.Remembered != 1 || n.Asked != 2 {
		t.Fatalf("counts %+v", n)
	}
	// With no memory configured nothing is remembered.
	r2 := &Reader{Now: func() time.Time { return now }, Logf: t.Logf}
	r2.mem.put(c.key(), "k", f, now)
	if _, err := r2.Object(context.Background(), c, "k"); err == nil {
		t.Fatal("a reader with no memory answered from it")
	}
}

func TestEntryCacheKey(t *testing.T) {
	if entryKey("ab", []byte("c")) == entryKey("a", []byte("bc")) {
		t.Fatal("bucket and bytes run together in the key")
	}
	var c entryCache
	e := &Entry{}
	k := entryKey("b", []byte{1, 2})
	if c.get(k) != nil {
		t.Fatal("an empty cache answered")
	}
	c.put(k, e)
	if c.get(k) != e || c.get(entryKey("b2", []byte{1, 2})) != nil {
		t.Fatal("the cache does not answer by bucket and bytes")
	}
}
