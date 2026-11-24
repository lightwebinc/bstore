package reader

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"time"
)

// A reader proves every read against the chain's latest token. Asking a host
// and verifying its answer for every read of a key that has not changed is
// most of what a busy reading gateway does, so two things are kept.
//
// An entry carrier that verified is remembered by its bytes: verifying the
// same bytes again proves nothing new. This is always on.
//
// With Memory set, what a key was proven to name is remembered while the
// chain's latest token is the one it was proven against, and the token is
// held to be the latest for Memory after the last answer that proved it so.
// Inside that time a read of a remembered key asks no host. Any read that
// does ask (a key not yet remembered, or the first read after the time ran
// out) proves the latest token again for every key of the chain; a newer
// token forgets them all. A new token is therefore seen at most Memory late,
// and never rolled back to.

// memoryKeys bounds the keys remembered per chain.
const memoryKeys = 65536

type chainMemory struct {
	tip  [32]byte
	seq  uint64
	at   time.Time
	keys map[string]*Found
}

type memory struct {
	mu     sync.Mutex
	chains map[string]*chainMemory
}

// get is what key was proven to name, when the chain's token was proven the
// latest less than d before now.
func (m *memory) get(chain, key string, now time.Time, d time.Duration) *Found {
	m.mu.Lock()
	defer m.mu.Unlock()
	cm := m.chains[chain]
	if cm == nil || now.Sub(cm.at) >= d || now.Before(cm.at) {
		return nil
	}
	return cm.keys[key]
}

// put remembers what a read that began at `at` proved. An answer proven
// against an older token than the one remembered is not kept: a slower read
// finishing late must not bring the older token back.
func (m *memory) put(chain, key string, f *Found, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.chains == nil {
		m.chains = map[string]*chainMemory{}
	}
	cm := m.chains[chain]
	switch {
	case cm == nil || f.Token.Bucket.Seq > cm.seq:
		cm = &chainMemory{tip: f.Token.Txid, seq: f.Token.Bucket.Seq, at: at, keys: map[string]*Found{}}
		m.chains[chain] = cm
	case f.Token.Bucket.Seq < cm.seq || f.Token.Txid != cm.tip:
		return
	case at.After(cm.at):
		cm.at = at
	}
	if len(cm.keys) >= memoryKeys {
		n := 0
		for k := range cm.keys {
			delete(cm.keys, k)
			if n++; n >= memoryKeys/4 {
				break
			}
		}
	}
	cm.keys[key] = f
}

// entryCache remembers verified entry carriers by bucket and bytes.
type entryCache struct {
	mu sync.Mutex
	m  map[[32]byte]*Entry
}

func entryKey(bucket string, beef []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(bucket))
	h.Write([]byte{0})
	h.Write(beef)
	var k [32]byte
	h.Sum(k[:0])
	return k
}

func (c *entryCache) get(k [32]byte) *Entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[k]
}

func (c *entryCache) put(k [32]byte, e *Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[[32]byte]*Entry{}
	}
	if len(c.m) >= 16384 {
		n := 0
		for x := range c.m {
			delete(c.m, x)
			if n++; n >= 4096 {
				break
			}
		}
	}
	c.m[k] = e
}

// Counts are how a reader's object reads were answered.
type Counts struct {
	// Asked reads asked a host; Remembered reads were answered from memory.
	Asked, Remembered int64
	// EntriesVerified and EntriesKnown are entry carriers verified and
	// carriers whose bytes had verified before.
	EntriesVerified, EntriesKnown int64
}

type counters struct {
	asked, remembered, verified, known atomic.Int64
}

// Counts reads the reader's counters.
func (r *Reader) Counts() Counts {
	return Counts{Asked: r.n.asked.Load(), Remembered: r.n.remembered.Load(),
		EntriesVerified: r.n.verified.Load(), EntriesKnown: r.n.known.Load()}
}
