package publish

import (
	"sync"
	"sync/atomic"
)

// blobCache keeps the bytes of small objects Get has held to their hash, so
// a gateway serving the same object many times a second reads it from
// memory: a read from the store is an open, a stat, a read and a close, and
// at a web node's small-read ceiling those calls were a fifth of its CPU.
//
// The bytes are shared and read-only: every caller of Get must leave them as
// they are. Nothing is lost by a miss, and an object whose file goes
// (remove, drop) leaves the cache with it. Eviction is a clock: an entry
// read since the hand last passed is kept one more turn.
type blobCache struct {
	mu   sync.RWMutex
	m    map[[32]byte]*cached
	ring [][32]byte
	hand int
	held int
	max  int
}

type cached struct {
	b    []byte
	cost int
	ref  atomic.Bool
}

// entryCost is what an entry takes beyond its bytes' capacity: the map
// slot, the entry, its ring slot. A store of many tiny objects is held to
// the bound by what they cost, not by their lengths.
const entryCost = 160

// cacheBytes bounds the bytes a store's cache holds, and cacheObject the
// largest object it keeps: a large object's read is its copy, not its calls.
var (
	cacheBytes  = 64 << 20
	cacheObject = 1 << 20
)

func newBlobCache(max int) *blobCache {
	return &blobCache{m: map[[32]byte]*cached{}, max: max}
}

func (c *blobCache) get(h [32]byte) []byte {
	c.mu.RLock()
	e := c.m[h]
	c.mu.RUnlock()
	if e == nil {
		return nil
	}
	if !e.ref.Load() {
		e.ref.Store(true)
	}
	return e.b
}

func (c *blobCache) add(h [32]byte, b []byte) {
	cost := cap(b) + entryCost
	if len(b) > cacheObject || cost > c.max {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[h] != nil {
		return
	}
	for c.held+cost > c.max && c.evictLocked() {
	}
	c.m[h] = &cached{b: b, cost: cost}
	c.ring = append(c.ring, h)
	c.held += cost
}

// evictLocked removes one entry; false when none is left. Ring slots whose
// entry already went are dropped as the hand meets them.
func (c *blobCache) evictLocked() bool {
	for len(c.ring) > 0 {
		if c.hand >= len(c.ring) {
			c.hand = 0
		}
		h := c.ring[c.hand]
		e := c.m[h]
		if e != nil && e.ref.Swap(false) {
			c.hand++
			continue
		}
		last := len(c.ring) - 1
		c.ring[c.hand] = c.ring[last]
		c.ring = c.ring[:last]
		if e != nil {
			delete(c.m, h)
			c.held -= e.cost
			return true
		}
	}
	return false
}

// forget drops an object whose file is gone.
func (c *blobCache) forget(h [32]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.m[h]; e != nil {
		delete(c.m, h)
		c.held -= e.cost
		// Its ring slot goes when the hand meets it; a ring of mostly
		// gone slots is compacted here so it stays near the entries' count.
		if len(c.ring) > 2*len(c.m)+64 {
			n := 0
			for _, k := range c.ring {
				if c.m[k] != nil {
					c.ring[n] = k
					n++
				}
			}
			c.ring, c.hand = c.ring[:n], 0
		}
	}
}
