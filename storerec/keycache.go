package storerec

import (
	"math/big"
	"sync"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/pushdrop"
)

// keyCacheSize bounds each key cache; a full cache starts over.
const keyCacheSize = 4096

// A bucket's writers sign many records, and the keys a carrier's rules read
// cost more than the rest of them but the field signature: CheckKey's parse
// is a square root, LockingKey's derivation a base point multiplication.
// Both are pure functions of their bytes, so each answer is remembered
// under the exact bytes it was computed from, and only answers that held:
// a refusal is computed again every time. A hit answers a copy, so no
// caller shares a point another caller holds.
var (
	checkedKeys = &keyCache[[33]byte]{}
	lockingKeys = &keyCache[lockingRef]{}
)

// lockingRef is one LockingKey question: a derivation and an identity.
type lockingRef struct {
	d  pushdrop.Derivation
	id [33]byte
}

type keyCache[K comparable] struct {
	mu sync.Mutex
	m  map[K]*ec.PublicKey
}

func (c *keyCache[K]) get(k K) (*ec.PublicKey, bool) {
	c.mu.Lock()
	p, ok := c.m[k]
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	return copyKey(p), true
}

func (c *keyCache[K]) put(k K, p *ec.PublicKey) {
	if p == nil || p.X == nil || p.Y == nil {
		return
	}
	p = copyKey(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil || len(c.m) >= keyCacheSize {
		c.m = make(map[K]*ec.PublicKey, 64)
	}
	c.m[k] = p
}

func copyKey(p *ec.PublicKey) *ec.PublicKey {
	return &ec.PublicKey{Curve: p.Curve, X: new(big.Int).Set(p.X), Y: new(big.Int).Set(p.Y)}
}
