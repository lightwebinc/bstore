package storerec

import (
	"bytes"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/pushdrop"
)

func randomIdentity(t *testing.T) []byte {
	t.Helper()
	k, err := ec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k.PubKey().Compressed()
}

func cached[K comparable](c *keyCache[K], k K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.m[k]
	return ok
}

func samePoint(a, b *ec.PublicKey) bool {
	return a.X.Cmp(b.X) == 0 && a.Y.Cmp(b.Y) == 0
}

// CheckKey answers a remembered key as it answered it first, as a copy of
// its own; a refusal is never remembered.
func TestCheckKeyRemembered(t *testing.T) {
	for range 50 {
		b := randomIdentity(t)
		want, err := guard.ParsePubKey(b)
		if err != nil {
			t.Fatal(err)
		}
		first, err := CheckKey(b)
		if err != nil || !samePoint(first, want) || !cached(checkedKeys, [33]byte(b)) {
			t.Fatalf("first: %v", err)
		}
		first.X.SetInt64(7) // a caller that writes to its key
		second, err := CheckKey(b)
		if err != nil || !samePoint(second, want) || second == first || !bytes.Equal(second.Compressed(), b) {
			t.Fatalf("remembered: %v", err)
		}
	}
	notOnCurve := append([]byte{0x02}, bytes.Repeat([]byte{0}, 31)...)
	notOnCurve = append(notOnCurve, 5) // x = 5: x^3 + 7 is not a square
	for _, b := range [][]byte{AnyoneKey, notOnCurve, randomIdentity(t)[:32], nil} {
		for range 2 {
			if _, err := CheckKey(b); err == nil {
				t.Fatalf("%x accepted", b)
			}
		}
		if len(b) == 33 && cached(checkedKeys, [33]byte(b)) {
			t.Fatalf("%x remembered as a refusal", b)
		}
	}
}

// LockingKey answers what the derivation answers, remembered or not, under
// each derivation separately.
func TestLockingKeyRemembered(t *testing.T) {
	ds := []pushdrop.Derivation{BucketDerivation, ChainDerivation, EntryDerivation, PieceDerivation, OutcomeDerivation, FrontDerivation,
		HoldDerivation, ChallengeDerivation, FundDerivation, AdDerivation}
	for range 20 {
		id := randomIdentity(t)
		pub, err := guard.ParsePubKey(id)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range ds {
			want, err := d.ExpectedLockingKey(pub)
			if err != nil {
				t.Fatal(err)
			}
			first, err := LockingKey(d, id)
			if err != nil || !samePoint(first, want) || !cached(lockingKeys, lockingRef{d: d, id: [33]byte(id)}) {
				t.Fatalf("first: %v", err)
			}
			first.Y.SetInt64(7)
			second, err := LockingKey(d, id)
			if err != nil || !samePoint(second, want) || second == first {
				t.Fatalf("remembered: %v", err)
			}
		}
	}
	for _, id := range [][]byte{nil, randomIdentity(t)[:32], append([]byte{0x04}, randomIdentity(t)[1:]...)} {
		if _, err := LockingKey(EntryDerivation, id); err != ErrIdentity {
			t.Fatalf("%x: %v", id, err)
		}
	}
}

// A full cache starts over: it never holds more than keyCacheSize keys.
func TestKeyCacheBounded(t *testing.T) {
	c := &keyCache[int]{}
	k := &ec.PublicKey{Curve: ec.S256(), X: ec.S256().Params().Gx, Y: ec.S256().Params().Gy}
	for i := range 3*keyCacheSize + 5 {
		c.put(i, k)
		if len(c.m) > keyCacheSize {
			t.Fatalf("%d keys held", len(c.m))
		}
	}
	if _, ok := c.get(3*keyCacheSize + 4); !ok {
		t.Fatal("the last key put is not held")
	}
	c.put(-1, &ec.PublicKey{})
	if _, ok := c.get(-1); ok {
		t.Fatal("a key with no point was held")
	}
}

func BenchmarkCarrierKeys(b *testing.B) {
	id := func() []byte {
		k, _ := ec.NewPrivateKey()
		return k.PubKey().Compressed()
	}()
	b.Run("uncached", func(b *testing.B) {
		for b.Loop() {
			pub, _ := guard.ParsePubKey(id)
			_, _ = EntryDerivation.ExpectedLockingKey(pub)
			_, _ = guard.ParsePubKey(id)
		}
	})
	b.Run("remembered", func(b *testing.B) {
		for b.Loop() {
			_, _ = LockingKey(EntryDerivation, id)
			_, _ = CheckKey(id)
		}
	})
}
