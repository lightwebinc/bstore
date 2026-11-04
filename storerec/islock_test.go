package storerec

import (
	"bytes"
	"math/rand/v2"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/pushdrop"
)

// lockFields are field lengths and single bytes at every boundary of the
// minimal push: OP_0, OP_1 to OP_16, OP_1NEGATE, a direct push, and
// OP_PUSHDATA1, 2 and 4.
func lockFields(rng *rand.Rand) [][]byte {
	var out [][]byte
	for _, b := range []byte{0, 1, 2, 16, 17, 0x80, 0x81, 0x82, 0xff} {
		out = append(out, []byte{b})
	}
	for _, n := range []int{0, 2, 33, 75, 76, 255, 256, 65535, 65536, 70000} {
		f := make([]byte, n)
		for i := range f {
			f[i] = byte(rng.Uint32())
		}
		out = append(out, f)
	}
	return out
}

// isLock agrees with comparing against pushdrop.Script on the canonical
// lock of every field shape, with and without a signature, and on every
// change of one byte at the lock's joints, a byte cut or added, a push
// spelled another way and another key.
func TestIsLockAgreesWithTheRebuild(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	k1, _ := ec.NewPrivateKey()
	k2, _ := ec.NewPrivateKey()
	keys := []*ec.PublicKey{k1.PubKey(), k2.PubKey()}
	fs := lockFields(rng)
	sigs := [][]byte{nil, {}, {0x30, 0x44}, bytes.Repeat([]byte{0x30}, 71), {7}}
	agree := func(s []byte, key *ec.PublicKey, fields [][]byte, sig []byte) {
		t.Helper()
		want := bytes.Equal(s, pushdrop.Script(key, fields, sig))
		if got := isLock(s, key, fields, sig); got != want {
			t.Fatalf("isLock %v, the rebuild %v (fields %d, lens %d, sig %x, script %d bytes)", got, want, len(fields), lens(fields), sig, len(s))
		}
	}
	checked := 0
	for i, f := range fs {
		for _, sig := range sigs {
			for _, fields := range [][][]byte{{f}, {f, fs[(i+3)%len(fs)]}, {f, fs[(i+5)%len(fs)], fs[(i+7)%len(fs)]}} {
				key := keys[i%2]
				s := pushdrop.Script(key, fields, sig)
				if !isLock(s, key, fields, sig) {
					t.Fatalf("the canonical lock refused (fields %v, sig %x)", lens(fields), sig)
				}
				agree(s, keys[(i+1)%2], fields, sig)
				agree(s, key, fields, nil)
				agree(s, key, fields[:len(fields)-1], sig)
				agree(s[:len(s)-1], key, fields, sig)
				agree(append(append([]byte(nil), s...), 0x75), key, fields, sig)
				// Every byte of the first and last 80, and some between.
				pos := map[int]bool{}
				for p := 0; p < 80 && p < len(s); p++ {
					pos[p], pos[len(s)-1-p] = true, true
				}
				for range 40 {
					pos[rng.IntN(len(s))] = true
				}
				for p := range pos {
					for _, x := range []byte{0x01, 0x80, 0xff} {
						m := append([]byte(nil), s...)
						m[p] ^= x
						agree(m, key, fields, sig)
					}
				}
				checked++
			}
		}
	}
	// A push spelled long where a short one is written.
	key := keys[0]
	f := []byte("a field of twenty bytes")
	long := append([]byte{33}, key.Compressed()...)
	long = append(long, 0xac, 0x4c, byte(len(f)))
	long = append(append(long, f...), 0x75)
	agree(long, key, [][]byte{f}, nil)
	agree(append(append([]byte{33}, key.Compressed()...), 0xac, 0x01, 0x05, 0x75), key, [][]byte{{5}}, nil)
	if checked == 0 {
		t.Fatal("nothing checked")
	}
}

func lens(fs [][]byte) []int {
	var out []int
	for _, f := range fs {
		out = append(out, len(f))
	}
	return out
}

// FuzzIsLock holds isLock to the rebuild on any script against one field and
// an optional signature.
func FuzzIsLock(f *testing.F) {
	k, _ := ec.NewPrivateKey()
	key := k.PubKey()
	f.Add(pushdrop.Script(key, [][]byte{[]byte("record")}, []byte{0x30, 1}), []byte("record"), []byte{0x30, 1}, true)
	f.Add(pushdrop.Script(key, [][]byte{{0}}, nil), []byte{0}, []byte(nil), false)
	f.Add(pushdrop.Script(key, [][]byte{{0x81}}, []byte{}), []byte{0x81}, []byte{}, true)
	f.Fuzz(func(t *testing.T, s, field, sig []byte, signed bool) {
		if !signed {
			sig = nil
		} else if sig == nil {
			sig = []byte{}
		}
		fields := [][]byte{field}
		if want, got := bytes.Equal(s, pushdrop.Script(key, fields, sig)), isLock(s, key, fields, sig); got != want {
			t.Fatalf("isLock %v, the rebuild %v", got, want)
		}
		c := pushdrop.Script(key, fields, sig)
		if !isLock(c, key, fields, sig) {
			t.Fatal("the canonical lock refused")
		}
	})
}
