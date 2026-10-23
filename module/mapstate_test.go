package module

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lightwebinc/bstore/storerec"
)

// The host's map state against the codec's (storerec keymap.go), which the
// golden vectors hold (mapstate.test.ts): the same fold verdicts token by
// token, the same roots, and byte-identical object answers and pages, each
// passing a reader's checks.

// rng is a deterministic generator, so a failure is reproducible.
type rng struct{ s uint32 }

func (r *rng) next() float64 {
	r.s = r.s*1664525 + 1013904223
	return float64(r.s) / (1 << 32)
}

var alphabet = []string{"a", "b", "c", "é", "ß", "中", "\U0001F600", "Ａ", "", "/", "A", "0"}

func (r *rng) key() string {
	var sb strings.Builder
	n := 1 + int(r.next()*4)
	for range n {
		sb.WriteString(alphabet[int(r.next()*float64(len(alphabet)))])
	}
	return sb.String()
}

var counter int

func fresh() [32]byte {
	counter++
	return sha(fmt.Sprintf("c/%d", counter))
}

// TestKeyOrder: keys sort as their UTF-8 bytes do, which is Go's string
// order and the codec's (the TypeScript cmpKey's reason to exist).
func TestKeyOrder(t *testing.T) {
	r := &rng{s: 7}
	for range 20000 {
		a, b := r.key(), r.key()
		if !utf8.ValidString(a) || (a < b) != (bytes.Compare([]byte(a), []byte(b)) < 0) {
			t.Fatalf("%q %q", a, b)
		}
	}
	m := newMapState()
	keys := storerec.Map{"b": fresh(), "é": fresh(), "\U0001F600": fresh(), "Ａ": fresh(), "a": fresh()}
	var changes []storerec.Change
	for _, k := range keys.Keys() {
		changes = append(changes, storerec.Change{Key: k, C: keys[k]})
	}
	if why := m.apply(&storerec.Bucket{Changes: changes, Count: 5, MapRoot: keys.Root()}); why != "" {
		t.Fatal(why)
	}
	if strings.Join(m.keys, ",") != strings.Join(keys.Keys(), ",") {
		t.Fatal(m.keys)
	}
}

// chainOf is a chain of honest tokens over random puts and removals, with a
// dishonest one now and then.
func chainOf(seed uint32, tokens int) []*storerec.Bucket {
	r := &rng{s: seed}
	var out []*storerec.Bucket
	m := storerec.Map{}
	named := map[[32]byte]bool{}
	for seq := range tokens {
		b := &storerec.Bucket{Bucket: bucket, Seq: uint64(seq), MapRoot: m.Root()}
		if seq > 0 {
			var batch []storerec.Applicable
			for range 1 + int(r.next()*6) {
				k := r.key()
				if r.next() >= 0.6 && len(m) > 0 {
					ks := m.Keys()
					k = ks[int(r.next()*float64(len(ks)))]
				}
				kind := uint64(storerec.KindPut)
				if r.next() < 0.25 {
					kind = storerec.KindDelete
				}
				batch = append(batch, storerec.Applicable{C: fresh(), Key: k, Kind: kind, Created: uint64(r.next() * 5)})
			}
			changes, consumed := storerec.ResolveBatch(m, named, batch)
			next := m.Clone()
			nextNamed := map[[32]byte]bool{}
			for k, v := range named {
				nextNamed[k] = v
			}
			for _, ch := range changes {
				if ch.Removal {
					delete(next, ch.Key)
				} else {
					next[ch.Key] = ch.C
				}
				nextNamed[ch.C] = true
			}
			for _, c := range consumed {
				nextNamed[c] = true
			}
			b.Changes, b.Consumed, b.Count, b.MapRoot = changes, consumed, uint64(len(next)), next.Root()
			// Now and then a dishonest token: each reason the fold names.
			bad := r.next()
			switch {
			case bad < 0.04 && len(changes) > 0:
				b.MapRoot = sha("wrong")
			case bad < 0.07:
				b.Count++
			case bad < 0.09 && len(m) > 0:
				k0 := m.Keys()[0]
				var cs []storerec.Change
				for _, c := range changes {
					if c.Key != k0 {
						cs = append(cs, c)
					}
				}
				cs = append(cs, storerec.Change{Key: k0, C: m[k0]})
				sort.Slice(cs, func(i, j int) bool { return cs[i].Key < cs[j].Key })
				b.Changes = cs
			case bad < 0.11:
				var cs []storerec.Change
				for _, c := range changes {
					if c.Key != "zzz-absent" {
						cs = append(cs, c)
					}
				}
				cs = append(cs, storerec.Change{Key: "zzz-absent", C: fresh(), Removal: true})
				sort.Slice(cs, func(i, j int) bool { return cs[i].Key < cs[j].Key })
				b.Changes = cs
			case bad < 0.13 && len(out) > 1 && len(out[1].Changes) > 0:
				cs := append(append([][32]byte(nil), consumed...), out[1].Changes[0].C)
				sort.Slice(cs, func(i, j int) bool { return bytes.Compare(cs[i][:], cs[j][:]) < 0 })
				b.Consumed = cs
			default:
				m, named = next, nextNamed
			}
		}
		out = append(out, b)
	}
	return out
}

func TestFoldAgainstTheCodec(t *testing.T) {
	reasons := map[string]bool{}
	for s := uint32(1); s <= 1000; s++ {
		tokens := chainOf(s, 12)
		want, err := storerec.FoldChain(tokens)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range want.Reasons {
			if x != "" {
				reasons[x] = true
			}
		}
		m := newMapState()
		for i, tk := range tokens {
			r := m.apply(tk)
			if r != want.Reasons[i] {
				t.Fatalf("chain %d seq %d: %q, want %q", s, i, r, want.Reasons[i])
			}
			status := storerec.StatusConsistent
			if r != "" {
				status = storerec.StatusMapMismatch
			}
			if status != want.Statuses[i] {
				t.Fatalf("chain %d seq %d: %s, want %s", s, i, status, want.Statuses[i])
			}
			if r != "" {
				break
			}
		}
		if m.root() != want.Map.Root() || strings.Join(m.keys, "\x00") != strings.Join(want.Map.Keys(), "\x00") {
			t.Fatalf("chain %d: the map differs", s)
		}
	}
	// Every reason a chain can reach was reached.
	var got []string
	for r := range reasons {
		got = append(got, r)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "count,map-root,named-twice,removal-of-absent" {
		t.Fatal(got)
	}
}

func encLeaves(t *testing.T, l []storerec.MapLeaf) string {
	t.Helper()
	b, err := storerec.EncodeTokenContext(l)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func encCtx(t *testing.T, c *storerec.EntryContext) string {
	t.Helper()
	if c == nil {
		return ""
	}
	b, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func TestAnswersAgainstTheCodec(t *testing.T) {
	r := &rng{s: 99}
	for range 300 {
		n := int(r.next() * 40)
		km := storerec.Map{}
		for range n {
			km[r.key()] = fresh()
		}
		keys := km.Keys()
		st := newMapState()
		var changes []storerec.Change
		for _, k := range keys {
			changes = append(changes, storerec.Change{Key: k, C: km[k]})
		}
		if why := st.apply(&storerec.Bucket{Changes: changes, Count: uint64(len(keys)), MapRoot: km.Root()}); why != "" {
			t.Fatal(why)
		}
		unheld := map[[32]byte]bool{}
		for _, k := range keys {
			if r.next() < 0.1 {
				unheld[km[k]] = true
			}
		}
		held := func(c [32]byte) bool { return !unheld[c] }
		for range 20 {
			k := r.key()
			if r.next() < 0.5 && len(keys) > 0 {
				k = keys[int(r.next()*float64(len(keys)))]
			}
			a1, l1 := km.ProveObject(k, held)
			a2, l2 := st.proveObject(k, held)
			if encLeaves(t, l2) != encLeaves(t, l1) || encCtx(t, a2) != encCtx(t, a1) {
				t.Fatalf("object %q differs", k)
			}
			var entry *[32]byte
			if a2 != nil {
				c := km[k]
				entry = &c
			}
			if _, _, err := storerec.CheckObject(uint64(len(km)), km.Root(), k, entry, a2, l2); err != nil {
				t.Fatalf("object %q: %v", k, err)
			}
			p := ""
			if r.next() >= 0.4 {
				p = string([]rune(r.key())[0])
			}
			a := ""
			if r.next() >= 0.5 {
				a = r.key()
			}
			max := 1 + int(r.next()*6)
			e1, p1 := km.ProvePage(p, a, max, held)
			e2, p2 := st.provePage(p, a, max, held)
			if encLeaves(t, p2) != encLeaves(t, p1) || len(e1) != len(e2) {
				t.Fatalf("page %q %q %d differs", p, a, max)
			}
			for i := range e1 {
				if e1[i].Key != e2[i].Key || e1[i].C != e2[i].C || encCtx(t, &e1[i].Ctx) != encCtx(t, &e2[i].Ctx) {
					t.Fatalf("page %q %q %d entry %d differs", p, a, max, i)
				}
			}
			if err := storerec.CheckPage(uint64(len(km)), km.Root(), p, a, max, e2, p2); err != nil {
				t.Fatalf("page %q %q %d: %v", p, a, max, err)
			}
		}
	}
}
