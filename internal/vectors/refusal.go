package vectors

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/guard"

	"github.com/lightwebinc/bstore/internal/storerec"
)

type refusalCase struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Hex   string `json:"hex"`
	Label string `json:"label"`
	Rule  string `json:"rule,omitempty"`
	// Fill, when set, stands for a run of one byte the hex leaves out:
	// the bytes are hex[:At], then Len bytes of Byte, then hex[At:] (At in
	// bytes).
	Fill *fill `json:"fill,omitempty"`
}

type fill struct {
	At   int  `json:"at"`
	Byte byte `json:"byte"`
	Len  int  `json:"len"`
}

// compact replaces the longest run of one byte value, when it is longer than
// 4096 bytes, by a fill, so that a vector at a megabyte bound stays small.
func compact(c *refusalCase) {
	b := unhex(c.Hex)
	best, at := 0, 0
	for i := 0; i < len(b); {
		j := i
		for j < len(b) && b[j] == b[i] {
			j++
		}
		if j-i > best {
			best, at = j-i, i
		}
		i = j
	}
	if best <= 4096 {
		return
	}
	c.Fill = &fill{At: at, Byte: b[at], Len: best}
	c.Hex = hx(append(append([]byte{}, b[:at]...), b[at+best:]...))
}

// expand is the bytes a case stands for.
func expand(c refusalCase) []byte {
	b := unhex(c.Hex)
	if c.Fill == nil {
		return b
	}
	out := append([]byte{}, b[:c.Fill.At]...)
	out = append(out, bytes.Repeat([]byte{c.Fill.Byte}, c.Fill.Len)...)
	return append(out, b[c.Fill.At:]...)
}

func toMap(r encoder) cbor.Map {
	b := must(r.Encode())
	v := must(cbor.DecodeValue(b))
	return v.(cbor.Map)
}

func with(m cbor.Map, k any, v cbor.Value) cbor.Map {
	out := make(cbor.Map, 0, len(m)+1)
	done := false
	for _, p := range m {
		if p.Key == k {
			out = append(out, cbor.Pair{Key: k, Val: v})
			done = true
			continue
		}
		out = append(out, p)
	}
	if !done {
		out = append(out, cbor.Pair{Key: k, Val: v})
	}
	return out
}

func without(m cbor.Map, k uint64) cbor.Map {
	out := make(cbor.Map, 0, len(m))
	for _, p := range m {
		if p.Key != k {
			out = append(out, p)
		}
	}
	return out
}

// padTo adds key k holding a byte string sized so that the map encodes to
// exactly n bytes.
func padTo(m cbor.Map, k uint64, n int) []byte {
	for l := max(0, n-len(enc(m))-16); l <= n; l++ {
		b := enc(with(m, k, make([]byte, l)))
		if len(b) == n {
			return b
		}
		if len(b) > n {
			break
		}
	}
	panic(fmt.Sprintf("cannot pad to %d", n))
}

// Bad keys: each breaks the key rule a different way.
var (
	key32     = bytes.Repeat([]byte{0x02}, 32)
	key04     = append([]byte{0x04}, Bob.Pub()[1:]...)
	keyAlias  = append([]byte{0x02}, bytes.Repeat([]byte{0xff}, 32)...)
	keyOffCrv = func() []byte {
		for x := int64(1); ; x++ {
			k := append([]byte{0x02}, make([]byte, 32)...)
			b := big.NewInt(x).Bytes()
			copy(k[33-len(b):], b)
			if _, err := guard.ParsePubKey(k); err != nil {
				return k
			}
		}
	}()
	badKeys = []struct {
		name string
		k    []byte
	}{{"32 bytes", key32}, {"prefix 04", key04}, {"x at or above the field prime", keyAlias},
		{"x not on the curve", keyOffCrv}, {"the anyone key", anyonePK}}
)

// rawCBOR are byte strings that are not one canonical CBOR value of the
// subset.
var rawCBOR = []struct {
	name string
	b    []byte
}{
	{"not CBOR", []byte{0xff}},
	{"truncated", []byte{0xa1, 0x00}},
	{"trailing bytes", append(enc(cbor.Map{{Key: uint64(0), Val: []byte("x")}}), 0x00)},
	{"keys out of order", []byte{0xa2, 0x01, 0x00, 0x00, 0x00}},
	{"duplicate key", []byte{0xa2, 0x00, 0x00, 0x00, 0x00}},
	{"non-shortest integer", []byte{0xa1, 0x18, 0x00, 0x00}},
	{"indefinite-length map", []byte{0xbf, 0x00, 0x00, 0xff}},
	{"a float", []byte{0xa1, 0x00, 0xf9, 0x3c, 0x00}},
	{"a tag", []byte{0xa1, 0x00, 0xc1, 0x00}},
	{"invalid UTF-8 text", []byte{0xa1, 0x00, 0x61, 0xff}},
	{"nesting deeper than 16", append([]byte{0xa1, 0x00}, append(bytes.Repeat([]byte{0x81}, 17), 0x00)...)},
}

// recordSteps are the shared record steps every record refuses in this
// order: too-large, cbor, key-type, magic.
func recordSteps(kind string, valid cbor.Map, max int, magic []byte, lastKey uint64) []refusalCase {
	var out []refusalCase
	add := func(name string, b []byte, label string) {
		out = append(out, refusalCase{Kind: kind, Name: name, Hex: hx(b), Label: label})
	}
	add("one byte over the bound, whatever it is", make([]byte, max+1), "too-large")
	add("one byte over the bound, a record otherwise valid", padTo(valid, lastKey+20, max+1), "too-large")
	for _, c := range rawCBOR {
		add(c.name, c.b, "cbor")
	}
	add("an array, not a map", enc([]cbor.Value{magic}), "cbor")
	big := cbor.Map{}
	for i := uint64(0); i < 65; i++ {
		big = with(big, i, uint64(0))
	}
	add("more than 64 keys", enc(with(big, uint64(0), magic)), "too-large")
	add("a text key", enc(with(valid, "k", uint64(1))), "key-type")
	add("a negative key", enc(with(valid, int64(-1), uint64(1))), "key-type")
	add("another magic", enc(with(valid, uint64(0), []byte("stx\x01"))), "magic")
	add("magic version 2", enc(with(valid, uint64(0), append(append([]byte{}, magic[:3]...), 2))), "magic")
	add("magic as text", enc(with(valid, uint64(0), string(magic))), "magic")
	add("no magic", enc(without(valid, 0)), "magic")
	return out
}

func genRefusal(files map[string][]byte) error {
	var cases []refusalCase
	cases = append(cases, bucketRefusals()...)
	cases = append(cases, entryRefusals()...)
	cases = append(cases, pieceRefusals()...)
	cases = append(cases, frontRefusals()...)
	cases = append(cases, outcomeRefusals()...)
	cases = append(cases, requestRefusals()...)
	var ok []refusalCase
	for i, c := range cases {
		_, _, err := decodeKind(c.Kind, expand(c))
		got := reason(err)
		rule := storerec.Rule(err)
		if got != c.Label {
			return fmt.Errorf("refusal %s %q: got %s (%v), want %s", c.Kind, c.Name, got, err, c.Label)
		}
		if c.Label == "ok" {
			ok = append(ok, c)
		}
		cases[i].Rule = rule
		compact(&cases[i])
		if !bytes.Equal(expand(cases[i]), expand(c)) {
			return fmt.Errorf("refusal %s: compaction", c.Name)
		}
	}
	return put(files, "refusal-v1.json", map[string]any{
		"note":  "every step of every record's refusal order; each case is refused for its label (rule: the codec's finer name where a label covers several rules), or admitted (ok) at a bound",
		"cases": cases,
	})
}

type caseList struct {
	kind  string
	cases []refusalCase
}

func (l *caseList) add(name string, b []byte, label string) {
	l.cases = append(l.cases, refusalCase{Kind: l.kind, Name: name, Hex: hx(b), Label: label})
}

func (l *caseList) m(name string, m cbor.Map, label string) { l.add(name, enc(m), label) }

// keyCases adds the missing, type and identity cases of a key field.
func (l *caseList) keyCases(valid cbor.Map, k uint64, what string) {
	l.m(what+" missing", without(valid, k), "missing")
	l.m(what+" as text", with(valid, k, "02"), "type")
	for _, bk := range badKeys {
		l.m(what+" "+bk.name, with(valid, k, bk.k), "identity")
	}
}

func (l *caseList) bucketCases(valid cbor.Map, k uint64) {
	l.m("bucket missing", without(valid, k), "missing")
	l.m("bucket as bytes", with(valid, k, []byte(Bucket)), "type")
	for _, b := range []string{"Photos_qzvmkwerty", "photos__x_qzvmkwerty", "photos_qzvmkwert", "photos_qzvmkwertyy",
		"_photos_qzvmkwerty", "photos__qzvmkwerty", "photos1_qzvmkwerty", "photo_s_qzvmkwerty_", "qzvmkwerty",
		strings.Repeat("a", 26) + "_qzvmkwerty", "p_qzvmkwerTy", "p-q_qzvmkwerty"} {
		l.m("bucket "+b, with(valid, k, b), "bucket")
	}
	l.m("bucket name of 25 bytes", with(valid, k, strings.Repeat("a", 25)+"_qzvmkwerty"), "ok")
	l.m("bucket name of 1 byte", with(valid, k, "a_qzvmkwerty"), "ok")
	l.m("bucket name with single underscores", with(valid, k, "a_b_c_qzvmkwerty"), "ok")
}

func (l *caseList) createdCases(valid cbor.Map, k uint64) {
	l.m("created missing", without(valid, k), "missing")
	l.m("created negative", with(valid, k, int64(-1)), "type")
	l.m("created 0", with(valid, k, uint64(0)), "created")
	l.m("created 1", with(valid, k, uint64(1)), "ok")
	l.m("created at its bound", with(valid, k, uint64(storerec.MaxTime)), "ok")
	l.m("created one over its bound", with(valid, k, uint64(storerec.MaxTime+1)), "created")
}

func (l *caseList) b32Cases(valid cbor.Map, k uint64, what string) {
	l.m(what+" missing", without(valid, k), "missing")
	l.m(what+" of 31 bytes", with(valid, k, make([]byte, 31)), "type")
	l.m(what+" of 33 bytes", with(valid, k, make([]byte, 33)), "type")
	l.m(what+" as text", with(valid, k, strings.Repeat("a", 32)), "type")
}

func objKeyCases(add func(name string, k cbor.Value, label string)) {
	add("an empty key", "", "objkey")
	add("a key with a newline", "a\nb", "objkey")
	add("a key with DEL", "a\x7fb", "objkey")
	add("a key with U+0085", "a\u0085b", "objkey")
	add("a key with U+2028", "a b", "objkey")
	add("a key with U+2029", "a b", "objkey")
	add("a key of 1025 bytes", strings.Repeat("k", 1025), "objkey")
	add("a key of 1024 bytes", strings.Repeat("k", 1024), "ok")
	add("a key of 1024 bytes ending in a two-byte character", strings.Repeat("k", 1022)+"é", "ok")
	add("a key of 1025 bytes ending in a two-byte character", strings.Repeat("k", 1023)+"é", "objkey")
	add("a key with U+00A0 and an emoji", "a \U0001F600", "ok")
}

func bucketRefusals() []refusalCase {
	b1 := toMap(sampleBucket1())
	b0 := toMap(sampleBucket0())
	l := &caseList{kind: "bucket"}
	l.cases = recordSteps("bucket", b1, storerec.MaxBucketRecord, storerec.MagicBucket, storerec.BKLast)
	l.add("exactly 65,536 bytes", padTo(b1, 40, storerec.MaxBucketRecord), "ok")
	l.bucketCases(b1, storerec.BKBucket)
	l.keyCases(b1, storerec.BKOwner, "owner")
	l.b32Cases(b1, storerec.BKID, "id")
	l.m("seq missing", without(b1, storerec.BKSeq), "missing")
	l.m("seq negative", with(b1, storerec.BKSeq, int64(-1)), "type")
	l.m("seq as text", with(b1, storerec.BKSeq, "1"), "type")
	l.m("seq 2^53 - 1", with(b1, storerec.BKSeq, uint64(storerec.MaxSafe)), "ok")
	l.m("seq 2^53", with(b1, storerec.BKSeq, uint64(storerec.MaxSafe+1)), "seq")
	l.b32Cases(b1, storerec.BKPrev, "prev")
	// writers
	l.m("writers missing", without(b1, storerec.BKWriters), "missing")
	l.m("writers a map", with(b1, storerec.BKWriters, cbor.Map{}), "type")
	ws := func(n int) []cbor.Value {
		ks := sortedKeys(n)
		out := make([]cbor.Value, n)
		for i := range ks {
			out[i] = ks[i]
		}
		return out
	}
	l.m("16 writers", with(b1, storerec.BKWriters, ws(16)), "ok")
	l.m("17 writers", with(b1, storerec.BKWriters, ws(17)), "writers")
	for _, bk := range badKeys {
		l.m("a writer "+bk.name, with(b1, storerec.BKWriters, []cbor.Value{bk.k}), "identity")
	}
	l.m("a writer as text", with(b1, storerec.BKWriters, []cbor.Value{"02"}), "identity")
	two := ws(2)
	l.m("writers descending", with(b1, storerec.BKWriters, []cbor.Value{two[1], two[0]}), "writers")
	l.m("a writer twice", with(b1, storerec.BKWriters, []cbor.Value{two[0], two[0]}), "writers")
	l.m("the owner a writer", with(b1, storerec.BKWriters, []cbor.Value{Owner.Pub()}), "writers")
	l.m("17 writers, one of them bad (the count first)", with(b1, storerec.BKWriters, append(ws(16), anyonePK)), "writers")
	l.m("writers descending and one bad (identity first)", with(b1, storerec.BKWriters, []cbor.Value{two[1], two[0], key32}), "identity")
	// changes
	put := func(k string, c string) cbor.Value { return []cbor.Value{k, must32(c)} }
	rem := func(k string, c string) cbor.Value { return []cbor.Value{k, make([]byte, 32), must32(c)} }
	ch := func(cs ...cbor.Value) cbor.Map { return with(b1, storerec.BKChanges, cs) }
	l.m("changes missing", without(b1, storerec.BKChanges), "missing")
	l.m("changes a byte string", with(b1, storerec.BKChanges, []byte{}), "type")
	many := func(n int) []cbor.Value {
		out := make([]cbor.Value, n)
		for i := range out {
			out[i] = put(fmt.Sprintf("k%05d", i), fmt.Sprintf("c%d", i))
		}
		return out
	}
	l.m("1024 changes", ch(many(1024)...), "map-ok")
	l.m("1025 changes", ch(many(1025)...), "changes")
	l.m("a change of one element", ch([]cbor.Value{"a"}), "changes")
	l.m("a change of four elements", ch([]cbor.Value{"a", make([]byte, 32), make([]byte, 32), must32("x")}), "changes")
	l.m("a change not an array", ch("a"), "changes")
	l.m("a change's key as bytes", ch([]cbor.Value{[]byte("a"), must32("x")}), "changes")
	objKeyCases(func(name string, k cbor.Value, label string) {
		if label == "ok" {
			label = "map-ok"
		} else {
			label = "changes"
		}
		l.m("a change with "+name, ch([]cbor.Value{k, must32("x")}), label)
	})
	l.m("a put's C zero", ch([]cbor.Value{"a", make([]byte, 32)}), "changes")
	l.m("a put's C of 31 bytes", ch([]cbor.Value{"a", make([]byte, 31)}), "changes")
	l.m("a removal's Z not zero", ch([]cbor.Value{"a", must32("z"), must32("d")}), "changes")
	l.m("a removal's Z of 31 bytes", ch([]cbor.Value{"a", make([]byte, 31), must32("d")}), "changes")
	l.m("a removal's D zero", ch([]cbor.Value{"a", make([]byte, 32), make([]byte, 32)}), "changes")
	l.m("changes descending", ch(put("b", "1"), put("a", "2")), "changes")
	l.m("a key twice", ch(put("a", "1"), rem("a", "2")), "changes")
	l.m("a key and its extension, ascending by bytes", ch(put("a", "1"), put("a/", "2"), put("ab", "3")), "map-ok")
	l.m("keys compared by bytes, not by code point", ch(put("￿", "1"), put("\U00010000", "2")), "map-ok")
	l.m("keys compared by bytes, descending", ch(put("\U00010000", "1"), put("￿", "2")), "changes")
	// count
	l.m("count missing", without(b1, storerec.BKCount), "missing")
	l.m("count as text", with(b1, storerec.BKCount, "2"), "type")
	l.m("count 1,048,576", with(b1, storerec.BKCount, uint64(storerec.MaxKeysInMap)), "ok")
	l.m("count 1,048,577", with(b1, storerec.BKCount, uint64(storerec.MaxKeysInMap+1)), "count")
	l.b32Cases(b1, storerec.BKMapRoot, "mapRoot")
	l.createdCases(b1, storerec.BKCreated)
	// consumed
	cs := func(n int) []cbor.Value {
		out := make([]cbor.Value, n)
		for i := range out {
			x := make([]byte, 32)
			x[30], x[31] = byte(i>>8), byte(i)
			x[0] = 0x80
			out[i] = x
		}
		return out
	}
	l.m("consumed missing", without(b1, storerec.BKConsumed), "missing")
	l.m("consumed a map", with(b1, storerec.BKConsumed, cbor.Map{}), "type")
	l.m("1024 consumed", with(b1, storerec.BKConsumed, cs(1024)), "ok")
	l.m("1025 consumed", with(b1, storerec.BKConsumed, cs(1025)), "consumed")
	l.m("a consumed of 31 bytes", with(b1, storerec.BKConsumed, []cbor.Value{make([]byte, 31)}), "consumed")
	l.m("a consumed as text", with(b1, storerec.BKConsumed, []cbor.Value{"x"}), "consumed")
	c2 := cs(2)
	l.m("consumed descending", with(b1, storerec.BKConsumed, []cbor.Value{c2[1], c2[0]}), "consumed")
	l.m("consumed twice", with(b1, storerec.BKConsumed, []cbor.Value{c2[0], c2[0]}), "consumed")
	l.m("consumed names a put's C", with(b1, storerec.BKConsumed, []cbor.Value{lb("C a/1")}), "consumed")
	l.m("consumed names a removal's D", with(with(b1, storerec.BKChanges, []cbor.Value{rem("a", "D a")}), storerec.BKConsumed, []cbor.Value{must32("D a")}), "consumed")
	// first
	l.m("seq 0, the first token", b0, "ok")
	l.m("seq 0 with prev not zero", with(b0, storerec.BKPrev, must32("p")), "first")
	l.m("seq 0 with a change", with(b0, storerec.BKChanges, []cbor.Value{put("a", "1")}), "first")
	l.m("seq 0 with a removal", with(b0, storerec.BKChanges, []cbor.Value{rem("a", "1")}), "first")
	l.m("seq 0 with a consumed", with(b0, storerec.BKConsumed, cs(1)), "first")
	l.m("seq 0 with count 1", with(b0, storerec.BKCount, uint64(1)), "first")
	l.m("seq 0 with another mapRoot", with(b0, storerec.BKMapRoot, must32("root")), "first")
	l.m("seq 0 with writers (allowed)", with(b0, storerec.BKWriters, []cbor.Value{Alice.Pub()}), "ok")
	l.m("seq 1 with prev zero", with(b1, storerec.BKPrev, make([]byte, 32)), "first")
	// order: the first broken rule wins.
	l.m("order: bucket before owner", with(with(b1, storerec.BKBucket, "X"), storerec.BKOwner, key32), "bucket")
	l.m("order: owner missing before id wrong", with(without(b1, storerec.BKOwner), storerec.BKID, "x"), "missing")
	l.m("order: changes before count", with(with(b1, storerec.BKChanges, "x"), storerec.BKCount, uint64(storerec.MaxKeysInMap+1)), "type")
	l.m("order: consumed before first", with(with(b0, storerec.BKConsumed, []cbor.Value{"x"}), storerec.BKPrev, must32("p")), "consumed")
	l.m("order: magic before everything", with(with(b1, uint64(0), []byte("xxx\x01")), storerec.BKBucket, uint64(1)), "magic")
	for i := range l.cases {
		if l.cases[i].Label == "map-ok" {
			// A record whose changes are well formed but whose count and
			// mapRoot are not their fold: the record rules admit it; the fold
			// is the index's (map-v1.json).
			l.cases[i].Label = "ok"
		}
	}
	return l.cases
}

func must32(s string) []byte { l := label("c:%s", s); return l[:] }

func lb(s string) []byte { l := label("%s", s); return l[:] }

// sortedKeys are n distinct identity keys, ascending.
func sortedKeys(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = party(fmt.Sprintf("w%d", i), byte(0x10+i)).Pub()
	}
	for i := 1; i < n; i++ {
		for j := i; j > 0 && bytes.Compare(out[j-1], out[j]) > 0; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
