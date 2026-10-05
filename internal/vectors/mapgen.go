package vectors

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func mapOf(keys ...string) storerec.Map {
	m := storerec.Map{}
	for _, k := range keys {
		m[k] = label("map C %s", k)
	}
	return m
}

func mapView(m storerec.Map) [][]string {
	out := [][]string{}
	for _, k := range m.Keys() {
		out = append(out, []string{k, h32(m[k])})
	}
	return out
}

func changeView(cs []storerec.Change) [][]string {
	out := [][]string{}
	for _, c := range cs {
		if c.Removal {
			out = append(out, []string{c.Key, h32([32]byte{}), h32(c.C)})
		} else {
			out = append(out, []string{c.Key, h32(c.C)})
		}
	}
	return out
}

func list32(cs [][32]byte) []string {
	out := []string{}
	for _, c := range cs {
		out = append(out, h32(c))
	}
	return out
}

func leavesView(ls []storerec.MapLeaf) string { return hx(must(storerec.EncodeTokenContext(ls))) }

func genMap(files map[string][]byte) error {
	// Roots.
	var roots []map[string]any
	for _, m := range []storerec.Map{mapOf(), mapOf("a"), mapOf("a", "b"), mapOf("a", "b", "c"),
		mapOf("photos/1", "photos/2", "photos/10", "z", "é", "\U0001F600", "a/", "a"), mapOf("k1", "k2", "k3", "k4", "k5")} {
		leaves := []string{}
		for _, l := range m.Leaves() {
			leaves = append(leaves, hx(l))
		}
		roots = append(roots, map[string]any{"map": mapView(m), "leaves": leaves, "root": h32(m.Root())})
	}

	// Folds.
	type tok struct {
		changes  []storerec.Change
		consumed [][32]byte
		count    *uint64
		root     *[32]byte
	}
	C := func(s string) [32]byte { return label("fold %s", s) }
	pc := func(k, c string) storerec.Change { return storerec.Change{Key: k, C: C(c)} }
	rem := func(k, d string) storerec.Change { return storerec.Change{Key: k, C: C(d), Removal: true} }
	chain := func(toks ...tok) []*storerec.Bucket {
		out := []*storerec.Bucket{sampleBucket0()}
		m := storerec.Map{}
		for i, t := range toks {
			for _, c := range t.changes {
				if c.Removal {
					delete(m, c.Key)
				} else {
					m[c.Key] = c.C
				}
			}
			n, r := uint64(len(m)), m.Root()
			if t.count != nil {
				n = *t.count
			}
			if t.root != nil {
				r = *t.root
			}
			b := &storerec.Bucket{Bucket: Bucket, Owner: Owner.Pub(), ID: bucketID, Seq: uint64(i + 1), Prev: label("fold prev %d", i),
				Writers: [][]byte{}, Changes: t.changes, Count: n, MapRoot: r, Created: Created, Consumed: t.consumed}
			if b.Consumed == nil {
				b.Consumed = [][32]byte{}
			}
			out = append(out, b)
		}
		return out
	}
	u := func(n uint64) *uint64 { return &n }
	r := func(s string) *[32]byte { x := label("%s", s); return &x }
	folds := []struct {
		name string
		toks []tok
	}{
		{"puts, a replacement and a removal, each token consistent", []tok{
			{changes: []storerec.Change{pc("a", "1"), pc("b", "2")}},
			{changes: []storerec.Change{pc("a", "3"), rem("b", "D2")}, consumed: [][32]byte{C("x")}},
			{changes: []storerec.Change{pc("b", "4")}}}},
		{"a removal of an absent key", []tok{{changes: []storerec.Change{pc("a", "1")}}, {changes: []storerec.Change{rem("b", "D")}}}},
		{"a put of the value the key already names", []tok{{changes: []storerec.Change{pc("a", "1")}}, {changes: []storerec.Change{pc("a", "1")}}}},
		{"a commitment an earlier token named in its changes", []tok{{changes: []storerec.Change{pc("a", "1")}}, {changes: []storerec.Change{pc("b", "1")}}}},
		{"a commitment an earlier token named in consumed", []tok{{consumed: [][32]byte{C("9")}}, {changes: []storerec.Change{pc("b", "9")}}}},
		{"a removal's D an earlier token named", []tok{{changes: []storerec.Change{pc("a", "1"), pc("b", "2")}}, {changes: []storerec.Change{rem("a", "2")}}}},
		{"a commitment named twice in one token's changes", []tok{{changes: []storerec.Change{pc("a", "1"), pc("b", "1")}}}},
		{"a commitment in consumed an earlier token consumed", []tok{{consumed: [][32]byte{C("8")}}, {consumed: [][32]byte{C("8")}}}},
		{"a count other than the map's size", []tok{{changes: []storerec.Change{pc("a", "1")}, count: u(2)}}},
		{"a mapRoot other than the map's root", []tok{{changes: []storerec.Change{pc("a", "1")}, root: r("wrong")}}},
		{"sticky: every token after a break is map-mismatch", []tok{
			{changes: []storerec.Change{pc("a", "1")}}, {changes: []storerec.Change{rem("z", "D")}}, {changes: []storerec.Change{pc("b", "2")}}}},
		{"an empty interval: no changes, a consumed entry", []tok{{changes: []storerec.Change{pc("a", "1")}}, {consumed: [][32]byte{C("noop")}}}},
	}
	var foldOut []map[string]any
	for _, f := range folds {
		toks := chain(f.toks...)
		res, err := storerec.FoldChain(toks)
		if err != nil {
			return err
		}
		var tv []map[string]any
		for _, t := range toks {
			tv = append(tv, map[string]any{"seq": t.Seq, "changes": changeView(t.Changes), "consumed": list32(t.Consumed),
				"count": t.Count, "mapRoot": h32(t.MapRoot)})
		}
		var st []string
		for _, s := range res.Statuses {
			st = append(st, string(s))
		}
		foldOut = append(foldOut, map[string]any{"name": f.name, "tokens": tv, "statuses": st, "reasons": res.Reasons,
			"map": mapView(res.Map), "mapRoot": h32(res.Map.Root())})
	}

	// Batches the tooling resolves.
	type app struct {
		c       string
		key     string
		kind    uint64
		created uint64
	}
	batches := []struct {
		name    string
		current storerec.Map
		named   []string
		entries []app
	}{
		{"several entries for one key: the last by created wins, the rest consumed", storerec.Map{},
			nil, []app{{"p1", "k", 1, 30}, {"p2", "k", 1, 10}, {"p3", "k", 1, 20}}},
		{"equal created: the higher commitment wins", storerec.Map{}, nil, []app{{"q1", "k", 1, 5}, {"q2", "k", 1, 5}, {"q3", "k", 1, 5}}},
		{"a delete of a key the map does not hold is consumed", storerec.Map{}, nil, []app{{"d1", "gone", 2, 1}}},
		{"a delete of a key the map holds is a removal naming it", storerec.Map{"k": C("p0")}, []string{"p0"}, []app{{"d2", "k", 2, 1}}},
		{"a put then a delete of one key in a batch, the delete last", storerec.Map{"k": C("p0")}, []string{"p0"}, []app{{"p4", "k", 1, 1}, {"d3", "k", 2, 2}}},
		{"a delete then a put, the put last", storerec.Map{"k": C("p0")}, []string{"p0"}, []app{{"d4", "k", 2, 1}, {"p5", "k", 1, 2}}},
		{"an applied delete found again after a later put is skipped", storerec.Map{"k": C("p6")}, []string{"d5", "p6"}, []app{{"d5", "k", 2, 1}}},
		{"a put superseded in its batch does not come back", storerec.Map{"k": C("p8")}, []string{"p7", "p8"}, []app{{"p7", "k", 1, 1}}},
		{"unknown kinds are not applied", storerec.Map{}, nil, []app{{"u1", "k", 7, 1}}},
		{"keys in order, each its own change", storerec.Map{"b": C("old")}, []string{"old"}, []app{{"c1", "c", 1, 1}, {"a1", "a", 1, 1}, {"b1", "b", 2, 1}}},
	}
	var batchOut []map[string]any
	for _, bt := range batches {
		named := map[[32]byte]bool{}
		var nv []string
		for _, n := range bt.named {
			named[C(n)] = true
			nv = append(nv, h32(C(n)))
		}
		var in []storerec.Applicable
		var ev []map[string]any
		for _, e := range bt.entries {
			in = append(in, storerec.Applicable{C: C(e.c), Key: e.key, Kind: e.kind, Created: e.created})
			ev = append(ev, map[string]any{"c": h32(C(e.c)), "key": e.key, "kind": e.kind, "created": e.created})
		}
		changes, consumed := storerec.ResolveBatch(bt.current, named, in)
		// The tooling's output always folds.
		next := &storerec.Bucket{Changes: changes, Consumed: consumed}
		m := bt.current.Clone()
		for _, c := range changes {
			if c.Removal {
				delete(m, c.Key)
			} else {
				m[c.Key] = c.C
			}
		}
		next.Count, next.MapRoot = uint64(len(m)), m.Root()
		if nv == nil {
			nv = []string{}
		}
		batchOut = append(batchOut, map[string]any{"name": bt.name, "current": mapView(bt.current), "named": nv, "entries": ev,
			"changes": changeView(changes), "consumed": list32(consumed), "map": mapView(m)})
	}

	// Contexts and object answers over one map.
	om := mapOf("b", "d", "f", "photos/1", "photos/2")
	tokCount, tokRoot := uint64(len(om)), om.Root()
	all := func([32]byte) bool { return true }
	var objects []map[string]any
	addObj := func(name, k string, entry *[32]byte, ctx *storerec.EntryContext, leaves []storerec.MapLeaf) error {
		// A reader decodes the token's context before it checks it.
		decoded, err := storerec.DecodeTokenContext(must(storerec.EncodeTokenContext(leaves)))
		var res storerec.ObjectResult
		var c [32]byte
		if err == nil {
			res, c, err = storerec.CheckObject(tokCount, tokRoot, k, entry, ctx, decoded)
		}
		o := map[string]any{"name": name, "key": k, "tokenContext": leavesView(leaves), "result": objResult(res, err), "rule": storerec.Rule(err)}
		if entry != nil {
			o["c"] = h32(*entry)
			o["entryContext"] = hx(must(ctx.Encode()))
		}
		if err == nil && res != storerec.ObjectAbsent {
			o["named"] = h32(c)
		}
		objects = append(objects, o)
		return nil
	}
	for _, k := range []string{"b", "photos/2", "a", "c", "z", "photos/", "photos/10"} {
		ctx, leaves := om.ProveObject(k, all)
		var e *[32]byte
		if ctx != nil {
			x := om[k]
			e = &x
		}
		if err := addObj("honest: "+k, k, e, ctx, leaves); err != nil {
			return err
		}
	}
	// Unheld, honest.
	_, l2 := om.ProveObject("d", func(c [32]byte) bool { return c != om["d"] })
	_ = addObj("honest: d unheld (role 2)", "d", nil, nil, l2)
	// Empty map.
	{
		res, _, err := storerec.CheckObject(0, storerec.EmptyHash, "x", nil, nil, nil)
		objects = append(objects, map[string]any{"name": "an empty map proves every key absent", "key": "x", "count": 0,
			"mapRoot": h32(storerec.EmptyHash), "tokenContext": leavesView(nil), "result": objResult(res, err), "rule": storerec.Rule(err)})
	}
	// Dishonest objects.
	{
		ctx, _ := om.ProveEntry("d")
		x := om["d"]
		_ = addObj("an entry proven at another key", "b", &x, &ctx, nil)
		y := label("not the map's")
		_ = addObj("another commitment for the key", "d", &y, &ctx, nil)
		_, la := om.ProveObject("c", all)
		_ = addObj("an entry with a token context too", "d", &x, &ctx, la)
		// Absence with a gap: role 0 "b" (index 0), role 1 "f" (index 2).
		_, lb := om.ProveObject("a", all) // role 1 at "b"
		_, lf := om.ProveObject("e", all) // role 0 "d", role 1 "f"
		r0 := lb[0]
		r0.Role = storerec.RoleBefore
		_ = addObj("absence with a gap between the leaves", "c", nil, nil, []storerec.MapLeaf{r0, lf[1]})
		_ = addObj("two leaves of one role", "c", nil, nil, []storerec.MapLeaf{lb[0], lf[1]})
		_ = addObj("absence whose role 0 is the key itself", "b", nil, nil, []storerec.MapLeaf{r0, lf[0]})
		_ = addObj("absence of a key that exists (leaves around it)", "d", nil, nil, la)
		_ = addObj("absence of a key past the end without the last key", "z", nil, nil, []storerec.MapLeaf{lf[0]})
		_ = addObj("absence of a key before the start with no leaf", "a", nil, nil, nil)
		_ = addObj("a role 2 leaf for another key", "c", nil, nil, l2)
		bad := l2[0]
		bad.Path = append([][32]byte{}, bad.Path...)
		bad.Path[0][0] ^= 1
		_ = addObj("a role 2 leaf with a broken path", "d", nil, nil, []storerec.MapLeaf{bad})
	}

	// Context encodings and their refusals.
	ctxCases := []map[string]any{}
	addCtx := func(name, which string, raw []byte) {
		var err error
		if which == "entry" {
			_, err = storerec.DecodeEntryContext(raw)
		} else {
			_, err = storerec.DecodeTokenContext(raw)
		}
		ctxCases = append(ctxCases, map[string]any{"name": name, "which": which, "hex": hx(raw), "label": reason(err), "rule": storerec.Rule(err)})
	}
	ec, _ := om.ProveEntry("d")
	addCtx("an entry's context", "entry", must(ec.Encode()))
	path := func(n int) []cbor.Value {
		out := make([]cbor.Value, n)
		for i := range out {
			out[i] = make([]byte, 32)
		}
		return out
	}
	addCtx("20 steps", "entry", enc([]cbor.Value{uint64(0), path(20)}))
	addCtx("21 steps", "entry", enc([]cbor.Value{uint64(0), path(21)}))
	addCtx("index 1,048,575", "entry", enc([]cbor.Value{uint64(storerec.MaxKeysInMap - 1), path(1)}))
	addCtx("index 1,048,576", "entry", enc([]cbor.Value{uint64(storerec.MaxKeysInMap), path(1)}))
	addCtx("a step of 31 bytes", "entry", enc([]cbor.Value{uint64(0), []cbor.Value{make([]byte, 31)}}))
	addCtx("three elements", "entry", enc([]cbor.Value{uint64(0), path(1), uint64(0)}))
	addCtx("not CBOR", "entry", []byte{0xff})
	addCtx("1,025 bytes", "entry", make([]byte, 1025))
	_, lv := om.ProveObject("c", all)
	addCtx("a token's context of two leaves", "token", must(storerec.EncodeTokenContext(lv)))
	addCtx("an empty token context", "token", must(storerec.EncodeTokenContext(nil)))
	lvRaw := func(ls ...[]cbor.Value) []byte {
		a := make([]cbor.Value, len(ls))
		for i, l := range ls {
			a[i] = l
		}
		return enc(a)
	}
	leafV := func(role uint64, key string) []cbor.Value {
		return []cbor.Value{role, uint64(0), key, make([]byte, 32), path(1)}
	}
	addCtx("three leaves", "token", lvRaw(leafV(0, "a"), leafV(1, "b"), leafV(2, "c")))
	addCtx("roles out of order", "token", lvRaw(leafV(1, "a"), leafV(0, "b")))
	addCtx("two leaves of one role", "token", lvRaw(leafV(0, "a"), leafV(0, "b")))
	addCtx("role 1 beside role 2", "token", lvRaw(leafV(1, "a"), leafV(2, "b")))
	addCtx("role 0 beside role 2", "token", lvRaw(leafV(0, "a"), leafV(2, "b")))
	addCtx("role 3", "token", lvRaw(leafV(3, "a")))
	addCtx("a leaf's key empty", "token", lvRaw(leafV(0, "")))
	addCtx("a leaf's key with a newline", "token", lvRaw(leafV(0, "a\nb")))
	addCtx("a leaf of four elements", "token", enc([]cbor.Value{[]cbor.Value{uint64(0), uint64(0), "a", make([]byte, 32)}}))
	addCtx("4,097 bytes", "token", make([]byte, 4097))
	big := lvRaw([]cbor.Value{uint64(0), uint64(0), strings.Repeat("k", 1024), make([]byte, 32), path(20)}, []cbor.Value{uint64(1), uint64(1), strings.Repeat("l", 1024), make([]byte, 32), path(20)})
	addCtx("the largest token context: two leaves of 1,024-byte keys and 20 steps, under 4,096 bytes", "token", big)

	// Pages over a map, honest and dishonest.
	pm := mapOf("a1", "az", "b1", "b2", "b3", "ba", "c")
	pages, err := pageVectors(pm)
	if err != nil {
		return err
	}
	// The listing maximum: 300 keys under one prefix.
	big300 := storerec.Map{}
	for i := 0; i < 300; i++ {
		big300[fmt.Sprintf("p/%03d", i)] = label("big %d", i)
	}
	big300["q"] = label("big q")
	full, fl := big300.ProvePage("p/", "", storerec.PageList, all)
	pages = append(pages, pageCase("list: a full page of 256 carries no role 1", big300, "p/", "", storerec.PageList, full, fl))
	pages = append(pages, pageCase("list: 255 entries and no role 1 is short", big300, "p/", "", storerec.PageList, full[:255], fl))
	full2, fl2 := big300.ProvePage("p/", "p/255", storerec.PageList, all)
	pages = append(pages, pageCase("list: the last page of the prefix ends at role 1", big300, "p/", "p/255", storerec.PageList, full2, fl2))
	ex, exl := big300.ProvePage("", "", storerec.PageExport, all)
	pages = append(pages, pageCase("export: one page holds all 301 keys, the range ends", big300, "", "", storerec.PageExport, ex, exl))

	// Abstract page checks sampled from the exhaustive oracle enumeration.
	abstract := abstractPages()
	reader, err := readerVectors()
	if err != nil {
		return err
	}

	return put(files, "map-v1.json", map[string]any{
		"note":         "key maps and their roots; folds of change lists and their statuses; batches the owner's tooling resolves; object answers proving inclusion, absence or an unheld key; context encodings; list and export pages, honest and dishonest, each check of spec section 11.3 refusing; abstract page checks (indices and keys only) sampled from an exhaustive enumeration",
		"roots":        roots,
		"folds":        foldOut,
		"batches":      batchOut,
		"objectMap":    mapView(om),
		"objectCount":  tokCount,
		"objectRoot":   h32(tokRoot),
		"objects":      objects,
		"contexts":     ctxCases,
		"pages":        pages,
		"abstractMax":  2,
		"abstract":     abstract,
		"maxMapPath":   storerec.MaxMapPath,
		"emptyMapRoot": h32(commit.RootOfBytes(nil)),
		"reader":       reader,
	})
}

func objResult(res storerec.ObjectResult, err error) string {
	if err != nil {
		return reason(err)
	}
	switch res {
	case storerec.ObjectFound:
		return "found"
	case storerec.ObjectAbsent:
		return "absent"
	case storerec.ObjectUnheld:
		return "unheld"
	}
	return "?"
}

func pageCase(name string, m storerec.Map, p, a string, max int, entries []storerec.PageEntry, leaves []storerec.MapLeaf) map[string]any {
	var ev []map[string]any
	for _, e := range entries {
		ev = append(ev, map[string]any{"key": e.Key, "c": h32(e.C), "context": hx(must(e.Ctx.Encode()))})
	}
	if ev == nil {
		ev = []map[string]any{}
	}
	err := storerec.CheckPage(uint64(len(m)), m.Root(), p, a, max, entries, leaves)
	return map[string]any{"name": name, "map": mapView(m), "prefix": p, "after": a, "max": max, "entries": ev,
		"tokenContext": leavesView(leaves), "label": reason(err), "rule": storerec.Rule(err)}
}

// pageVectors are pages of one small map with a page maximum of 2: every
// honest page for each prefix and cursor, then dishonest pages that each
// check refuses.
func pageVectors(m storerec.Map) ([]map[string]any, error) {
	all := func([32]byte) bool { return true }
	keys := m.Keys()
	var out []map[string]any
	for _, p := range []string{"", "a", "b", "b1", "ba", "c", "d", "0"} {
		for _, a := range []string{"", "a1", "b", "b1", "b3", "zz"} {
			e, l := m.ProvePage(p, a, 2, all)
			c := pageCase(fmt.Sprintf("honest: p=%q a=%q", p, a), m, p, a, 2, e, l)
			if c["label"] != "ok" {
				return nil, fmt.Errorf("honest page %q %q refused: %v", p, a, c["rule"])
			}
			out = append(out, c)
		}
	}
	leaf := func(role uint64, k string) storerec.MapLeaf {
		i := sort.SearchStrings(keys, k)
		ctx, _ := m.ProveEntry(k)
		return storerec.MapLeaf{Role: role, Index: uint64(i), Key: k, C: m[k], Path: ctx.Path}
	}
	ent := func(k string) storerec.PageEntry {
		ctx, _ := m.ProveEntry(k)
		return storerec.PageEntry{Key: k, C: m[k], Ctx: ctx}
	}
	L := func(ls ...storerec.MapLeaf) []storerec.MapLeaf { return ls }
	E := func(es ...storerec.PageEntry) []storerec.PageEntry { return es }
	held := func(c [32]byte) bool { return c != m["b2"] }
	he, hl := m.ProvePage("b", "", 2, held)
	dishonest := []struct {
		name, p, a string
		e          []storerec.PageEntry
		l          []storerec.MapLeaf
		want       string
	}{
		{"an empty page bracketing nothing: role 0 a1, role 1 az hide b1", "b", "", nil, L(leaf(0, "a1"), leaf(1, "az")), "check-4"},
		{"an empty page whose role 0 sits after the range", "b", "", nil, L(leaf(0, "c")), "check-2"},
		{"an empty page: role 0 inside the range", "b", "", nil, L(leaf(0, "b1"), leaf(1, "b2")), "check-2"},
		{"role 1 inside the range on a short page", "b", "", E(ent("b1")), L(leaf(0, "az"), leaf(1, "b2")), "check-4"},
		{"role 1 before the range", "b", "b2", E(ent("b3")), L(leaf(0, "b2"), leaf(1, "b1")), "check-4"},
		{"a short page without role 1", "b", "", E(ent("b1")), L(leaf(0, "az")), "check-4"},
		{"a page that skips an entry", "b", "", E(ent("b1"), ent("b3")), L(leaf(0, "az")), "check-1"},
		{"an entry outside the prefix", "b", "", E(ent("az"), ent("b1")), L(leaf(0, "a1")), "check-1"},
		{"an entry at or before the cursor", "b", "b1", E(ent("b1"), ent("b2")), L(leaf(0, "az")), "check-1"},
		{"no role 0 when the page starts after index 0", "b", "", E(ent("b1"), ent("b2")), nil, "check-2"},
		{"role 0 not just before the start", "b", "", E(ent("b1"), ent("b2")), L(leaf(0, "a1")), "check-2"},
		{"a role 2 leaf past the next index", "b", "", E(ent("b1")), L(leaf(0, "az"), leaf(2, "b3")), "check-3"},
		{"a role 2 leaf outside the range", "b", "", nil, L(leaf(0, "az"), leaf(2, "c")), "check-2"},
		{"a role 2 leaf before the cursor", "b", "b1", E(), L(leaf(0, "b1"), leaf(2, "b1")), "check-2"},
		{"honest: a page stopped by an unheld entry", "b", "", he, hl, "ok"},
		{"a page that names an unheld entry and hides it: no role 2, short", "b", "", he, hl[:1], "check-4"},
	}
	bad := false
	defer func() {
		if bad {
			panic("page expectations")
		}
	}()
	for _, d := range dishonest {
		c := pageCase("dishonest: "+d.name, m, d.p, d.a, 2, d.e, d.l)
		got := c["rule"]
		if c["label"] == "ok" {
			got = "ok"
		}
		if got != d.want {
			// The case's own expectation is a generator check.
			fmt.Printf("page %q: got %v, want %s\n", d.name, got, d.want)
			bad = true
		}
		out = append(out, c)
	}
	// A page whose entry's path does not verify.
	e := ent("b1")
	e.Ctx.Path = append([][32]byte{}, e.Ctx.Path...)
	e.Ctx.Path[0][1] ^= 1
	out = append(out, pageCase("dishonest: an entry whose path does not verify", m, "b", "", 2, E(e, ent("b2")), L(leaf(0, "az"))))
	f := ent("b1")
	f.C = label("another")
	out = append(out, pageCase("dishonest: an entry under another commitment", m, "b", "", 2, E(f, ent("b2")), L(leaf(0, "az"))))
	return out, nil
}

// abstractPages samples the exhaustive enumeration of pages_test.go: every
// set of one to six keys of length one to three over "ab", prefixes,
// cursors, starts, page sizes up to 2 and role combinations.
func abstractPages() []map[string]any {
	var U []string
	var rec func(p string)
	rec = func(p string) {
		if len(p) > 0 {
			U = append(U, p)
		}
		if len(p) == 3 {
			return
		}
		rec(p + "a")
		rec(p + "b")
	}
	rec("")
	sort.Strings(U)
	prefixes := append([]string{""}, U[:6]...)
	afters := append([]string{""}, U[:8]...)
	var out []map[string]any
	n := 0
	var subsets func(start int, cur []string)
	pk := func(K []string, i int) *storerec.PageKey {
		if i < 0 {
			return nil
		}
		return &storerec.PageKey{Index: uint64(i), Key: K[i]}
	}
	subsets = func(start int, cur []string) {
		if len(cur) > 0 && len(cur)%2 == 1 {
			K := cur
			for _, p := range prefixes {
				for _, a := range afters {
					for st := 0; st <= len(K); st++ {
						for m := 0; m <= 2 && st+m <= len(K); m++ {
							nx := st + m
							for _, r0 := range []int{-1, st - 1} {
								for _, r1 := range []int{-1, nx} {
									for _, r2 := range []int{-1, nx} {
										if (r0 == st-1 && st == 0) || (nx >= len(K) && (r1 >= 0 || r2 >= 0)) {
											continue
										}
										n++
										if n%3989 != 0 {
											continue
										}
										var ans []storerec.PageKey
										for i := st; i < nx; i++ {
											ans = append(ans, storerec.PageKey{Index: uint64(i), Key: K[i]})
										}
										err := storerec.CheckPageOrder(uint64(len(K)), p, a, 2, ans, pk(K, r0), pk(K, r1), pk(K, r2))
										v := "ok"
										if err != nil {
											v = storerec.Rule(err)
										}
										out = append(out, map[string]any{"keys": append([]string(nil), K...), "p": p, "a": a,
											"start": st, "answered": m, "r0": r0, "r1": r1, "r2": r2, "verdict": v})
									}
								}
							}
						}
					}
				}
			}
		}
		if len(cur) == 5 {
			return
		}
		for i := start; i < len(U); i++ {
			subsets(i+1, append(append([]string(nil), cur...), U[i]))
		}
	}
	subsets(0, nil)
	return out
}

// readerVectors are a reader's steps 1 to 5 on records it verified: the
// walk back by prev, and an object answer's head checks.
func readerVectors() (map[string]any, error) {
	b0, b1, b2 := sampleBucket0(), sampleBucket1(), sampleBucket2()
	t0, t1, t2 := label("walk 0"), label("walk 1"), label("walk 2")
	l0 := storerec.Link{Txid: t0, Bucket: b0, Spends: &storerec.Outpoint{Txid: firstOutpoint, Vout: 0}}
	b1c := *b1
	b1c.Prev = t0
	l1 := storerec.Link{Txid: t1, Bucket: &b1c}
	b2c := *b2
	b2c.Prev = t1
	l2 := storerec.Link{Txid: t2, Bucket: &b2c}
	other := *b0
	other.Owner = Dave.Pub()
	gap := b2c
	gap.Seq = 3
	var walks []map[string]any
	h := func(seq uint64, txid [32]byte) *storerec.Held { return &storerec.Held{Seq: seq, Txid: txid} }
	addWalk := func(name string, held *storerec.Held, ls ...storerec.Link) {
		lv := []map[string]any{}
		for _, l := range ls {
			x := map[string]any{"txid": h32(l.Txid), "record": hx(must(l.Bucket.Encode()))}
			if l.Spends != nil {
				x["spends"] = map[string]any{"txid": h32(l.Spends.Txid), "vout": l.Spends.Vout}
			}
			lv = append(lv, x)
		}
		w := map[string]any{"name": name, "walk": lv, "label": reason(storerec.CheckWalk(held, ls))}
		if held != nil {
			w["held"] = map[string]any{"seq": held.Seq, "txid": h32(held.Txid)}
		}
		walks = append(walks, w)
	}
	addWalk("from seq 2 to seq 0, nothing held", nil, l2, l1, l0)
	addWalk("from seq 2 to the token held", h(1, t1), l2, l1)
	addWalk("from seq 2 to another token at the seq held", h(1, label("held elsewhere")), l2, l1)
	addWalk("the token held, answered again", h(2, t2), l2)
	addWalk("a token below the seq held", h(2, t2), l1, l0)
	addWalk("prev is not the next token's txid", nil, l2, storerec.Link{Txid: label("not it"), Bucket: &b1c}, l0)
	addWalk("a seq skipped", nil, storerec.Link{Txid: t2, Bucket: &gap}, l1, l0)
	addWalk("another owner below", nil, l1, storerec.Link{Txid: t0, Bucket: &other, Spends: l0.Spends})
	addWalk("the walk stops above the seq held", h(0, t0), l2, l1)
	addWalk("the first token's id is not its outpoint's", nil, l1, storerec.Link{Txid: t0, Bucket: b0, Spends: &storerec.Outpoint{Txid: firstOutpoint, Vout: 1}})
	addWalk("the first token without its outpoint", nil, l1, storerec.Link{Txid: t0, Bucket: b0})
	addWalk("no token", nil)

	// Heads over a token whose map names one put.
	put := samplePut()
	c := label("head C")
	m := storerec.Map{put.Key: c}
	tok := &storerec.Bucket{Bucket: Bucket, Owner: Owner.Pub(), ID: bucketID, Seq: 1, Prev: t0, Writers: [][]byte{Alice.Pub()},
		Changes: []storerec.Change{{Key: put.Key, C: c}}, Count: 1, MapRoot: m.Root(), Created: Created, Consumed: [][32]byte{}}
	noWriters := *tok
	noWriters.Writers = [][]byte{}
	var heads []map[string]any
	addHead := func(name string, t *storerec.Bucket, key string, e *storerec.Entry) {
		ctx, leaves := m.ProveObject(key, func([32]byte) bool { return e != nil })
		var cp *[32]byte
		h := map[string]any{"name": name, "token": hx(must(t.Encode())), "key": key, "tokenContext": leavesView(leaves)}
		if ctx != nil {
			cp = &c
			h["c"] = h32(c)
			h["entryContext"] = hx(must(ctx.Encode()))
			h["entry"] = hx(must(e.Encode()))
		}
		res, err := storerec.CheckHead(t, key, cp, e, ctx, leaves)
		h["result"] = objResult(res, err)
		heads = append(heads, h)
	}
	addHead("a put by a writer of the token", tok, put.Key, put)
	byOwner := *put
	byOwner.Writer = Owner.Pub()
	addHead("a put by the owner, no writers named", &noWriters, put.Key, &byOwner)
	addHead("a put by a writer the token does not name", &noWriters, put.Key, put)
	byDave := *put
	byDave.Writer = Dave.Pub()
	addHead("a put by a stranger", tok, put.Key, &byDave)
	otherKey := *put
	otherKey.Key = "photos/2026/dog.jpg"
	addHead("an entry for another key", tok, put.Key, &otherKey)
	otherBucket := *put
	otherBucket.Bucket = OtherBucket
	addHead("an entry for another bucket", tok, put.Key, &otherBucket)
	otherID := *put
	otherID.ID = label("another chain")
	addHead("an entry for another chain", tok, put.Key, &otherID)
	del := *sampleDelete()
	addHead("a delete the map names as a value", tok, put.Key, &del)
	unk := *sampleUnknownKind()
	unk.Key = put.Key
	addHead("an unknown kind", tok, put.Key, &unk)
	addHead("a key the map proves absent", tok, "photos/2026/emu.jpg", nil)
	return map[string]any{"walks": walks, "heads": heads}, nil
}
