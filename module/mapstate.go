package module

import (
	"crypto/sha256"
	"sort"
	"strings"

	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/storerec"
)

// emptyMapRoot is SHA-256 of no bytes: the root of an empty map.
var emptyMapRoot = sha256.Sum256(nil)

// mapState is the key map of one branch of a served chain, as the host
// folds it (spec sections 3.4 and 12.2): storerec's fold, done on flat
// slices so that a token costs its changes and one tree over the map, and
// kept with that tree so that an answer's paths cost a lookup each.
// storerec.Map's fold clones the map and sorts it again for every token,
// and its paths rebuild the tree for every leaf; it is the reference, and
// the tests hold this one to it token by token and page by page. Keys sort
// by their bytes (UTF-8), a key's leaf hash is
// SHA-256(0x00 || uint16be(len(key)) || key || C), a node
// SHA-256(0x01 || left || right), the root of no leaves SHA-256("").
type mapState struct {
	// keys ascending; cs the commitment of each, in key order.
	keys []string
	cs   [][32]byte
	// levels[0] are the leaf hashes in key order; the last level the root.
	levels [][][32]byte
	// named is every commitment a token of the branch named.
	named map[[32]byte]bool
}

func newMapState() *mapState { return &mapState{levels: [][][32]byte{nil}, named: map[[32]byte]bool{}} }

func (m *mapState) size() int { return len(m.keys) }

// root is the map root.
func (m *mapState) root() [32]byte {
	if len(m.keys) == 0 {
		return emptyMapRoot
	}
	return m.levels[len(m.levels)-1][0]
}

// lower is the index of the first key at or above k.
func (m *mapState) lower(k string) int { return sort.SearchStrings(m.keys, k) }

// upper is the index of the first key above k.
func (m *mapState) upper(k string) int {
	return sort.Search(len(m.keys), func(i int) bool { return m.keys[i] > k })
}

// get is the commitment key k names.
func (m *mapState) get(k string) ([32]byte, bool) {
	i := m.lower(k)
	if i < len(m.keys) && m.keys[i] == k {
		return m.cs[i], true
	}
	return [32]byte{}, false
}

// path is the audit path of leaf i, leaf to root (RFC 6962 section 2.1.1).
func (m *mapState) path(i int) [][32]byte {
	out := [][32]byte{}
	n := len(m.keys)
	for l := 0; n > 1; l++ {
		if sib := i ^ 1; sib < n {
			out = append(out, m.levels[l][sib])
		}
		i >>= 1
		n = (n + 1) / 2
	}
	return out
}

// leafAt is the leaf at index i, in full, with its path.
func (m *mapState) leafAt(role uint64, i int) storerec.MapLeaf {
	return storerec.MapLeaf{Role: role, Index: uint64(i), Key: m.keys[i], C: m.cs[i], Path: m.path(i)}
}

// levelsOf is every level of the RFC 6962 tree over the leaf hashes,
// leaves first; an odd node is carried up unchanged.
func levelsOf(hs [][32]byte) [][][32]byte {
	out := [][][32]byte{hs}
	cur := hs
	for len(cur) > 1 {
		next := make([][32]byte, (len(cur)+1)/2)
		for i := 0; i+1 < len(cur); i += 2 {
			next[i/2] = commit.NodeHash(cur[i], cur[i+1])
		}
		if len(cur)%2 == 1 {
			next[len(next)-1] = cur[len(cur)-1]
		}
		out = append(out, next)
		cur = next
	}
	return out
}

type mapEdit struct {
	key     string
	c       [32]byte
	removal bool
	at      int
	found   bool
}

// apply folds one token onto the map (spec section 3.4), as storerec's
// fold does: every change must change the map, no commitment may be named
// twice in the branch, and the map after must hold count keys under
// mapRoot. It answers why the token does not fold, leaving the map as it
// was, or applies it and answers "".
func (m *mapState) apply(t *storerec.Bucket) string {
	seen := map[[32]byte]bool{}
	name := func(c [32]byte) bool {
		if m.named[c] || seen[c] {
			return false
		}
		seen[c] = true
		return true
	}
	var edits []mapEdit
	size := len(m.keys)
	for _, ch := range t.Changes {
		if !name(ch.C) {
			return "named-twice"
		}
		at := m.lower(ch.Key)
		found := at < len(m.keys) && m.keys[at] == ch.Key
		if ch.Removal {
			if !found {
				return "removal-of-absent"
			}
			edits = append(edits, mapEdit{key: ch.Key, removal: true, at: at, found: true})
			size--
			continue
		}
		if found && m.cs[at] == ch.C {
			return "put-unchanged"
		}
		edits = append(edits, mapEdit{key: ch.Key, c: ch.C, at: at, found: found})
		if !found {
			size++
		}
	}
	for _, c := range t.Consumed {
		if !name(c) {
			return "named-twice"
		}
	}
	if size < 0 || uint64(size) != t.Count {
		return "count"
	}
	if size > storerec.MaxKeysInMap {
		return "too-many-keys"
	}
	// A token's changes are ascending by key (the record's rule), so their
	// places are too; the edits are applied in order.
	sort.SliceStable(edits, func(i, j int) bool { return edits[i].key < edits[j].key })
	keys := make([]string, 0, size)
	cs := make([][32]byte, 0, size)
	hs := make([][32]byte, 0, size)
	old := m.levels
	from := 0
	copyTo := func(until int) {
		keys = append(keys, m.keys[from:until]...)
		cs = append(cs, m.cs[from:until]...)
		hs = append(hs, old[0][from:until]...)
		from = until
	}
	for _, e := range edits {
		if e.at < from {
			return "count"
		}
		copyTo(e.at)
		if e.found {
			from++
		}
		if e.removal {
			continue
		}
		keys = append(keys, e.key)
		cs = append(cs, e.c)
		hs = append(hs, commit.HashLeaf(storerec.Leaf(e.key, e.c)))
	}
	copyTo(len(m.keys))
	levels := levelsOf(hs)
	root := emptyMapRoot
	if len(keys) > 0 {
		root = levels[len(levels)-1][0]
	}
	if root != t.MapRoot {
		return "map-root"
	}
	m.keys, m.cs, m.levels = keys, cs, levels
	for c := range seen {
		m.named[c] = true
	}
	return ""
}

// clone is an independent copy: the branch's map moves to a child, and a
// second child (a reorganisation) folds from a copy.
func (m *mapState) clone() *mapState {
	n := &mapState{keys: append([]string(nil), m.keys...), cs: append([][32]byte(nil), m.cs...), named: make(map[[32]byte]bool, len(m.named))}
	for _, l := range m.levels {
		n.levels = append(n.levels, append([][32]byte(nil), l...))
	}
	for c := range m.named {
		n.named[c] = true
	}
	return n
}

// proveObject is the honest object answer for k (storerec Map.ProveObject
// over the kept tree): the entry's context, or the leaves that prove k
// absent or name its entry unheld.
func (m *mapState) proveObject(k string, held func([32]byte) bool) (*storerec.EntryContext, []storerec.MapLeaf) {
	i := m.lower(k)
	if i < len(m.keys) && m.keys[i] == k {
		if held(m.cs[i]) {
			return &storerec.EntryContext{Index: uint64(i), Path: m.path(i)}, nil
		}
		return nil, []storerec.MapLeaf{m.leafAt(storerec.RoleUnheld, i)}
	}
	var out []storerec.MapLeaf
	if i > 0 {
		out = append(out, m.leafAt(storerec.RoleBefore, i-1))
	}
	if i < len(m.keys) {
		out = append(out, m.leafAt(storerec.RoleAfter, i))
	}
	return nil, out
}

// provePage is the honest page for prefix p after a (storerec
// Map.ProvePage over the kept tree), at most max entries, stopping at the
// first entry in range that held refuses (role 2).
func (m *mapState) provePage(p, a string, max int, held func([32]byte) bool) ([]storerec.PageEntry, []storerec.MapLeaf) {
	n := len(m.keys)
	// The first key that is neither at or before a nor before p.
	s := m.upper(a)
	if l := m.lower(p); l > s {
		s = l
	}
	var entries []storerec.PageEntry
	var leaves []storerec.MapLeaf
	if s > 0 {
		leaves = append(leaves, m.leafAt(storerec.RoleBefore, s-1))
	}
	i := s
	for ; i < n && len(entries) < max && strings.HasPrefix(m.keys[i], p) && m.keys[i] > a; i++ {
		if !held(m.cs[i]) {
			return entries, append(leaves, m.leafAt(storerec.RoleUnheld, i))
		}
		entries = append(entries, storerec.PageEntry{Key: m.keys[i], C: m.cs[i], Ctx: storerec.EntryContext{Index: uint64(i), Path: m.path(i)}})
	}
	if len(entries) < max && i < n {
		leaves = append(leaves, m.leafAt(storerec.RoleAfter, i))
	}
	return entries, leaves
}
