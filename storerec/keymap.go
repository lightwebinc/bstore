package storerec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/commit"
)

// The fold's and a reader's map labels.
var (
	ErrMapMismatch = errors.New("storerec: a token's changes do not fold to its count and mapRoot")
	ErrMap         = errors.New("storerec: a context does not prove what the answer claims")
)

// Leaf is a map leaf's bytes: uint16be(len(key)) || key || C.
func Leaf(key string, c [32]byte) []byte {
	out := make([]byte, 2, 2+len(key)+32)
	binary.BigEndian.PutUint16(out, uint16(len(key)))
	out = append(out, key...)
	return append(out, c[:]...)
}

// Map is a key map: key to commitment.
type Map map[string][32]byte

// Keys returns the map's keys ascending by their bytes.
func (m Map) Keys() []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Leaves returns the map's leaves in key order.
func (m Map) Leaves() [][]byte {
	keys := m.Keys()
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = Leaf(k, m[k])
	}
	return out
}

// Root is the map root (spec section 3.4).
func (m Map) Root() [32]byte { return commit.RootOfBytes(m.Leaves()) }

// Clone copies the map.
func (m Map) Clone() Map {
	out := make(Map, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Status is a rooted token's fold status (spec section 12.2).
type Status string

const (
	StatusConsistent  Status = "consistent"
	StatusMapMismatch Status = "map-mismatch"
)

// Fold is a chain folded from seq 0.
type Fold struct {
	// Statuses has one status per token given.
	Statuses []Status
	// Reasons names, for each map-mismatch token that broke the chain, what
	// broke it; "" for the rest.
	Reasons []string
	// Map is the map of the last consistent token; Named is every
	// commitment the consistent tokens named.
	Map   Map
	Named map[[32]byte]bool
}

// FoldChain folds the rooted tokens of one chain, given in seq order from
// 0, as a host and a replaying reader do: each token's changes applied to
// its predecessor's map in changes order must each change the map, name no
// commitment an earlier token of the chain (or an earlier change of its
// own) named, and leave a map of count keys, at most 1,048,576, whose root
// is mapRoot; else map-mismatch, which is sticky.
func FoldChain(tokens []*Bucket) (*Fold, error) {
	f := &Fold{Map: Map{}, Named: map[[32]byte]bool{}}
	broken := false
	for i, t := range tokens {
		if t.Seq != uint64(i) {
			return nil, fmt.Errorf("storerec: token %d has seq %d", i, t.Seq)
		}
		if broken {
			f.Statuses = append(f.Statuses, StatusMapMismatch)
			f.Reasons = append(f.Reasons, "")
			continue
		}
		next, named, why := apply(f.Map, f.Named, t)
		if why != "" {
			broken = true
			f.Statuses = append(f.Statuses, StatusMapMismatch)
			f.Reasons = append(f.Reasons, why)
			continue
		}
		f.Map, f.Named = next, named
		f.Statuses = append(f.Statuses, StatusConsistent)
		f.Reasons = append(f.Reasons, "")
	}
	return f, nil
}

// apply folds one token onto a map, returning the next map and named set,
// or why the token is a map-mismatch.
func apply(m Map, named map[[32]byte]bool, t *Bucket) (Map, map[[32]byte]bool, string) {
	next := m.Clone()
	seen := make(map[[32]byte]bool, len(named)+len(t.Changes)+len(t.Consumed))
	for c := range named {
		seen[c] = true
	}
	name := func(c [32]byte) bool {
		if seen[c] {
			return false
		}
		seen[c] = true
		return true
	}
	for _, c := range t.Changes {
		if !name(c.C) {
			return nil, nil, "named-twice"
		}
		cur, ok := next[c.Key]
		if c.Removal {
			if !ok {
				return nil, nil, "removal-of-absent"
			}
			delete(next, c.Key)
			continue
		}
		if ok && cur == c.C {
			return nil, nil, "put-unchanged"
		}
		next[c.Key] = c.C
	}
	for _, c := range t.Consumed {
		if !name(c) {
			return nil, nil, "named-twice"
		}
	}
	if uint64(len(next)) != t.Count {
		return nil, nil, "count"
	}
	if len(next) > MaxKeysInMap {
		return nil, nil, "too-many-keys"
	}
	if next.Root() != t.MapRoot {
		return nil, nil, "map-root"
	}
	return next, seen, ""
}

// Applicable is an entry as the owner's anchoring tooling sees it.
type Applicable struct {
	C       [32]byte
	Key     string
	Kind    uint64
	Created uint64
}

// ResolveBatch is the owner's tooling (spec section 3.4): from the entries
// it would apply against the current map, it skips every entry a token of
// the chain has named (and every kind other than put and delete); orders
// one key's entries by created, then by commitment, the last deciding the
// key's change and the others going to consumed; and moves to consumed
// every change that would not change the map (a delete of a key the map
// does not hold, a put of the value the key already names). Changes come
// back ascending by key and consumed ascending, so that no change it writes
// can be a fold error.
func ResolveBatch(current Map, named map[[32]byte]bool, batch []Applicable) ([]Change, [][32]byte) {
	byKey := map[string][]Applicable{}
	taken := map[[32]byte]bool{}
	for _, e := range batch {
		if named[e.C] || taken[e.C] || (e.Kind != KindPut && e.Kind != KindDelete) {
			continue
		}
		taken[e.C] = true
		byKey[e.Key] = append(byKey[e.Key], e)
	}
	var changes []Change
	var consumed [][32]byte
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		es := byKey[k]
		sort.Slice(es, func(i, j int) bool {
			if es[i].Created != es[j].Created {
				return es[i].Created < es[j].Created
			}
			return bytes.Compare(es[i].C[:], es[j].C[:]) < 0
		})
		for _, e := range es[:len(es)-1] {
			consumed = append(consumed, e.C)
		}
		last := es[len(es)-1]
		cur, ok := current[k]
		switch {
		case last.Kind == KindDelete && !ok, last.Kind == KindPut && ok && cur == last.C:
			consumed = append(consumed, last.C)
		case last.Kind == KindDelete:
			changes = append(changes, Change{Key: k, C: last.C, Removal: true})
		default:
			changes = append(changes, Change{Key: k, C: last.C})
		}
	}
	sort.Slice(consumed, func(i, j int) bool { return bytes.Compare(consumed[i][:], consumed[j][:]) < 0 })
	return changes, consumed
}

// ---- contexts ----

// Roles of a token context's leaves (spec section 11.3).
const (
	RoleBefore = 0
	RoleAfter  = 1
	RoleUnheld = 2
)

// EntryContext is an entry's context [index, path].
type EntryContext struct {
	Index uint64
	Path  [][32]byte
}

// MapLeaf is one leaf of a token's context [role, index, key, C, path].
type MapLeaf struct {
	Role  uint64
	Index uint64
	Key   string
	C     [32]byte
	Path  [][32]byte
}

func pathValue(p [][32]byte) []cbor.Value {
	out := make([]cbor.Value, len(p))
	for i, h := range p {
		out[i] = b(h)
	}
	return out
}

// Encode writes an entry's context.
func (c EntryContext) Encode() ([]byte, error) {
	out, err := cbor.Encode([]cbor.Value{c.Index, pathValue(c.Path)})
	if err == nil && len(out) > MaxEntryContext {
		return nil, ErrTooLarge
	}
	return out, err
}

// EncodeTokenContext writes a token's context, at most two leaves.
func EncodeTokenContext(leaves []MapLeaf) ([]byte, error) {
	a := make([]cbor.Value, len(leaves))
	for i, l := range leaves {
		a[i] = []cbor.Value{l.Role, l.Index, l.Key, b(l.C), pathValue(l.Path)}
	}
	out, err := cbor.Encode(a)
	if err == nil && len(out) > MaxTokenContext {
		return nil, ErrTooLarge
	}
	return out, err
}

func readPath(v cbor.Value) ([][32]byte, bool) {
	a, ok := v.([]cbor.Value)
	if !ok || len(a) > MaxMapPath {
		return nil, false
	}
	out := make([][32]byte, len(a))
	for i, e := range a {
		x, ok := e.([]byte)
		if !ok || len(x) != 32 {
			return nil, false
		}
		out[i] = [32]byte(x)
	}
	return out, true
}

func mapErr(rule string) error { return ruled(ErrMap, rule) }

// DecodeEntryContext reads an entry's context: map with rule too-large
// (over 1,024 bytes), cbor, or shape ([index, path], index at most
// 1,048,575, path at most 20 32-byte steps).
func DecodeEntryContext(raw []byte) (EntryContext, error) {
	if len(raw) > MaxEntryContext {
		return EntryContext{}, mapErr("too-large")
	}
	v, err := cbor.DecodeValue(raw)
	if err != nil {
		return EntryContext{}, mapErr("cbor")
	}
	a, ok := v.([]cbor.Value)
	if !ok || len(a) != 2 {
		return EntryContext{}, mapErr("shape")
	}
	idx, ok := a[0].(uint64)
	if !ok || idx >= MaxKeysInMap {
		return EntryContext{}, mapErr("shape")
	}
	p, ok := readPath(a[1])
	if !ok {
		return EntryContext{}, mapErr("shape")
	}
	return EntryContext{Index: idx, Path: p}, nil
}

// DecodeTokenContext reads a token's context: map with rule too-large (over
// 4,096 bytes), cbor, or shape (an array of at most two leaves [role,
// index, key, C, path], role 0 to 2, index at most 1,048,575, key an object
// key, C 32 bytes, path at most 20 steps; at most one leaf of each role, in
// role order, and no role 1 beside a role 2).
func DecodeTokenContext(raw []byte) ([]MapLeaf, error) {
	if len(raw) > MaxTokenContext {
		return nil, mapErr("too-large")
	}
	v, err := cbor.DecodeValue(raw)
	if err != nil {
		return nil, mapErr("cbor")
	}
	a, ok := v.([]cbor.Value)
	if !ok || len(a) > 2 {
		return nil, mapErr("shape")
	}
	out := make([]MapLeaf, len(a))
	for i, e := range a {
		l, ok := e.([]cbor.Value)
		if !ok || len(l) != 5 {
			return nil, mapErr("shape")
		}
		role, ok1 := l[0].(uint64)
		idx, ok2 := l[1].(uint64)
		key, ok3 := l[2].(string)
		c, ok4 := l[3].([]byte)
		p, ok5 := readPath(l[4])
		if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || role > RoleUnheld || idx >= MaxKeysInMap || len(c) != 32 || CheckObjectKey(key) != nil {
			return nil, mapErr("shape")
		}
		out[i] = MapLeaf{Role: role, Index: idx, Key: key, C: [32]byte(c), Path: p}
		if i > 0 && out[i-1].Role >= role {
			return nil, mapErr("shape")
		}
	}
	if len(out) == 2 && out[0].Role == RoleAfter && out[1].Role == RoleUnheld {
		return nil, mapErr("shape")
	}
	return out, nil
}

// proves reports whether (key, c) sits at index in the map of count keys
// with root.
func proves(key string, c [32]byte, index uint64, path [][32]byte, count uint64, root [32]byte) bool {
	return commit.VerifyAt(commit.HashLeaf(Leaf(key, c)), index, count, path, root)
}

// ProveEntry returns the context of the entry for key in m.
func (m Map) ProveEntry(key string) (EntryContext, bool) {
	keys := m.Keys()
	i := sort.SearchStrings(keys, key)
	if i == len(keys) || keys[i] != key {
		return EntryContext{}, false
	}
	return EntryContext{Index: uint64(i), Path: m.path(keys, i)}, true
}

func (m Map) path(keys []string, i int) [][32]byte {
	leaves := make([][]byte, len(keys))
	for j, k := range keys {
		leaves[j] = Leaf(k, m[k])
	}
	p, _ := commit.ProveBytes(leaves, i)
	return p.Siblings()
}

func (m Map) leaf(keys []string, role uint64, i int) MapLeaf {
	return MapLeaf{Role: role, Index: uint64(i), Key: keys[i], C: m[keys[i]], Path: m.path(keys, i)}
}

// ObjectResult is what an object answer proves about key k.
type ObjectResult int

const (
	ObjectFound ObjectResult = iota + 1
	ObjectAbsent
	ObjectUnheld
)

// CheckObject is a reader's step 2 for an object answer (spec section
// 11.3): with an entry (its commitment c and context), the token's context
// is empty and (k, c) is proven at the context's index; without one, the
// token's context is either the role 2 leaf whose key is k (the key exists,
// its entry unheld) or proves k absent: role 0 at j with a key below k and
// role 1 at j + 1 with a key above it, role 0 absent when j + 1 = 0 and role
// 1 absent when j + 1 = count. It returns the commitment the map names for
// k when it names one.
func CheckObject(count uint64, root [32]byte, k string, entry *[32]byte, ctx *EntryContext, leaves []MapLeaf) (ObjectResult, [32]byte, error) {
	if entry != nil {
		if len(leaves) != 0 || ctx == nil {
			return 0, zero32, mapErr("object-shape")
		}
		if !proves(k, *entry, ctx.Index, ctx.Path, count, root) {
			return 0, zero32, mapErr("path")
		}
		return ObjectFound, *entry, nil
	}
	for _, l := range leaves {
		if !proves(l.Key, l.C, l.Index, l.Path, count, root) {
			return 0, zero32, mapErr("path")
		}
	}
	if len(leaves) == 1 && leaves[0].Role == RoleUnheld {
		if leaves[0].Key != k {
			return 0, zero32, mapErr("object-unheld")
		}
		return ObjectUnheld, leaves[0].C, nil
	}
	var r0, r1 *MapLeaf
	for i := range leaves {
		switch leaves[i].Role {
		case RoleBefore:
			r0 = &leaves[i]
		case RoleAfter:
			r1 = &leaves[i]
		default:
			return 0, zero32, mapErr("object-absence")
		}
	}
	// next is j + 1, the index k would take.
	var next uint64
	switch {
	case r0 != nil:
		if r0.Key >= k {
			return 0, zero32, mapErr("object-absence")
		}
		next = r0.Index + 1
	case r1 != nil:
		next = 0
	default:
		next = 0
	}
	if r1 != nil {
		if r1.Index != next || r1.Key <= k {
			return 0, zero32, mapErr("object-absence")
		}
	} else if next != count {
		return 0, zero32, mapErr("object-absence")
	}
	if r0 == nil && next != 0 {
		return 0, zero32, mapErr("object-absence")
	}
	return ObjectAbsent, zero32, nil
}

// ProveObject is the honest answer for k: the entry's context when the map
// names k and held(c), else the role 2 leaf, or the absence leaves.
func (m Map) ProveObject(k string, held func([32]byte) bool) (*EntryContext, []MapLeaf) {
	keys := m.Keys()
	i := sort.SearchStrings(keys, k)
	if i < len(keys) && keys[i] == k {
		if held(m[k]) {
			return &EntryContext{Index: uint64(i), Path: m.path(keys, i)}, nil
		}
		return nil, []MapLeaf{m.leaf(keys, RoleUnheld, i)}
	}
	var out []MapLeaf
	if i > 0 {
		out = append(out, m.leaf(keys, RoleBefore, i-1))
	}
	if i < len(keys) {
		out = append(out, m.leaf(keys, RoleAfter, i))
	}
	return nil, out
}

// PageEntry is one answered entry of a page: its key, its commitment and
// its context.
type PageEntry struct {
	Key string
	C   [32]byte
	Ctx EntryContext
}

func inRange(k, p, a string) bool { return strings.HasPrefix(k, p) && k > a }
func below(k, p, a string) bool   { return k <= a || k < p }
func above(k, p, a string) bool   { return k > a && !strings.HasPrefix(k, p) && k > p }

// CheckPage is a reader's check of a list or export page (spec section
// 11.3) against a token of count keys and root, for prefix p (empty for
// export), after a, and the page maximum max (256 for list, 512 for
// export). Every entry's and every leaf's path is checked first (rule
// path), then checks 1 to 4 in order (rules check-1 to check-4). A page that
// fails is incomplete and names the host.
func CheckPage(count uint64, root [32]byte, p, a string, max int, entries []PageEntry, leaves []MapLeaf) error {
	for _, e := range entries {
		if !proves(e.Key, e.C, e.Ctx.Index, e.Ctx.Path, count, root) {
			return mapErr("path")
		}
	}
	var r0, r1, r2 *MapLeaf
	for i := range leaves {
		l := &leaves[i]
		if !proves(l.Key, l.C, l.Index, l.Path, count, root) {
			return mapErr("path")
		}
		switch l.Role {
		case RoleBefore:
			r0 = l
		case RoleAfter:
			r1 = l
		case RoleUnheld:
			r2 = l
		}
	}
	answered := make([]PageKey, len(entries))
	for i, e := range entries {
		answered[i] = PageKey{Index: e.Ctx.Index, Key: e.Key}
	}
	return CheckPageOrder(count, p, a, max, answered, pageKey(r0), pageKey(r1), pageKey(r2))
}

// PageKey is a page position: an index and the key there.
type PageKey struct {
	Index uint64
	Key   string
}

func pageKey(l *MapLeaf) *PageKey {
	if l == nil {
		return nil
	}
	return &PageKey{Index: l.Index, Key: l.Key}
}

// CheckPageOrder is checks 1 to 4 of spec section 11.3 on positions whose
// paths are already proven: the answered entries, and the role 0, role 1
// and role 2 leaves (nil when absent).
func CheckPageOrder(count uint64, p, a string, max int, answered []PageKey, r0, r1, r2 *PageKey) error {
	var s uint64
	switch {
	case len(answered) > 0:
		s = answered[0].Index
	case r2 != nil:
		s = r2.Index
	case r1 != nil:
		s = r1.Index
	default:
		s = count
	}
	// 1. every answered key is in the range, and the indices run on from s.
	for i, e := range answered {
		if !inRange(e.Key, p, a) || e.Index != s+uint64(i) {
			return mapErr("check-1")
		}
	}
	// 2. role 0 exactly when s > 0, at s - 1, strictly before the range.
	if (r0 != nil) != (s > 0) {
		return mapErr("check-2")
	}
	if r0 != nil && (r0.Index != s-1 || !below(r0.Key, p, a)) {
		return mapErr("check-2")
	}
	next := s + uint64(len(answered))
	// 3. a role 2 leaf is the next index, in the range, with no role 1.
	if r2 != nil && (r2.Index != next || !inRange(r2.Key, p, a) || r1 != nil) {
		return mapErr("check-3")
	}
	// 4. unless full or stopped at role 2: role 1 exactly when the next index
	// is below count, at it, strictly after the range.
	if len(answered) != max && r2 == nil {
		if (r1 != nil) != (next < count) {
			return mapErr("check-4")
		}
		if r1 != nil && (r1.Index != next || !above(r1.Key, p, a)) {
			return mapErr("check-4")
		}
	}
	return nil
}

// ProvePage is the honest page for prefix p after a of at most max entries,
// stopping at the first entry in range that held refuses (role 2).
func (m Map) ProvePage(p, a string, max int, held func([32]byte) bool) ([]PageEntry, []MapLeaf) {
	keys := m.Keys()
	n := len(keys)
	s := n
	for i, k := range keys {
		if !below(k, p, a) {
			s = i
			break
		}
	}
	// s is the first index not strictly before the range; the first in range
	// is s when that key is in the range.
	var entries []PageEntry
	var leaves []MapLeaf
	if s > 0 {
		leaves = append(leaves, m.leaf(keys, RoleBefore, s-1))
	}
	i := s
	for ; i < n && len(entries) < max && inRange(keys[i], p, a); i++ {
		if !held(m[keys[i]]) {
			return entries, append(leaves, m.leaf(keys, RoleUnheld, i))
		}
		entries = append(entries, PageEntry{Key: keys[i], C: m[keys[i]], Ctx: EntryContext{Index: uint64(i), Path: m.path(keys, i)}})
	}
	if len(entries) < max && i < n {
		leaves = append(leaves, m.leaf(keys, RoleAfter, i))
	}
	return entries, leaves
}
