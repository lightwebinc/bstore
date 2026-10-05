package anchor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func c(s string) [32]byte { return sha256.Sum256([]byte(s)) }

func sizer() func([]storerec.Change, [][32]byte) (int, error) {
	return RecordSizer(storerec.Bucket{Bucket: "example_photos_hmqzvtkwrx", Owner: append([]byte{0x02}, bytes.Repeat([]byte{0x11}, 32)...),
		Seq: storerec.MaxSafe, Count: storerec.MaxKeysInMap, Created: 253402300799999, Writers: [][]byte{}})
}

// fold applies plan's steps as a host folds tokens, from current.
func foldPlan(t *testing.T, current storerec.Map, named map[[32]byte]bool, p *Plan) storerec.Map {
	t.Helper()
	m := current.Clone()
	seen := map[[32]byte]bool{}
	for k := range named {
		seen[k] = true
	}
	for i, s := range p.Steps {
		for j := 1; j < len(s.Changes); j++ {
			if s.Changes[j-1].Key >= s.Changes[j].Key {
				t.Fatalf("step %d: changes not ascending", i)
			}
		}
		for j := 1; j < len(s.Consumed); j++ {
			if bytes.Compare(s.Consumed[j-1][:], s.Consumed[j][:]) >= 0 {
				t.Fatalf("step %d: consumed not ascending", i)
			}
		}
		for _, ch := range s.Changes {
			if seen[ch.C] {
				t.Fatalf("step %d: %x named twice", i, ch.C[:4])
			}
			seen[ch.C] = true
			cur, ok := m[ch.Key]
			if ch.Removal {
				if !ok {
					t.Fatalf("step %d: a removal of an absent key %s", i, ch.Key)
				}
				delete(m, ch.Key)
			} else {
				if ok && cur == ch.C {
					t.Fatalf("step %d: a put that changes nothing", i)
				}
				m[ch.Key] = ch.C
			}
		}
		for _, x := range s.Consumed {
			if seen[x] {
				t.Fatalf("step %d: consumed %x named twice", i, x[:4])
			}
			seen[x] = true
		}
		if s.Map.Root() != m.Root() || len(s.Map) != len(m) {
			t.Fatalf("step %d: the map after it is not the fold's", i)
		}
		n, err := sizer()(s.Changes, s.Consumed)
		if err != nil || n > storerec.MaxBucketRecord || len(s.Changes) > storerec.MaxChanges || len(s.Consumed) > storerec.MaxConsumed {
			t.Fatalf("step %d: %d bytes, %d changes, %d consumed", i, n, len(s.Changes), len(s.Consumed))
		}
	}
	return m
}

// A batch that fits one token is exactly spec section 3.4's resolution
// (storerec.ResolveBatch, which the map vectors pin).
func TestPlanMatchesResolveBatch(t *testing.T) {
	cur := storerec.Map{"a": c("a0"), "b": c("b0"), "same": c("same")}
	named := map[[32]byte]bool{c("old-delete"): true}
	batch := []storerec.Applicable{
		{C: c("a1"), Key: "a", Kind: storerec.KindPut, Created: 5},
		{C: c("a2"), Key: "a", Kind: storerec.KindPut, Created: 9},
		{C: c("a3"), Key: "a", Kind: storerec.KindPut, Created: 9},       // ties on created: the higher commitment decides
		{C: c("old-delete"), Key: "b", Kind: storerec.KindDelete},        // named: skipped, never applied again
		{C: c("z-del"), Key: "z", Kind: storerec.KindDelete},             // a delete of an absent key: consumed
		{C: c("same"), Key: "same", Kind: storerec.KindPut},              // the value the key names: consumed (and named? no: the map names it, a token named it)
		{C: c("b-del"), Key: "b", Kind: storerec.KindDelete, Created: 3}, // removes b
		{C: c("a1"), Key: "a", Kind: storerec.KindPut, Created: 5},       // the same entry twice: once
		{C: c("odd"), Key: "x", Kind: 7},                                 // an unknown kind: never applied
	}
	wantCh, wantCons := storerec.ResolveBatch(cur, named, batch)
	p, err := PlanBatch(cur, named, batch, sizer())
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Steps) != 1 {
		t.Fatalf("%d steps", len(p.Steps))
	}
	if !reflect.DeepEqual(p.Steps[0].Changes, wantCh) || !reflect.DeepEqual(p.Steps[0].Consumed, wantCons) {
		t.Fatalf("plan %+v\nresolve %+v %x", p.Steps[0], wantCh, wantCons)
	}
	foldPlan(t, cur, named, p)
}

// Nothing to apply plans no token.
func TestPlanEmpty(t *testing.T) {
	named := map[[32]byte]bool{c("x"): true}
	p, err := PlanBatch(storerec.Map{}, named, []storerec.Applicable{{C: c("x"), Key: "k", Kind: storerec.KindPut}}, sizer())
	if err != nil || len(p.Steps) != 0 {
		t.Fatalf("%v %+v", err, p)
	}
}

// Changes that do not fit one record of 65,536 bytes split over tokens,
// each within the bound, folding to the map one token would have made.
func TestPlanSplitsAtTheRecordBound(t *testing.T) {
	var batch []storerec.Applicable
	want := storerec.Map{}
	for i := 0; i < 400; i++ {
		k := fmt.Sprintf("long/%04d/%s", i, strings.Repeat("x", 900))
		batch = append(batch, storerec.Applicable{C: c(k), Key: k, Kind: storerec.KindPut, Created: uint64(i)})
		want[k] = c(k)
	}
	p, err := PlanBatch(storerec.Map{}, nil, batch, sizer())
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Steps) < 6 {
		t.Fatalf("%d steps for about 380 KB of changes", len(p.Steps))
	}
	got := foldPlan(t, storerec.Map{}, nil, p)
	if got.Root() != want.Root() {
		t.Fatal("the split fold differs")
	}
}

// More than 1024 changes split at the count bound.
func TestPlanSplitsAtTheChangeCount(t *testing.T) {
	var batch []storerec.Applicable
	for i := 0; i < 2500; i++ {
		k := fmt.Sprintf("k%05d", i)
		batch = append(batch, storerec.Applicable{C: c(k), Key: k, Kind: storerec.KindPut})
	}
	p, err := PlanBatch(storerec.Map{}, nil, batch, sizer())
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Steps) != 3 {
		t.Fatalf("%d steps", len(p.Steps))
	}
	if m := foldPlan(t, storerec.Map{}, nil, p); len(m) != 2500 {
		t.Fatalf("%d keys", len(m))
	}
}

// One key written 3000 times in a batch: 2999 superseded puts consumed,
// spilling into tokens of consumed alone, and one change, applied once.
func TestPlanSpillsOneKeysConsumed(t *testing.T) {
	var batch []storerec.Applicable
	for i := 0; i < 3000; i++ {
		batch = append(batch, storerec.Applicable{C: c(fmt.Sprint(i)), Key: "hot", Kind: storerec.KindPut, Created: uint64(i)})
	}
	p, err := PlanBatch(storerec.Map{}, nil, batch, sizer())
	if err != nil {
		t.Fatal(err)
	}
	changes, consumed := 0, 0
	for _, s := range p.Steps {
		changes += len(s.Changes)
		consumed += len(s.Consumed)
	}
	if changes != 1 || consumed != 2999 {
		t.Fatalf("%d changes, %d consumed", changes, consumed)
	}
	m := foldPlan(t, storerec.Map{}, nil, p)
	if m["hot"] != c("2999") {
		t.Fatal("the last put does not decide")
	}
}

// The tooling's whole-chain property: whatever a host answers again, an
// entry a token named is never applied twice. The second run's batch is
// the first's plus a new entry, against the chain after the first run.
func TestPlanNeverAppliesTwice(t *testing.T) {
	batch := []storerec.Applicable{
		{C: c("p1"), Key: "k", Kind: storerec.KindPut, Created: 1},
		{C: c("d1"), Key: "k", Kind: storerec.KindDelete, Created: 2},
		{C: c("p2"), Key: "j", Kind: storerec.KindPut, Created: 1},
	}
	p1, err := PlanBatch(storerec.Map{}, nil, batch, sizer())
	if err != nil {
		t.Fatal(err)
	}
	named := map[[32]byte]bool{}
	m := foldPlan(t, storerec.Map{}, nil, p1)
	for _, s := range p1.Steps {
		for _, ch := range s.Changes {
			named[ch.C] = true
		}
		for _, x := range s.Consumed {
			named[x] = true
		}
	}
	// A later put of k, and the old entries again.
	batch2 := append(batch, storerec.Applicable{C: c("p3"), Key: "k", Kind: storerec.KindPut, Created: 3})
	p2, err := PlanBatch(m, named, batch2, sizer())
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Steps) != 1 || len(p2.Steps[0].Changes) != 1 || p2.Steps[0].Changes[0].C != c("p3") || len(p2.Steps[0].Consumed) != 0 {
		t.Fatalf("second run: %+v", p2.Steps)
	}
	m2 := foldPlan(t, m, named, p2)
	if m2["k"] != c("p3") || m2["j"] != c("p2") {
		t.Fatal("the map after the second run")
	}
	// The delete that removed nothing (k was absent when it applied, put
	// and deleted in one batch) is consumed, and not found again.
	if !named[c("d1")] || !named[c("p1")] {
		t.Fatal("the first run did not name every entry")
	}
}

// Every token of a split encodes at its own seq: the sizing takes the
// widest seq, so a later token whose seq needs another byte still fits.
func TestPlanFitsAtEveryTokensSeq(t *testing.T) {
	cur := storerec.Map{}
	for i := 0; i < 70000; i++ {
		cur[fmt.Sprintf("a%06d", i)] = c(fmt.Sprint("m", i))
	}
	owner, _ := hex.DecodeString("02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5")
	for trial := 0; trial < 20; trial++ {
		latest := uint64(254) // the next tokens' seqs cross a CBOR width
		base := SizingBase("example_photos_hmqzvtkwrx", owner, c("id"), c("prev"), [][]byte{})
		var batch []storerec.Applicable
		for i := 0; i < 2500; i++ {
			k := fmt.Sprintf("k%05d/%s", i, strings.Repeat("x", 60+(i*7+trial*13)%140))
			batch = append(batch, storerec.Applicable{C: c(fmt.Sprint(trial, i)), Key: k, Kind: storerec.KindPut, Created: 1})
		}
		plan, err := PlanBatch(cur, nil, batch, RecordSizer(base))
		if err != nil {
			t.Fatal(err)
		}
		for i, s := range plan.Steps {
			b := base
			b.Seq = latest + 1 + uint64(i)
			b.Changes, b.Consumed = s.Changes, s.Consumed
			if b.Consumed == nil {
				b.Consumed = [][32]byte{}
			}
			b.Count, b.Created = uint64(len(s.Map)), 1_800_000_000_000
			if _, err := b.Encode(); err != nil {
				t.Fatalf("trial %d step %d seq %d: %v", trial, i, b.Seq, err)
			}
		}
	}
}
