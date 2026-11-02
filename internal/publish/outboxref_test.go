package publish

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"testing"
	"time"
)

// A carrier kept by reference comes back byte for byte: the item names the
// object and keeps the head and tail around it, bound by their own sum, no
// hash of the whole BEEF. An item an earlier build wrote, its Sum over the
// whole BEEF, is read too; one whose head or tail changed is not; one whose
// object is not placed is kept whole.
func TestOutboxRefRoundTrip(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 3<<20)
	_, _ = rand.Read(data)
	h, err := store.Put(data)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	o, err := openOutbox(t.TempDir(), &Delivery{}, store, func(string, ...any) {}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	targets := []Target{{Base: "http://p", Topic: "t"}}
	head, tail := []byte("the carrier's bytes before the object"), []byte("and after")
	beef := slices.Concat(head, data, tail)
	b, err := o.newBatch()
	if err != nil {
		t.Fatal(err)
	}
	it, err := o.addRef(b, "aa", "piece", beef, h, len(head), len(data), targets)
	if err != nil {
		t.Fatal(err)
	}
	if it.Ref != hex.EncodeToString(h[:]) || !bytes.Equal(it.Head, head) || !bytes.Equal(it.Tail, tail) || it.Ver != refVersion || it.Sum != refSum(head, tail) {
		t.Fatalf("kept as %+v", it)
	}
	o.done(it.ID)
	b.close()
	items := o.load()
	if len(items) != 1 || items[0].ID != "aa" || items[0].Ver != refVersion {
		t.Fatalf("loaded %d item(s): %+v", len(items), items)
	}
	got, err := o.beefOf(items[0])
	if err != nil || !bytes.Equal(got, beef) {
		t.Fatalf("rebuilt %d bytes (%v), want the %d of the BEEF", len(got), err, len(beef))
	}
	o.remove(items[0])
	o.closeBatches(items)
	// A tail or a head that changed is refused, read from the store or not.
	bad := *items[0]
	bad.Tail = append(slices.Clone(bad.Tail), 0)
	if _, err := o.beefOf(&bad); err == nil {
		t.Fatal("a tail that changed rebuilt a BEEF")
	}
	bad = *items[0]
	bad.Head, bad.Tail = bad.Head[:len(bad.Head)-1], append([]byte{bad.Head[len(bad.Head)-1]}, bad.Tail...)
	if _, err := o.beefOf(&bad); err == nil {
		t.Fatal("a head and tail that moved their meeting point rebuilt a BEEF")
	}
	// An item an earlier build wrote: no Ver, Sum over the whole BEEF.
	sum := sha256.Sum256(beef)
	old := &outItem{ID: "bb", What: "piece", Targets: targets, Created: now, Next: now, Ref: hex.EncodeToString(h[:]), Head: head, Tail: tail, Sum: hex.EncodeToString(sum[:])}
	if err := o.write(old); err != nil {
		t.Fatal(err)
	}
	items = o.load()
	if len(items) != 1 || items[0].ID != "bb" || items[0].Ver != 0 {
		t.Fatalf("loaded %d item(s): %+v", len(items), items)
	}
	if got, err := o.beefOf(items[0]); err != nil || !bytes.Equal(got, beef) {
		t.Fatalf("an item of the old form rebuilt %d bytes (%v)", len(got), err)
	}
	old.Tail = append(slices.Clone(tail), 0)
	if _, err := o.beefOf(old); err == nil {
		t.Fatal("an item of the old form whose tail changed rebuilt a BEEF")
	}
	o.remove(old)
	// An object not placed: the carrier is kept whole, in a file of its own.
	b, _ = o.newBatch()
	whole, err := o.addRef(b, "cc", "piece", beef, h, -1, len(data), targets)
	if err != nil {
		t.Fatal(err)
	}
	o.done(whole.ID)
	b.close()
	if whole.Ref != "" || whole.Head != nil {
		t.Fatalf("kept by reference without a place: %+v", whole)
	}
	if got, err := o.beefOf(whole); err != nil || !bytes.Equal(got, beef) {
		t.Fatalf("the carrier kept whole: %d bytes (%v)", len(got), err)
	}
}
