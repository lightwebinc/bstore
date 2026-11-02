package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// A writer's delete ends the key's put: the holding is dropped at once and
// its objects leave the store.
func TestDeleteReleasesTheHolding(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, true)

	data := content(2*storerec.BlobSize+777, 3)
	put := r.put(ctx, []*Provider{p}, data, "photos/gone.jpg", testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	root := put.Object.Built.RootHash
	rootHex := hex.EncodeToString(root[:])
	r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(root) != nil })
	objs, err := p.rootObjects(rootHex)
	if err != nil || len(objs) == 0 {
		t.Fatalf("root objects: %v (%d)", err, len(objs))
	}

	del, err := r.writer.Delete(ctx, "photos/gone.jpg")
	if err != nil {
		t.Fatal(err)
	}
	ans, err := r.writer.Submit(ctx, "https://"+p.Addr(), storerec.RecordsTopic(testBucket), del.BEEF)
	if err != nil {
		t.Fatal(err)
	}
	if ok, why := testwriter.Admitted(ans, storerec.RecordsTopic(testBucket)); !ok {
		t.Fatalf("delete not taken: %s", why)
	}

	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	e := p.st.Entries[put.C]
	p.mu.Unlock()
	if h != nil {
		t.Fatal("the holding survived its writer's delete")
	}
	if e == nil || e.Decision != decisionDeleted {
		t.Fatalf("entry decision = %q, want %q", e.Decision, decisionDeleted)
	}
	for _, o := range objs {
		if p.store.Has(o) {
			t.Fatalf("object %x is still in the store", o[:4])
		}
	}
	if usage := func() uint64 { p.mu.Lock(); defer p.mu.Unlock(); return p.usageLocked() }(); usage != 0 {
		t.Fatalf("store usage %d after the delete, want 0", usage)
	}
}

// A delete made before the put it would name leaves the put alone.
func TestEarlierDeleteDoesNotReachALaterPut(t *testing.T) {
	r := newRig(t)
	p := r.start(r.config(filepath.Join(t.TempDir(), "p")), false)
	p.mu.Lock()
	p.st.Entries["c1"] = &EntryRec{C: "c1", Bucket: "b", Key: "k", Root: "r1", Created: 2000, Decision: decisionHeld}
	p.mu.Unlock()
	p.noteDelete(&storerec.Entry{Bucket: "b", Key: "k", Kind: storerec.KindDelete, Created: 1000})
	p.mu.Lock()
	d := p.st.Entries["c1"].Decision
	p.mu.Unlock()
	if d != decisionHeld {
		t.Fatalf("decision %q, want held", d)
	}
}

// Pieces that waited for a declined entry leave with the decision.
func TestDeclinedEntryReleasesItsPieces(t *testing.T) {
	r := newRig(t)
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, false)
	data := content(storerec.BlobSize/2, 4)
	h := sha256.Sum256(data)
	if err := p.store.Put(h, data); err != nil {
		t.Fatal(err)
	}
	hh := hex.EncodeToString(h[:])
	p.mu.Lock()
	p.st.Unclaimed[hh] = &Unclaimed{Root: "r9", Writer: "w", Size: uint64(len(data)), At: p.now().Unix()}
	p.st.Entries["c9"] = &EntryRec{C: "c9", Bucket: "b", Key: "k", Root: "r9", At: p.now().UnixNano()}
	p.mu.Unlock()
	p.decide("c9", "the closure did not arrive in time")
	p.mu.Lock()
	_, kept := p.st.Unclaimed[hh]
	p.mu.Unlock()
	if kept || p.store.Has(h) {
		t.Fatal("the declined entry's piece stayed")
	}
}

// A delete that arrives before the put it ends (the plane does not order
// entries) ends the put on arrival: its pieces leave, no closure is waited for.
func TestDeleteArrivingFirstEndsThePutOnArrival(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	p := r.start(cfg, true)

	data := content(2*storerec.BlobSize+99, 5)
	o, err := r.writer.Build(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	put, err := r.writer.Entry(ctx, o, "photos/late.jpg", testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30})
	if err != nil {
		t.Fatal(err)
	}
	del, err := r.writer.Delete(ctx, "photos/late.jpg")
	if err != nil {
		t.Fatal(err)
	}
	base := "https://" + p.Addr()
	for _, pc := range o.Pieces {
		if _, err := r.writer.Submit(ctx, base, storerec.BlobTopic(testBucket), pc); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.writer.Submit(ctx, base, storerec.RecordsTopic(testBucket), del.BEEF); err != nil {
		t.Fatal(err)
	}
	ans, err := r.writer.Submit(ctx, base, storerec.RecordsTopic(testBucket), put.BEEF)
	if err != nil {
		t.Fatal(err)
	}
	if ok, why := testwriter.Admitted(ans, storerec.RecordsTopic(testBucket)); !ok {
		t.Fatalf("put not taken: %s", why)
	}
	p.mu.Lock()
	e := p.st.Entries[put.C]
	unclaimed := len(p.st.Unclaimed)
	p.mu.Unlock()
	if e == nil || e.Decision != decisionDeleted {
		t.Fatalf("entry decision = %v, want deleted on arrival", e)
	}
	if unclaimed != 0 {
		t.Fatalf("%d pieces still wait for a put its delete ended", unclaimed)
	}
}
