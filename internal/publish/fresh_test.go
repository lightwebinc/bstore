package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/storerec"
)

// corrupt rewrites an object's file with other bytes of the same length:
// a read that holds it to its hash refuses it, one that skips the hash
// returns it.
func corrupt(t *testing.T, s *Store, h [32]byte) {
	t.Helper()
	b, err := os.ReadFile(s.objectPath(h))
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(s.objectPath(h), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A put's piece pinned by the outbox while the put's stage still marks it
// stays fresh after the stage lets go: the outbox's later mint and its
// resends read it without hashing again. Once the pin goes, and in a new
// process (an outbox reloaded after a restart), it is held to its hash.
func TestOutboxPinKeepsAStagedPieceFresh(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("piece"), 1000)
	h := sha256.Sum256(body)
	if _, wrote, err := st.putHashed(h, body, true); err != nil || !wrote {
		t.Fatalf("stage write: %v %v", wrote, err)
	}
	fresh := st.pinFresh(h) // the outbox's pin, taken while the stage holds it
	if !fresh {
		t.Fatal("a pin taken while the stage marks the object is not fresh")
	}
	st.unstage([][32]byte{h})
	st.unpin([][32]byte{h})
	corrupt(t, st, h)
	p := &Publisher{cfg: Config{Store: st}}
	got, err := p.readPiece(context.Background(), nil, h)
	if err != nil || bytes.Equal(got, body) {
		t.Fatalf("the outbox's read of a fresh piece hashed it: %v", err)
	}
	if _, err := st.readInto(h, false, nil); err == nil {
		t.Fatal("a read that asks for the hash skipped it")
	}
	// The file went with that refusal: put it back, then let the pin go.
	if _, _, err := st.putHashed(h, body, false); err != nil {
		t.Fatal(err)
	}
	st.unpinFresh(h, fresh)
	corrupt(t, st, h)
	if _, err := p.readPiece(context.Background(), nil, h); err == nil {
		t.Fatal("a piece no fresh pin holds was read without its hash")
	}
	// A new process: the outbox reloads the item and pins the object, which
	// no stage of this process wrote.
	if _, _, err := st.putHashed(h, body, false); err != nil {
		t.Fatal(err)
	}
	again, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if again.pinFresh(h) {
		t.Fatal("a pin on bytes an earlier run left is fresh")
	}
	corrupt(t, again, h)
	if _, err := (&Publisher{cfg: Config{Store: again}}).readPiece(context.Background(), nil, h); err == nil {
		t.Fatal("bytes an earlier run left were read without their hash")
	}
}

// The outbox's resend of a minted piece by reference (beefOf) reads the
// object fresh while the item's pin is fresh: the head, the object as it
// is on disk, the tail. Once the item lets go of it, the read holds the
// object to its hash.
func TestOutboxResendReadsAFreshPieceUnhashed(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	o, err := openOutbox(filepath.Join(dir, "outbox"), &Delivery{}, st, t.Logf, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("carried"), 1000)
	h := sha256.Sum256(body)
	if _, _, err := st.putHashed(h, body, true); err != nil {
		t.Fatal(err)
	}
	head, tail := []byte("head"), []byte("tail")
	it := &outItem{ID: "item", Ref: hex.EncodeToString(h[:]), Head: head, Tail: tail, Ver: refVersion}
	it.Sum = refSum(head, tail)
	o.pinRef(it) // while the stage marks the object
	st.unstage([][32]byte{h})
	st.unpin([][32]byte{h})
	corrupt(t, st, h)
	beef, err := o.beefOf(it)
	if err != nil {
		t.Fatalf("a fresh resend was hashed: %v", err)
	}
	if !bytes.HasPrefix(beef, head) || !bytes.HasSuffix(beef, tail) || len(beef) != len(head)+len(body)+len(tail) {
		t.Fatal("the resend is not head, object, tail")
	}
	o.unpinRef(it.ID)
	if _, err := o.beefOf(it); err == nil {
		t.Fatal("a resend the item no longer pins read corrupt bytes")
	}
}

// A checked read that removes a corrupt object takes its freshness with it:
// bytes written under the same name afterwards without a stage's hash (a
// part linked in) are held to their hash, though the old pin is still held.
func TestRemovedObjectLosesItsFreshness(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("object"), 1000)
	h := sha256.Sum256(body)
	if _, _, err := st.putHashed(h, body, true); err != nil {
		t.Fatal(err)
	}
	fresh := st.pinFresh(h)
	st.unstage([][32]byte{h})
	corrupt(t, st, h)
	if _, err := st.readInto(h, false, nil); err == nil {
		t.Fatal("a checked read took corrupt bytes")
	}
	// Bytes under the same name again, not through a stage that hashed them.
	bad := bytes.Clone(body)
	bad[1] ^= 0xff
	if err := os.WriteFile(st.objectPath(h), bad, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.readInto(h, true, nil); err == nil {
		t.Fatal("replacement bytes inherited a removed object's freshness")
	}
	st.unpinFresh(h, fresh)
	if len(st.fresh) != 0 {
		t.Fatal("a pin released after its object's removal left freshness behind")
	}
}

// A stage keeps each blob's leaves while the blob is staged or a fresh pin
// holds it, and lets them go with the last mark, or with the blob's file.
func TestStoreKeepsLeavesWhileFresh(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("leaves, "), (storerec.BlobSize+4097)/8)
	s, err := st.Stage(context.Background(), bytes.NewReader(content), uint64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Manifest.Blobs) != 2 {
		t.Fatalf("%d blobs", len(s.Manifest.Blobs))
	}
	var hs [2][32]byte
	for i, x := range s.Manifest.Blobs {
		b, err := hex.DecodeString(x)
		if err != nil {
			t.Fatal(err)
		}
		hs[i] = [32]byte(b)
	}
	want, _ := storerec.LeavesOf(content[:storerec.BlobSize])
	if got := st.leavesOf(hs[0]); !bytes.Equal(got, want) {
		t.Fatalf("staged: %d bytes of leaves, want %d", len(got), len(want))
	}
	if st.leavesOf(s.Root) != nil {
		t.Fatal("the root has leaves")
	}
	fresh := st.pinFresh(hs[0])
	s.Release()
	if st.leavesOf(hs[0]) == nil || st.leavesOf(hs[1]) != nil {
		t.Fatal("after the stage let go: a pinned blob keeps its leaves, the other none")
	}
	st.unpinFresh(hs[0], fresh)
	if st.leavesOf(hs[0]) != nil {
		t.Fatal("leaves kept past the last pin")
	}

	// Staged again, the blobs are found held, not written: no leaves. A
	// blob written anew whose file then goes takes its leaves with it.
	again, err := st.Stage(context.Background(), bytes.NewReader(content), uint64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	defer again.Release()
	if st.leavesOf(hs[0]) != nil {
		t.Fatal("leaves for a blob the stage found already held")
	}
	other := bytes.Repeat([]byte("other leaves, "), 4096)
	s, err = st.Stage(context.Background(), bytes.NewReader(other), uint64(len(other)))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	b, err := hex.DecodeString(s.Manifest.Blobs[0])
	if err != nil {
		t.Fatal(err)
	}
	h := [32]byte(b)
	if st.leavesOf(h) == nil {
		t.Fatal("a blob written anew has no leaves")
	}
	st.mu.Lock()
	st.forgetLocked(h)
	kept := st.leavesBytes
	st.mu.Unlock()
	if st.leavesOf(h) != nil {
		t.Fatal("leaves kept for a forgotten object")
	}
	if kept != len(st.leavesOf(hs[0]))+len(st.leavesOf(hs[1])) {
		t.Fatalf("%d bytes of leaves counted", kept)
	}

	// Past the bound a blob keeps none (its provider hashes it).
	defer func(n int) { maxLeavesBytes = n }(maxLeavesBytes)
	maxLeavesBytes = storerec.LeavesLen(storerec.BlobSize)
	big := append(bytes.Repeat([]byte{'a'}, storerec.BlobSize), bytes.Repeat([]byte{'b'}, storerec.BlobSize)...)
	s2, err := st.Stage(context.Background(), bytes.NewReader(big), uint64(len(big)))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Release()
	n := 0
	for _, x := range s2.Manifest.Blobs {
		b, _ := hex.DecodeString(x)
		if st.leavesOf([32]byte(b)) != nil {
			n++
		}
	}
	st.mu.Lock()
	total := st.leavesBytes
	st.mu.Unlock()
	if n != 1 || total > maxLeavesBytes {
		t.Fatalf("%d of 2 blobs kept leaves, %d bytes, past a bound of one blob's", n, total)
	}
}
