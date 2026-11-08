package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"testing"
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
