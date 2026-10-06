package reader

import (
	"context"
	"os"
	"testing"

	"github.com/lightwebinc/bcommon/lookup"
)

// A walk's tokens survive a new Reader (a restart) on the same directory,
// are never used unverified, and go once the walk is done.
func TestWalkFileSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	c := Chain{Bucket: "b", Owner: []byte{2, 1}, ID: [32]byte{7}}
	a := &Reader{WalkDir: dir}
	a.walkKeep(c, [32]byte{1}, lookup.Output{Beef: []byte{1, 2, 3}, OutputIndex: 0})
	a.walkKeep(c, [32]byte{2}, lookup.Output{Beef: []byte{4}, OutputIndex: 1})
	a.walkKeep(c, [32]byte{2}, lookup.Output{Beef: []byte{4}, OutputIndex: 1})
	b := &Reader{WalkDir: dir}
	m := b.walkLoad(c)
	if len(m) != 2 || m[[32]byte{2}].OutputIndex != 1 || string(m[[32]byte{1}].Beef) != "\x01\x02\x03" {
		t.Fatalf("reloaded %v", m)
	}
	// Bytes that do not verify as the token asked for are not used.
	if tok := b.walkSaved(context.Background(), c, 0, [32]byte{1}); tok != nil {
		t.Fatal("an unverified token was used")
	}
	b.walkDone(c)
	if _, err := os.Stat(b.walkPath(c)); !os.IsNotExist(err) {
		t.Fatalf("file kept: %v", err)
	}
	if n := len((&Reader{WalkDir: dir}).walkLoad(c)); n != 0 {
		t.Fatalf("%d after done", n)
	}
}
