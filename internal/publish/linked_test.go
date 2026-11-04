package publish

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lightwebinc/bcommon/chirp"
)

// TestStageLinkedMatchesStage stages content from blob files, linked into
// the store and, from another file system where there is one, copied, and
// holds the result to Stage of the same bytes.
func TestStageLinkedMatchesStage(t *testing.T) {
	const c = chirp.ChunkSize
	dirs := []string{t.TempDir()}
	if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
		if d, err := os.MkdirTemp("/dev/shm", "bstore-linked-"); err == nil {
			t.Cleanup(func() { os.RemoveAll(d) })
			dirs = append(dirs, d)
		}
	}
	same := make([]byte, c)
	_, _ = rand.Read(same)
	for _, size := range []int{0, 1, c - 1, c, c + 1, 3*c + 12345} {
		for _, repeat := range []bool{false, true} {
			for _, src := range dirs {
				content := make([]byte, size)
				_, _ = rand.Read(content)
				if repeat {
					for off := 0; off+c <= size; off += c {
						copy(content[off:], same)
					}
				}
				st, err := OpenStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				var blobs []LinkedBlob
				for i, off := 0, 0; off < size; i, off = i+1, off+c {
					b := content[off:min(off+c, size)]
					h, seg, err := HashBlob(b)
					if err != nil {
						t.Fatal(err)
					}
					p := filepath.Join(src, fmt.Sprintf("%d-%t-%06d", size, repeat, i))
					if err := os.WriteFile(p, b, 0o600); err != nil {
						t.Fatal(err)
					}
					blobs = append(blobs, LinkedBlob{Path: p, Hash: h, SegRoot: seg, Length: uint64(len(b)), Fresh: i%2 == 0})
				}
				got, err := st.StageLinked(context.Background(), blobs, sha256.Sum256(content))
				if err != nil {
					t.Fatal(size, err)
				}
				ref, err := OpenStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				want, err := ref.Stage(context.Background(), bytes.NewReader(content), uint64(size))
				if err != nil {
					t.Fatal(err)
				}
				if got.Root != want.Root || got.ContentHash != want.ContentHash || got.ProofRoot != want.ProofRoot ||
					got.Size != want.Size || !reflect.DeepEqual(got.Manifest, want.Manifest) {
					t.Fatalf("size %d repeat %v: StageLinked differs from Stage", size, repeat)
				}
				for _, h := range objectsOf(got.Manifest) {
					if _, err := st.Get(h); err != nil {
						t.Fatalf("object %x: %v", h[:4], err)
					}
				}
				got.Release()
				want.Release()
				st.mu.Lock()
				left := len(st.pins) + len(st.staged)
				st.mu.Unlock()
				if left != 0 {
					t.Fatalf("%d pins or marks left after release", left)
				}
			}
		}
	}
}
