package publish

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// syncLog records a committer's syncs and what was on disk at each.
type syncLog struct {
	mu     sync.Mutex
	events []string
}

func (l *syncLog) add(s string) {
	l.mu.Lock()
	l.events = append(l.events, s)
	l.mu.Unlock()
}

func (l *syncLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// testCommitter is a committer whose syncs are fakes: with syncfs, one
// filesystem; without, fsync of each file and directory.
func testCommitter(syncfs bool, sync func(f *os.File) error) *committer {
	c := &committer{fsync: sync, mode: "group"}
	if syncfs {
		c.syncfs = sync
		c.devOf = func(*os.File) (uint64, error) { return 1, nil }
		c.fsync = func(*os.File) error { panic("fsync with syncfs") }
	}
	return c
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func emptyDir(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("%s holds %d entries, want none", dir, len(ents))
	}
}

// The data is durable before any rename, the renames are durable before
// the writer is released.
func TestCommitOrder(t *testing.T) {
	for _, syncfs := range []bool{true, false} {
		t.Run(fmt.Sprintf("syncfs=%v", syncfs), func(t *testing.T) {
			dir := t.TempDir()
			tmp, out := filepath.Join(dir, "tmp"), filepath.Join(dir, "out")
			for _, d := range []string{tmp, out} {
				if err := os.Mkdir(d, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			final := filepath.Join(out, "a.json")
			var log syncLog
			c := testCommitter(syncfs, func(f *os.File) error {
				kind := "file"
				if info, err := f.Stat(); err == nil && info.IsDir() {
					kind = "dir"
				}
				log.add(fmt.Sprintf("%s final=%v", kind, exists(final)))
				return nil
			})
			if err := c.write(tmp, final, []byte("whole")); err != nil {
				t.Fatal(err)
			}
			log.add("released")
			got := strings.Join(log.all(), ";")
			want := "file final=false;dir final=true;released"
			if syncfs {
				want = "file final=false;file final=true;released"
			}
			if got != want {
				t.Fatalf("syncs %q, want %q", got, want)
			}
			if b, _ := os.ReadFile(final); string(b) != "whole" {
				t.Fatalf("final holds %q", b)
			}
			emptyDir(t, tmp)
		})
	}
}

// Many writers share few commits, and each finds its own bytes.
func TestCommitGroups(t *testing.T) {
	for _, syncfs := range []bool{true, false} {
		t.Run(fmt.Sprintf("syncfs=%v", syncfs), func(t *testing.T) {
			dir := t.TempDir()
			var calls atomic.Int64
			c := testCommitter(syncfs, func(*os.File) error {
				calls.Add(1)
				time.Sleep(2 * time.Millisecond)
				return nil
			})
			const writers = 200
			var wg sync.WaitGroup
			errs := make(chan error, writers)
			for i := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs <- c.write(dir, filepath.Join(dir, strconv.Itoa(i)), []byte(strconv.Itoa(i)))
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			for i := range writers {
				if b, err := os.ReadFile(filepath.Join(dir, strconv.Itoa(i))); err != nil || string(b) != strconv.Itoa(i) {
					t.Fatalf("file %d: %q %v", i, b, err)
				}
			}
			n := c.commits.Load()
			if n >= writers/4 {
				t.Fatalf("%d writers took %d commits", writers, n)
			}
			if c.files.Load() != writers {
				t.Fatalf("files %d, want %d", c.files.Load(), writers)
			}
			if syncfs && calls.Load() != 2*n {
				t.Fatalf("%d syncfs calls for %d commits, want two each", calls.Load(), n)
			}
			t.Logf("%d writers, %d commits, %d syncs", writers, n, calls.Load())
		})
	}
}

// A failed data sync fails every writer of its group, and none of their
// files appears, under its final name or in tmp.
func TestCommitSyncErrorFailsTheGroup(t *testing.T) {
	for _, syncfs := range []bool{true, false} {
		t.Run(fmt.Sprintf("syncfs=%v", syncfs), func(t *testing.T) {
			dir := t.TempDir()
			tmp, out := filepath.Join(dir, "tmp"), filepath.Join(dir, "out")
			for _, d := range []string{tmp, out} {
				if err := os.Mkdir(d, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			bad := errors.New("injected EIO")
			gate, entered := make(chan struct{}), make(chan struct{})
			var first sync.Once
			// With syncfs the first group syncs twice (data, renames) and
			// every later sync fails.
			var calls atomic.Int64
			c := testCommitter(syncfs, func(f *os.File) error {
				first.Do(func() {
					close(entered)
					<-gate
				})
				if syncfs && calls.Add(1) > 2 {
					return bad
				}
				return nil
			})
			// The first writer's commit holds the committer while the
			// others gather into the next group.
			firstErr := make(chan error, 1)
			go func() { firstErr <- c.write(tmp, filepath.Join(out, "first"), []byte("x")) }()
			<-entered
			const writers = 20
			var wg sync.WaitGroup
			errs := make(chan error, writers+1)
			for i := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs <- c.write(tmp, filepath.Join(out, strconv.Itoa(i)), []byte("y"))
				}()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- c.syncDir(out)
			}()
			for {
				c.mu.Lock()
				n := 0
				if c.next != nil {
					n = len(c.next.entries)
				}
				c.mu.Unlock()
				if n == writers+1 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			// Without syncfs the first group's directory sync marks the
			// second group, whose file syncs fail.
			if !syncfs {
				var group2 atomic.Bool
				c.fsync = func(f *os.File) error {
					if info, err := f.Stat(); err == nil && info.IsDir() {
						group2.Store(true)
						return nil
					}
					if group2.Load() {
						return bad
					}
					return nil
				}
			}
			close(gate)
			if err := <-firstErr; err != nil {
				t.Fatalf("first writer: %v", err)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if !errors.Is(err, bad) {
					t.Fatalf("a writer of the failed group got %v", err)
				}
			}
			ents, _ := os.ReadDir(out)
			if len(ents) != 1 || ents[0].Name() != "first" {
				t.Fatalf("out holds %v, want only first", ents)
			}
			emptyDir(t, tmp)
		})
	}
}

// writeAll joins one group; syncFile and syncDir join it too.
func TestCommitWriteAllAndSyncs(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int64
	c := testCommitter(true, func(*os.File) error { calls.Add(1); return nil })
	var paths []string
	var bs [][]byte
	for i := range 5 {
		paths = append(paths, filepath.Join(dir, fmt.Sprint(i)))
		bs = append(bs, []byte{byte(i)})
	}
	if err := c.writeAll(dir, paths, bs); err != nil {
		t.Fatal(err)
	}
	if c.commits.Load() != 1 || calls.Load() != 2 {
		t.Fatalf("writeAll: %d commits, %d syncs; want 1, 2", c.commits.Load(), calls.Load())
	}
	for i, p := range paths {
		if b, _ := os.ReadFile(p); !bytes.Equal(b, bs[i]) {
			t.Fatalf("%s holds %v", p, b)
		}
	}
	f, err := os.Create(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := c.syncFile(f); err != nil {
		t.Fatal(err)
	}
	if err := c.syncDir(dir); err != nil {
		t.Fatal(err)
	}
	// A group with no rename syncs once.
	if c.commits.Load() != 3 || calls.Load() != 4 {
		t.Fatalf("%d commits, %d syncs; want 3, 4", c.commits.Load(), calls.Load())
	}
	// The file synced alone stays open.
	if _, err := f.Write([]byte("more")); err != nil {
		t.Fatal(err)
	}
}

// The process's committer, with the platform's real syncs.
func TestCommitPlatform(t *testing.T) {
	dir := t.TempDir()
	before := Commits()
	if err := writeFileSync(dir, filepath.Join(dir, "a"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a")); string(b) != "a" {
		t.Fatalf("read %q", b)
	}
	after := Commits()
	if after.Commits <= before.Commits || after.Syncs <= before.Syncs {
		t.Fatalf("counts did not move: %+v then %+v", before, after)
	}
}

// Not grouped, a write commits at once by fsyncs: its data, then its
// directory, then the writer is released; writeAll fsyncs its files at
// once and their directory once.
func TestCommitDirect(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "out", "a")
	if err := os.Mkdir(filepath.Dir(final), 0o700); err != nil {
		t.Fatal(err)
	}
	var log syncLog
	c := &committer{mode: "direct", fsync: func(f *os.File) error {
		kind := "file"
		if info, err := f.Stat(); err == nil && info.IsDir() {
			kind = "dir"
		}
		log.add(fmt.Sprintf("%s final=%v", kind, exists(final)))
		return nil
	}}
	if err := c.write(dir, final, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(log.all(), ";"); got != "file final=false;dir final=true" {
		t.Fatalf("syncs %q", got)
	}
	paths := []string{filepath.Join(dir, "out", "b"), filepath.Join(dir, "out", "c")}
	if err := c.writeAll(dir, paths, [][]byte{{1}, {2}}); err != nil {
		t.Fatal(err)
	}
	if c.syncs.Load() != 5 || c.commits.Load() != 2 {
		t.Fatalf("%d syncs, %d commits; want 5, 2", c.syncs.Load(), c.commits.Load())
	}
	if c.running {
		t.Fatal("a direct write started the group committer")
	}
}

// auto asks the platform per directory, once.
func TestCommitAuto(t *testing.T) {
	asked := map[string]int{}
	c := &committer{mode: "auto", groups: func(dir string) bool { asked[dir]++; return dir == "/xfs" }}
	for range 3 {
		if !c.grouped("/xfs") || c.grouped("/ext4") {
			t.Fatal("auto chose wrong")
		}
	}
	if asked["/xfs"] != 1 || asked["/ext4"] != 1 {
		t.Fatalf("asked %v", asked)
	}
	if err := SetCommitMode("fast"); err == nil {
		t.Fatal("an unknown mode was taken")
	}
}
