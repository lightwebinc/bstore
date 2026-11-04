package publish

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// The publisher's files are written through a temp file renamed into
// place, durably: the data synced before the rename, the rename synced
// before the write returns. Done one file at a time that is two syncs a
// file, and a small put writes a dozen files: on a drive that completes
// about 600 synced writes a second the gateway's small puts stopped near
// 100 a second, the drive flushing 66 % of the time (2026-11-04, about 7
// flushes a put).
//
// The committer can share the syncs among every write in flight (a group
// commit). A writer writes its temp file, unsynced, and joins the group
// gathering; the committer takes the groups one at a time and, for each,
// makes the data of every file in it durable, renames each into place,
// makes the renames durable, and only then releases the writers. While one
// group commits the next gathers, so the syncs a group costs are shared by
// every write that arrived during the commit before it. On Linux the syncs
// are one syncfs(2) per filesystem the group touches, which covers every
// file and directory in it; elsewhere (FreeBSD has no syncfs) each file
// and each distinct directory is fsynced, at once.
//
// Whether grouping pays depends on the filesystem, so it is chosen per
// filesystem (CommitMode). Measured with many small puts in flight
// (BenchmarkSmallPutDisk, 8 to 256 puts in flight, 2026-11-04): on XFS a
// group commit by syncfs took 1.3 to 1.9 times the puts a second of a sync
// per file, with 1.7 to 8.5 times fewer device flushes a put; on ext4 it
// lost (0.6 to 0.7 times the puts), because ext4's journal already commits concurrent fsyncs together
// and syncfs adds a writeback of the whole filesystem; grouping by fsyncs
// lost on both. So by default only XFS is grouped, and every other
// filesystem syncs each write as before, concurrently with the others.
//
// What a writer is told holds either way: a write that returns nil is
// durable under its final name, and no file is ever renamed into place
// before its data is durable, so a crash leaves each file whole or absent.
// Writes that must reach the disk in an order are still made in that
// order: the second is written only after the first returns, so it is in a
// later group.

// CommitMode chooses how synced writes reach the disk: "auto" (or empty)
// groups them on a filesystem where that measured faster (XFS on Linux)
// and syncs each alone elsewhere; "group" groups them everywhere; "direct"
// syncs each alone everywhere. bstore sets it from BSTORE_COMMIT.
var CommitMode = ""

// SetCommitMode sets CommitMode, refusing a name it does not know.
func SetCommitMode(mode string) error {
	switch mode {
	case "", "auto", "group", "direct":
		CommitMode = mode
		return nil
	}
	return fmt.Errorf("publish: commit mode %q is not auto, group or direct", mode)
}

// commitEntry is one writer's part of a commit: a temp file to sync and
// rename to path, an open file to sync, or a directory to sync.
type commitEntry struct {
	f    *os.File // the temp file, or the file or directory to sync
	tmp  string   // the temp file's name; empty for a sync alone
	path string   // where tmp goes
	dir  string   // a directory to sync; for a rename, path's
	own  bool     // f is the committer's to close
	err  error
}

type commitGroup struct {
	entries []*commitEntry
	done    chan struct{}
}

// committer runs the commits.
type committer struct {
	mu      sync.Mutex
	next    *commitGroup
	running bool

	// mode overrides CommitMode when set (tests, the benchmark).
	mode string
	// syncfs, when set, syncs a whole filesystem through an open file in it
	// (Linux), for a group; otherwise a group fsyncs its files and
	// directories one by one.
	syncfs func(*os.File) error
	// fsync syncs one file or directory; tests count or fail it.
	fsync func(*os.File) error
	// devOf names the filesystem a file is on, for syncfs.
	devOf func(*os.File) (uint64, error)
	// groups reports whether auto groups the writes of the filesystem a
	// directory is on; auto caches its answer per directory.
	groups func(dir string) bool
	auto   sync.Map

	commits, files, syncs atomic.Int64
	observe               atomic.Pointer[func(name string, seconds float64)]
}

// commits is the process's committer.
var commits = newCommitter()

func newCommitter() *committer {
	c := &committer{fsync: (*os.File).Sync}
	c.syncfs, c.devOf, c.groups = platformCommit()
	return c
}

// CommitStats are the committer's counts since the process started: the
// commits (a group, or one writer's own files when not grouped), the files
// and syncs they covered, and the device syncs (syncfs or fsync calls)
// they made.
type CommitStats struct {
	Commits, Files, Syncs int64
}

// Commits are the process's commit counts.
func Commits() CommitStats {
	return CommitStats{Commits: commits.commits.Load(), Files: commits.files.Load(), Syncs: commits.syncs.Load()}
}

// ObserveCommits has f told each commit's duration, in seconds, as
// `bstore_publish_commit_seconds`; nil stops it.
func ObserveCommits(f func(name string, seconds float64)) {
	if f == nil {
		commits.observe.Store(nil)
		return
	}
	commits.observe.Store(&f)
}

// grouped reports whether writes through dir join the group commit.
func (c *committer) grouped(dir string) bool {
	mode := c.mode
	if mode == "" {
		mode = CommitMode
	}
	switch mode {
	case "group":
		return true
	case "direct":
		return false
	}
	if c.groups == nil {
		return false
	}
	if v, ok := c.auto.Load(dir); ok {
		return v.(bool)
	}
	g := c.groups(dir)
	c.auto.Store(dir, g)
	return g
}

// write writes b to path through a temp file in tmp (on path's
// filesystem) and returns once it is durable under path.
func (c *committer) write(tmp, path string, b []byte) error {
	f, err := os.CreateTemp(tmp, "w-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	return c.join(tmp, &commitEntry{f: f, tmp: name, path: path, dir: filepath.Dir(path), own: true})
}

// writeAll is write of several files no one of which depends on another:
// they are committed together, and the first error is returned. Each file
// is whole or absent after a crash, but which of them a crash before the
// return keeps is not said.
func (c *committer) writeAll(tmp string, paths []string, bs [][]byte) error {
	es := make([]*commitEntry, 0, len(paths))
	fail := func(err error) error {
		for _, e := range es {
			e.f.Close()
			os.Remove(e.tmp)
		}
		return err
	}
	for i, path := range paths {
		f, err := os.CreateTemp(tmp, "w-*")
		if err != nil {
			return fail(err)
		}
		e := &commitEntry{f: f, tmp: f.Name(), path: path, dir: filepath.Dir(path), own: true}
		es = append(es, e)
		if _, err := f.Write(bs[i]); err != nil {
			return fail(err)
		}
	}
	return c.join(tmp, es...)
}

// syncFile returns once f's data is durable; f stays open.
func (c *committer) syncFile(f *os.File) error {
	return c.join(filepath.Dir(f.Name()), &commitEntry{f: f})
}

// syncDir returns once dir's entries (a file created in it) are durable.
func (c *committer) syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return c.join(dir, &commitEntry{f: d, dir: dir, own: true})
}

// join commits es: at once, by fsyncs, when the filesystem of dir is not
// grouped; else in the group gathering, starting the committer when it is
// idle. It returns once es are committed: the first error of es.
func (c *committer) join(dir string, es ...*commitEntry) error {
	if len(es) == 0 {
		return nil
	}
	if !c.grouped(dir) {
		c.commit(&commitGroup{entries: es}, nil)
		return firstErr(es)
	}
	c.mu.Lock()
	g := c.next
	if g == nil {
		g = &commitGroup{done: make(chan struct{})}
		c.next = g
	}
	g.entries = append(g.entries, es...)
	if !c.running {
		c.running = true
		go c.run()
	}
	c.mu.Unlock()
	<-g.done
	return firstErr(es)
}

func firstErr(es []*commitEntry) error {
	for _, e := range es {
		if e.err != nil {
			return e.err
		}
	}
	return nil
}

// run commits the groups one at a time until none is gathering.
func (c *committer) run() {
	for {
		c.mu.Lock()
		g := c.next
		c.next = nil
		if g == nil {
			c.running = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		c.commit(g, c.syncfs)
		close(g.done)
	}
}

// commit makes the entries of g durable: every file's data, then the
// renames, then the renames' durability. A failed data sync fails every
// entry and renames nothing. With syncfs set the syncs are one syncfs per
// filesystem; without, a fsync per file and per distinct directory.
func (c *committer) commit(g *commitGroup, syncfs func(*os.File) error) {
	t0 := time.Now()
	c.commits.Add(1)
	c.files.Add(int64(len(g.entries)))
	defer func() {
		for _, e := range g.entries {
			if e.own {
				e.f.Close()
			}
		}
		if f := c.observe.Load(); f != nil {
			(*f)("bstore_publish_commit_seconds", time.Since(t0).Seconds())
		}
	}()
	fail := func(err error) {
		for _, e := range g.entries {
			if e.err == nil {
				e.err = err
			}
		}
	}
	// The data.
	renames := false
	var data []*commitEntry
	for _, e := range g.entries {
		if e.tmp != "" {
			renames = true
		}
		// With syncfs any open file names its filesystem, so a directory
		// asked for alone is covered by this sync.
		if e.tmp != "" || e.dir == "" || syncfs != nil {
			data = append(data, e)
		}
	}
	if err := c.syncAll(data, false, syncfs); err != nil {
		for _, e := range g.entries {
			if e.tmp != "" {
				os.Remove(e.tmp)
			}
		}
		fail(err)
		return
	}
	// The renames, then their durability (with syncfs, the first sync
	// covered the directories asked for alone when nothing was renamed).
	var dirs []*commitEntry
	for _, e := range g.entries {
		if e.tmp != "" {
			if err := os.Rename(e.tmp, e.path); err != nil {
				os.Remove(e.tmp)
				e.err = err
				continue
			}
		}
		if e.dir != "" && e.err == nil {
			dirs = append(dirs, e)
		}
	}
	if syncfs != nil && !renames {
		return
	}
	if err := c.syncAll(dirs, true, syncfs); err != nil {
		fail(err)
	}
}

// syncAll syncs what es name: with syncfs, once per filesystem; else, at
// once, each file (dirs false) or each distinct directory (dirs true).
func (c *committer) syncAll(es []*commitEntry, dirs bool, syncfs func(*os.File) error) error {
	if len(es) == 0 {
		return nil
	}
	if syncfs != nil {
		seen := map[uint64]bool{}
		for _, e := range es {
			dev, err := c.devOf(e.f)
			if err != nil {
				return err
			}
			if seen[dev] {
				continue
			}
			seen[dev] = true
			c.syncs.Add(1)
			if err := syncfs(e.f); err != nil {
				return err
			}
		}
		return nil
	}
	var todo []func() error
	if dirs {
		seen := map[string]bool{}
		for _, e := range es {
			if seen[e.dir] {
				continue
			}
			seen[e.dir] = true
			if e.tmp == "" {
				// A directory asked for alone: its own open file.
				f := e.f
				todo = append(todo, func() error { return dirSynced(c.fsync(f)) })
				continue
			}
			dir := e.dir
			todo = append(todo, func() error {
				d, err := os.Open(dir)
				if err != nil {
					return err
				}
				defer d.Close()
				return dirSynced(c.fsync(d))
			})
		}
	} else {
		for _, e := range es {
			f := e.f
			todo = append(todo, func() error { return c.fsync(f) })
		}
	}
	c.syncs.Add(int64(len(todo)))
	return parallel(todo)
}

// dirSynced is a directory fsync's error, but none from a filesystem that
// cannot fsync a directory (EINVAL, ENOTSUP), whose renames are as durable
// as it makes them.
func dirSynced(err error) error {
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		return nil
	}
	return err
}

// commitParallel bounds the fsyncs one commit issues at once.
const commitParallel = 64

// parallel runs fs at once, at most commitParallel at a time, and returns
// the first error.
func parallel(fs []func() error) error {
	if len(fs) == 1 {
		return fs[0]()
	}
	var mu sync.Mutex
	var first error
	var wg sync.WaitGroup
	sem := make(chan struct{}, commitParallel)
	for _, f := range fs {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if err := f(); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return first
}
