package module

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// The rows journal's group commit (BSTORE_JOURNAL_SYNC_MS): lines in the
// file at once and in order, fsynced together off the caller's path, a
// process crash losing nothing, Rewrite and Close flushing, a failed fsync
// tried again. The bytes are TestJournalBytes's.

func oLine(i int) Line {
	return Line{K: "o", B: bucket, Txid: fmt.Sprintf("%064x", i), Vout: int64(i % 7), At: int64(1_798_761_600_000 + i)}
}

// countingSyncs makes j count its fsyncs, each still a real one.
func countingSyncs(j *FileJournal) *atomic.Int64 {
	var n atomic.Int64
	j.fsync = func(f *os.File) error {
		n.Add(1)
		return f.Sync()
	}
	return &n
}

func loadAll(t *testing.T, dir string) []Line {
	t.Helper()
	lines, err := (&FileJournal{Dir: dir, Path: filepath.Join(dir, JournalFile)}).Load()
	if err != nil {
		t.Fatal(err)
	}
	return lines
}

func TestJournalEveryLine(t *testing.T) {
	dir := t.TempDir()
	j, _ := NewFileJournal(dir)
	syncs := countingSyncs(j)
	for i := range 5 {
		if err := j.Append(oLine(i)); err != nil {
			t.Fatal(err)
		}
		if got := syncs.Load(); got != int64(i+1) {
			t.Fatalf("line %d: %d fsyncs, want one a line before Append returns", i, got)
		}
	}
	if j.armed || j.pending != 0 {
		t.Fatal("a window armed with SyncEvery zero")
	}
	_ = j.Close()
	if got := loadAll(t, dir); len(got) != 5 {
		t.Fatalf("%d lines", len(got))
	}
}

func TestJournalGroupCommitOrder(t *testing.T) {
	dir := t.TempDir()
	j, _ := NewFileJournal(dir)
	j.SyncEvery = 2 * time.Millisecond
	syncs := countingSyncs(j)
	var covered atomic.Int64
	j.OnSync = func(lines int, _ time.Duration, err error) {
		if err != nil {
			t.Error(err)
		}
		covered.Add(int64(lines))
	}
	const n = 2000
	for i := range n {
		if err := j.Append(oLine(i)); err != nil {
			t.Fatal(err)
		}
		if i%250 == 0 {
			time.Sleep(3 * time.Millisecond)
		}
	}
	// Every line is in the file, in order, before any fsync is asked for.
	got := loadAll(t, dir)
	if len(got) != n {
		t.Fatalf("%d lines in the file, want %d", len(got), n)
	}
	for i, l := range got {
		if l != oLine(i) {
			t.Fatalf("line %d is %+v", i, l)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for covered.Load() < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if covered.Load() != n {
		t.Fatalf("timed fsyncs covered %d of %d lines", covered.Load(), n)
	}
	if s := syncs.Load(); s < 1 || s >= n/4 {
		t.Fatalf("%d fsyncs for %d lines: not grouped", s, n)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if got := loadAll(t, dir); len(got) != n {
		t.Fatalf("%d lines after Close", len(got))
	}
}

// The append path never waits for a timed fsync: Append returns while one
// is still running.
func TestJournalAppendDoesNotWaitForSync(t *testing.T) {
	dir := t.TempDir()
	j, _ := NewFileJournal(dir)
	j.SyncEvery = time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	j.fsync = func(f *os.File) error {
		once.Do(func() { close(entered) })
		<-release
		return f.Sync()
	}
	if err := j.Append(oLine(0)); err != nil {
		t.Fatal(err)
	}
	<-entered
	done := make(chan error, 1)
	go func() { done <- j.Append(oLine(1)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Append waited for the fsync in flight")
	}
	close(release)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if got := loadAll(t, dir); len(got) != 2 || got[1] != oLine(1) {
		t.Fatalf("%+v", got)
	}
}

// A process that dies with lines still waiting for their fsync loses none
// of them: they were written to the file before Append returned. The child
// is this test binary, killed by SIGKILL mid-window.
func TestJournalCrashKeepsWrittenLines(t *testing.T) {
	if dir := os.Getenv("BSTORE_JOURNAL_CRASH_DIR"); dir != "" {
		j, _ := NewFileJournal(dir)
		j.SyncEvery = time.Hour
		j.fsync = func(*os.File) error { return errors.New("no fsync in the crash child") }
		for i := range 300 {
			if err := j.Append(oLine(i)); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}
		}
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		time.Sleep(time.Minute)
		os.Exit(4)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestJournalCrashKeepsWrittenLines$", "-test.count=1")
	cmd.Env = append(os.Environ(), "BSTORE_JOURNAL_CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("the child was not killed: %v %s", err, out)
	}
	if ws, ok := ee.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("the child ended %v: %s", err, out)
	}
	got := loadAll(t, dir)
	if len(got) != 300 {
		t.Fatalf("%d lines survived the crash, want 300", len(got))
	}
	for i, l := range got {
		if l != oLine(i) {
			t.Fatalf("line %d is %+v", i, l)
		}
	}
}

func TestJournalRewriteWithSyncPending(t *testing.T) {
	dir := t.TempDir()
	j, _ := NewFileJournal(dir)
	j.SyncEvery = time.Hour
	syncs := countingSyncs(j)
	var timed atomic.Int64
	j.OnSync = func(int, time.Duration, error) { timed.Add(1) }
	for i := range 10 {
		if err := j.Append(oLine(i)); err != nil {
			t.Fatal(err)
		}
	}
	if !j.armed || j.pending != 10 || syncs.Load() != 0 {
		t.Fatalf("armed %v pending %d fsyncs %d", j.armed, j.pending, syncs.Load())
	}
	state := []Line{oLine(100), oLine(101)}
	if err := j.Rewrite(state); err != nil {
		t.Fatal(err)
	}
	// One fsync, the new file's; the old file's pending lines are superseded.
	if syncs.Load() != 1 || j.pending != 0 {
		t.Fatalf("fsyncs %d pending %d after Rewrite", syncs.Load(), j.pending)
	}
	if err := j.Append(oLine(102)); err != nil {
		t.Fatal(err)
	}
	if got := loadAll(t, dir); len(got) != 3 || got[0] != oLine(100) || got[2] != oLine(102) {
		t.Fatalf("%+v", got)
	}
	// The window armed before the rewrite still covers the line after it.
	j.timedSync()
	if syncs.Load() != 2 || j.pending != 0 || timed.Load() != 1 {
		t.Fatalf("fsyncs %d pending %d timed %d", syncs.Load(), j.pending, timed.Load())
	}
	// A timed fsync that fires with nothing pending does nothing.
	j.timedSync()
	if syncs.Load() != 2 || timed.Load() != 1 {
		t.Fatal("an empty window fsynced")
	}
	_ = j.Close()
}

func TestJournalCloseFlushes(t *testing.T) {
	dir := t.TempDir()
	j, _ := NewFileJournal(dir)
	j.SyncEvery = time.Hour
	syncs := countingSyncs(j)
	var timed atomic.Int64
	j.OnSync = func(int, time.Duration, error) { timed.Add(1) }
	for i := range 4 {
		if err := j.Append(oLine(i)); err != nil {
			t.Fatal(err)
		}
	}
	if syncs.Load() != 0 {
		t.Fatal("an fsync before the window")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() != 1 || j.pending != 0 || j.armed || j.timer != nil || j.f != nil {
		t.Fatalf("fsyncs %d pending %d armed %v", syncs.Load(), j.pending, j.armed)
	}
	// Nothing pending: a second Close does not fsync; the stopped window
	// never fires.
	if err := j.Close(); err != nil || syncs.Load() != 1 {
		t.Fatal(err, syncs.Load())
	}
	j.timedSync()
	if timed.Load() != 0 {
		t.Fatal("a timed fsync after Close")
	}
	// Append after Close opens the file again.
	if err := j.Append(oLine(4)); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil || syncs.Load() != 2 {
		t.Fatal(err, syncs.Load())
	}
	if got := loadAll(t, dir); len(got) != 5 {
		t.Fatalf("%d lines", len(got))
	}
}

func TestJournalFailedSyncTriedAgain(t *testing.T) {
	dir := t.TempDir()
	j, _ := NewFileJournal(dir)
	j.SyncEvery = time.Hour
	boom := errors.New("disk full")
	var fail atomic.Bool
	fail.Store(true)
	j.fsync = func(f *os.File) error {
		if fail.Load() {
			return boom
		}
		return f.Sync()
	}
	var errs, oks atomic.Int64
	j.OnSync = func(_ int, _ time.Duration, err error) {
		if err != nil {
			errs.Add(1)
		} else {
			oks.Add(1)
		}
	}
	_ = j.Append(oLine(0))
	_ = j.Append(oLine(1))
	j.timedSync()
	if errs.Load() != 1 || j.pending != 2 || !j.armed {
		t.Fatalf("errors %d pending %d armed %v: the lines are not left for the next window", errs.Load(), j.pending, j.armed)
	}
	// The next Append writes its line and reports the failure, once.
	if err := j.Append(oLine(2)); err == nil || !errors.Is(err, boom) {
		t.Fatalf("%v", err)
	}
	if err := j.Append(oLine(3)); err != nil {
		t.Fatalf("reported twice: %v", err)
	}
	fail.Store(false)
	j.timedSync()
	if oks.Load() != 1 || j.pending != 0 {
		t.Fatalf("oks %d pending %d", oks.Load(), j.pending)
	}
	_ = j.Close()
	if got := loadAll(t, dir); len(got) != 4 {
		t.Fatalf("%d lines", len(got))
	}
}

// Appends from several goroutines, a rewrite now and then and a 1 ms
// window, for the race detector: no torn or lost line, every goroutine's
// lines in its order.
func TestJournalConcurrent(t *testing.T) {
	dir := t.TempDir()
	j, _ := NewFileJournal(dir)
	j.SyncEvery = time.Millisecond
	j.OnSync = func(_ int, _ time.Duration, err error) {
		if err != nil {
			t.Error(err)
		}
	}
	const writers, each = 4, 400
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var synced atomic.Int64
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if err := j.Sync(); err != nil {
				t.Error(err)
			}
			synced.Add(1)
		}
	}()
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				if err := j.Append(oLine(w*100_000 + i)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	got := loadAll(t, dir)
	if len(got) != writers*each {
		t.Fatalf("%d lines, want %d", len(got), writers*each)
	}
	next := make([]int, writers)
	for _, l := range got {
		var n int
		if _, err := fmt.Sscanf(l.Txid, "%x", &n); err != nil {
			t.Fatal(err)
		}
		w, i := n/100_000, n%100_000
		if i != next[w] {
			t.Fatalf("writer %d: line %d after %d", w, i, next[w]-1)
		}
		next[w]++
	}
	// A rewrite under the same load keeps the journal whole.
	k, _ := NewFileJournal(dir)
	k.SyncEvery = time.Millisecond
	var wg2 sync.WaitGroup
	for w := range 2 {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for i := range 200 {
				_ = k.Append(oLine(900_000 + w*1000 + i))
				if i%50 == 0 {
					_ = k.Rewrite([]Line{oLine(1)})
				}
			}
		}()
	}
	wg2.Wait()
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Load(); err != nil {
		t.Fatal(err)
	}
}

func TestParseJournalSync(t *testing.T) {
	base := map[string]string{"BSTORE_BUCKETS": b1, "BSTORE_STATE_DIR": "/var/lib/bstore", "BSTORE_HEADERS_URL": "https://headers.example"}
	with := func(v string) map[string]string {
		m := map[string]string{"BSTORE_JOURNAL_SYNC_MS": v}
		for k, x := range base {
			m[k] = x
		}
		return m
	}
	if c, err := ParseConfig(env(base)); err != nil || c.JournalSync != 100*time.Millisecond {
		t.Fatal(c, err)
	}
	for v, want := range map[string]time.Duration{"0": 0, "5": 5 * time.Millisecond, "60000": time.Minute} {
		if c, err := ParseConfig(env(with(v))); err != nil || c.JournalSync != want {
			t.Fatal(v, c, err)
		}
	}
	for _, bad := range []string{"60001", "-1", "1.5", "x", "100ms"} {
		if _, err := ParseConfig(env(with(bad))); err == nil || !strings.Contains(err.Error(), "BSTORE_JOURNAL_SYNC_MS") {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

// The factory gives the state directory's journal the configured window,
// presets and keeps the journal counters, logs a run of failed fsyncs once,
// and Start closes the journal when its context ends.
func TestFactoryJournal(t *testing.T) {
	dir := t.TempDir()
	e := map[string]string{"BSTORE_BUCKETS": b1, "BSTORE_STATE_DIR": dir, "BSTORE_HEADERS_URL": "http://127.0.0.1:1",
		"OVERLAY_TOPICS": "tm_bstore_" + b1, "BSTORE_JOURNAL_SYNC_MS": "7"}
	host := newFakeHost()
	m, err := New(host, env(e))
	if err != nil {
		t.Fatal(err)
	}
	fj, ok := m.journal.(*FileJournal)
	if !ok || fj.SyncEvery != 7*time.Millisecond || fj.OnSync == nil {
		t.Fatalf("%T %+v", m.journal, fj)
	}
	for _, c := range []string{"bstore_journal_lines_total", "bstore_journal_wait_seconds_total", "bstore_journal_sync_seconds_total",
		`bstore_journal_syncs_total{result="ok"}`, `bstore_journal_syncs_total{result="error"}`} {
		if _, ok := host.counters[c]; !ok {
			t.Errorf("%s is not preset", c)
		}
	}
	if !host.logged("bstore module mounted", func(x map[string]any) bool { return x["journal_sync"] == "every 7ms" }) {
		t.Fatal("the window is not logged")
	}
	m.LS.mu.Lock()
	m.LS.write(oLine(1))
	m.LS.mu.Unlock()
	if host.count("bstore_journal_lines_total", nil) != 1 || host.count("bstore_journal_wait_seconds_total", nil) <= 0 {
		t.Fatal("the write is not counted")
	}
	deadline := time.Now().Add(2 * time.Second)
	for host.count("bstore_journal_syncs_total", map[string]string{"result": "ok"}) < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if host.count("bstore_journal_syncs_total", map[string]string{"result": "ok"}) != 1 {
		t.Fatal("the timed fsync is not counted")
	}
	// Two failures in a row log once; the recovery logs once.
	on := fj.OnSync
	on(3, time.Millisecond, errors.New("eio"))
	on(3, time.Millisecond, errors.New("eio"))
	on(4, time.Millisecond, nil)
	n := 0
	host.mu.Lock()
	for _, l := range host.lines {
		if strings.HasPrefix(l.msg, "ls_bstore could not sync the journal") || l.msg == "ls_bstore journal synced again" {
			n++
		}
	}
	host.mu.Unlock()
	if n != 2 || host.count("bstore_journal_syncs_total", map[string]string{"result": "error"}) != 2 {
		t.Fatalf("%d log lines, %v errors", n, host.count("bstore_journal_syncs_total", map[string]string{"result": "error"}))
	}
	// Start returns when its context ends, with the journal closed.
	m.LS.mu.Lock()
	m.LS.write(oLine(2))
	m.LS.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	fj.mu.Lock()
	open, pending := fj.f != nil, fj.pending
	fj.mu.Unlock()
	if open || pending != 0 {
		t.Fatalf("after Start: open %v pending %d", open, pending)
	}
	if got := loadAll(t, dir); len(got) != 2 {
		t.Fatalf("%d lines", len(got))
	}
}
