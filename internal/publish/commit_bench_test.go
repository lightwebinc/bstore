package publish

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// BenchmarkSmallPutDisk is what one small put writes to the gateway's disk,
// done by many puts at once: two store objects and the manifest, a batch
// file created (its directory synced) and a line synced in it, the state,
// the race, the entry log (.beef, .json) and the outbox item (.beef,
// .json), each synced in the order the put syncs it. Modes: direct (every
// write syncs alone, as before the committer, and as auto does on all but
// XFS), fsync (group commit by fsyncs, as group does on FreeBSD), and
// syncfs (group commit by syncfs, as group does on Linux).
//
// It reports puts/s, syncs/put (the committer's sync calls) and, on Linux,
// flushes/put (the device's flush requests, /sys/block/<disk>/stat). Run it
// with TMPDIR on a real disk (not tmpfs):
//
//	TMPDIR=/path/on/disk go test -run '^$' -bench SmallPutDisk -benchtime 2000x ./internal/publish/
//
// BSTORE_BENCH_PUTS sets the puts in flight (default 64).
func BenchmarkSmallPutDisk(b *testing.B) {
	inflight := 64
	if v, err := strconv.Atoi(os.Getenv("BSTORE_BENCH_PUTS")); err == nil && v > 0 {
		inflight = v
	}
	modes := []string{"direct", "fsync"}
	if commits.syncfs != nil {
		modes = append(modes, "syncfs")
	}
	for _, mode := range modes {
		b.Run(fmt.Sprintf("%s/inflight=%d", mode, inflight), func(b *testing.B) {
			c := &committer{fsync: (*os.File).Sync, mode: "group"}
			switch mode {
			case "direct":
				c.mode = "direct"
			case "syncfs":
				c.syncfs, c.devOf, _ = platformCommit()
			}
			dir := b.TempDir()
			tmp := filepath.Join(dir, "tmp")
			for _, d := range []string{"tmp", "objects", "roots", "outbox", "entries", "races"} {
				if err := os.Mkdir(filepath.Join(dir, d), 0o700); err != nil {
					b.Fatal(err)
				}
			}
			small, beef := make([]byte, 1024), make([]byte, 2048)
			state := make([]byte, 16<<10)
			put := func(i int) error {
				id := strconv.Itoa(i)
				for _, o := range []string{"blob", "root"} {
					if err := c.write(tmp, filepath.Join(dir, "objects", id+o), small); err != nil {
						return err
					}
				}
				if err := c.write(tmp, filepath.Join(dir, "roots", id+".json"), small[:300]); err != nil {
					return err
				}
				bp := filepath.Join(dir, "outbox", id+".batch")
				f, err := os.OpenFile(bp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
				if err != nil {
					return err
				}
				defer os.Remove(bp)
				defer f.Close()
				if err := c.syncDir(filepath.Join(dir, "outbox")); err != nil {
					return err
				}
				if _, err := f.Write(small[:400]); err != nil {
					return err
				}
				if err := c.syncFile(f); err != nil {
					return err
				}
				steps := []struct {
					path string
					b    []byte
				}{
					{filepath.Join(dir, "state-"+id+".json"), state},
					{filepath.Join(dir, "races", id+".json"), small},
					{filepath.Join(dir, "entries", id+".beef"), beef},
					{filepath.Join(dir, "entries", id+".json"), small[:500]},
					{filepath.Join(dir, "outbox", id+".beef"), beef},
					{filepath.Join(dir, "outbox", id+".json"), small[:300]},
				}
				for _, s := range steps {
					if err := c.write(tmp, s.path, s.b); err != nil {
						return err
					}
				}
				return nil
			}
			flush0 := deviceFlushes(dir)
			var next atomic.Int64
			var wg sync.WaitGroup
			errs := make(chan error, inflight)
			b.ResetTimer()
			t0 := time.Now()
			for range inflight {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(next.Add(1))
						if i > b.N {
							return
						}
						if err := put(i); err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			wg.Wait()
			el := time.Since(t0)
			b.StopTimer()
			close(errs)
			for err := range errs {
				b.Fatal(err)
			}
			b.ReportMetric(float64(b.N)/el.Seconds(), "puts/s")
			b.ReportMetric(float64(c.syncs.Load())/float64(b.N), "syncs/put")
			if mode != "direct" {
				b.ReportMetric(float64(c.files.Load())/float64(c.commits.Load()), "files/commit")
			}
			if flush0 >= 0 {
				b.ReportMetric(float64(deviceFlushes(dir)-flush0)/float64(b.N), "flushes/put")
			}
		})
	}
}

// deviceFlushes is the flush requests the disk holding dir has completed
// (the 16th field of its stat, Linux 5.5 on), or -1 when it cannot be read.
func deviceFlushes(dir string) int64 {
	info, err := os.Stat(dir)
	if err != nil {
		return -1
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return -1
	}
	dev := uint64(st.Dev)
	major, minor := (dev>>8)&0xfff, (dev&0xff)|((dev>>12)&0xfff00)
	sys, err := filepath.EvalSymlinks(fmt.Sprintf("/sys/dev/block/%d:%d", major, minor))
	if err != nil {
		return -1
	}
	if _, err := os.Stat(filepath.Join(sys, "partition")); err == nil {
		sys = filepath.Join(sys, "..")
	}
	return statFlushes(sys)
}

// statFlushes reads a block device's flushes from sys, or its members'
// (an md array counts none of its own).
func statFlushes(sys string) int64 {
	if slaves, _ := os.ReadDir(filepath.Join(sys, "slaves")); len(slaves) > 0 {
		var sum int64
		for _, s := range slaves {
			n := statFlushes(filepath.Join(sys, "slaves", s.Name()))
			if n < 0 {
				return -1
			}
			sum += n
		}
		return sum
	}
	raw, err := os.ReadFile(filepath.Join(sys, "stat"))
	if err != nil {
		return -1
	}
	f := strings.Fields(string(raw))
	if len(f) < 17 {
		return -1
	}
	n, err := strconv.ParseInt(f[15], 10, 64)
	if err != nil {
		return -1
	}
	return n
}
