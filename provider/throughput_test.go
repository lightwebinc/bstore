package provider

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/testwriter"
)

// TestRootThroughput measures the roots verified and decided a second
// through the worker, with the provider's data on the directory
// BSTORE_BENCH_DIR names (a disk, not a tmpfs: the cost is the reads and
// syncs). BSTORE_BENCH_ROOTS 1 KiB puts (default 200) arrive while every
// verification is held, then all are let go at once and timed until every
// entry is held. BSTORE_BENCH_WORKERS sets limits.verify_workers.
func TestRootThroughput(t *testing.T) {
	dir := os.Getenv("BSTORE_BENCH_DIR")
	if dir == "" {
		t.Skip("BSTORE_BENCH_DIR names a disk-backed directory for the measurement")
	}
	n := 200
	if s := os.Getenv("BSTORE_BENCH_ROOTS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	data, err := os.MkdirTemp(dir, "bstore-bench-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(data) })
	r := newRig(t)
	cfg := r.config(data)
	if s := os.Getenv("BSTORE_BENCH_WORKERS"); s != "" {
		cfg.Limits.VerifyWorkers, _ = strconv.Atoi(s)
	}
	p := r.start(cfg, true)
	// Every verification waits for its slot until all the puts are in.
	slots := cap(p.limits.verifyMu)
	for range slots {
		p.limits.verifyMu <- struct{}{}
	}
	ctx := context.Background()
	// The test chain mines a block for each put's transactions: the
	// deadline is far enough ahead to outlast them.
	terms := bounty
	terms.DeadlineAhead = 100000
	puts := make([]*testwriter.Put, n)
	for i := range puts {
		b := content(1024, byte(i))
		binary.BigEndian.PutUint32(b, uint32(i))
		puts[i] = r.put(ctx, []*Provider{p}, b, fmt.Sprintf("bench/%d", i), terms)
	}
	start := time.Now()
	for range slots {
		<-p.limits.verifyMu
	}
	r.eventually("all held", 10*time.Minute, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, put := range puts {
			if e := p.st.Entries[put.C]; e == nil || e.Decision != decisionHeld {
				return false
			}
		}
		return true
	})
	took := time.Since(start)
	t.Logf("%d roots held in %v: %.1f roots/s (verification slots %d)", n, took.Round(time.Millisecond), float64(n)/took.Seconds(), slots)
}
