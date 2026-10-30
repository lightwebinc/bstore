package s3d

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/storerec"
)

// A write is named by what retryOf compares, its metadata in any order;
// any other field makes it another write.
func TestWriteKey(t *testing.T) {
	meta := []storerec.MetaPair{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}
	swapped := []storerec.MetaPair{meta[1], meta[0]}
	k := writeKey("bk", "pw", "k", "text/plain", meta, "abc", [32]byte{9}, 10)
	if writeKey("bk", "pw", "k", "text/plain", swapped, "abc", [32]byte{9}, 10) != k {
		t.Fatal("metadata order changed the key")
	}
	for name, other := range map[string][32]byte{
		"bucket":  writeKey("bj", "pw", "k", "text/plain", meta, "abc", [32]byte{9}, 10),
		"writer":  writeKey("bk", "px", "k", "text/plain", meta, "abc", [32]byte{9}, 10),
		"key":     writeKey("bk", "pw", "j", "text/plain", meta, "abc", [32]byte{9}, 10),
		"type":    writeKey("bk", "pw", "k", "text/html", meta, "abc", [32]byte{9}, 10),
		"meta":    writeKey("bk", "pw", "k", "text/plain", meta[:1], "abc", [32]byte{9}, 10),
		"etag":    writeKey("bk", "pw", "k", "text/plain", meta, "abd", [32]byte{9}, 10),
		"content": writeKey("bk", "pw", "k", "text/plain", meta, "abc", [32]byte{8}, 10),
		"size":    writeKey("bk", "pw", "k", "text/plain", meta, "abc", [32]byte{9}, 11),
		// Length-prefixed fields: moving bytes between two is another write.
		"split": writeKey("bkp", "w", "k", "text/plain", meta, "abc", [32]byte{9}, 10),
	} {
		if other == k {
			t.Errorf("another %s, same key", name)
		}
	}
}

func retries(m *metrics) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c := m.c["bstore_s3d_put_retries_total"]; c != nil {
		return c.Load()
	}
	return 0
}

// The same write sent again while the first still publishes (an S3 client
// that timed out and resent) waits for that publish and answers with its
// entry: one write, however many resends. A resend whose request ends stops
// waiting; a later one, or another write, publishes on its own.
func TestOneWriteSharesAnInFlightWrite(t *testing.T) {
	s := &Server{metrics: newMetrics()}
	k := writeKey("bk", "pw", "k", "text/plain", nil, "abc", [32]byte{9}, 10)
	first := &storerec.Entry{Root: [32]byte{9}}
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	type answer struct {
		entry *storerec.Entry
		err   *s3Error
	}
	answers := make(chan answer, 3)
	go func() {
		e, err := s.oneWrite(context.Background(), k, func() (*storerec.Entry, *s3Error) {
			calls.Add(1)
			close(started)
			<-release
			return first, nil
		})
		answers <- answer{e, err}
	}()
	<-started

	again := func() (*storerec.Entry, *s3Error) {
		calls.Add(1)
		return &storerec.Entry{Root: [32]byte{7}}, nil
	}
	for range 2 {
		go func() {
			e, err := s.oneWrite(context.Background(), k, again)
			answers <- answer{e, err}
		}()
	}
	gone, cancel := context.WithCancel(context.Background())
	stopped := make(chan *s3Error, 1)
	go func() {
		_, err := s.oneWrite(gone, k, again)
		stopped <- err
	}()
	for deadline := time.Now().Add(5 * time.Second); retries(s.metrics) < 3; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d resends joined the write, want 3", retries(s.metrics))
		}
	}
	cancel()
	if err := <-stopped; err != errSlowDown {
		t.Fatalf("a resend whose request ended: %v, want SlowDown", err)
	}

	close(release)
	for range 3 {
		if a := <-answers; a.err != nil || a.entry != first {
			t.Fatalf("answer %+v, want the first write's entry", a)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d writes published, want 1", n)
	}

	if e, err := s.oneWrite(context.Background(), k, again); err != nil || e.Root != [32]byte{7} || calls.Load() != 2 {
		t.Fatalf("a write after the flight ended was not published on its own (%v, %d writes)", err, calls.Load())
	}
	other := writeKey("bk", "pw", "j", "text/plain", nil, "abc", [32]byte{9}, 10)
	if _, err := s.oneWrite(context.Background(), other, again); err != nil || calls.Load() != 3 {
		t.Fatalf("another write was not published (%v, %d writes)", err, calls.Load())
	}
}

// A leader that panics still releases its waiters, with SlowDown.
func TestOneWriteReleasesWaitersOnPanic(t *testing.T) {
	s := &Server{metrics: newMetrics()}
	k := writeKey("bk", "pw", "k", "", nil, "", [32]byte{1}, 1)
	started, release := make(chan struct{}), make(chan struct{})
	go func() {
		defer func() { _ = recover() }()
		_, _ = s.oneWrite(context.Background(), k, func() (*storerec.Entry, *s3Error) {
			close(started)
			<-release
			panic("write failed")
		})
	}()
	<-started
	waited := make(chan *s3Error, 1)
	go func() {
		_, err := s.oneWrite(context.Background(), k, func() (*storerec.Entry, *s3Error) {
			t.Error("the waiter published")
			return nil, nil
		})
		waited <- err
	}()
	for retries(s.metrics) < 1 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-waited; err != errSlowDown {
		t.Fatalf("waiter after a panic: %v, want SlowDown", err)
	}
}
