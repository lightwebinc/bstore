package s3d

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

func embedEntry() *storerec.Entry {
	return &storerec.Entry{Size: 1000, Created: 1_800_000_000_000, ContentType: "text/html", Root: [32]byte{7}}
}

// A read in process answers the statuses and headers the S3 face would,
// with no body for a head or a status that has none.
func TestReadEntryStatusesAndHeaders(t *testing.T) {
	s := &Server{reads: make(chan struct{}, 1), metrics: newMetrics()}
	ent := embedEntry()
	etag := etagOf(ent)
	for _, c := range []struct {
		name   string
		cond   http.Header
		status int
		want   map[string]string
	}{
		{"head", nil, 200, map[string]string{"Content-Length": "1000", "Content-Type": "text/html", "Etag": etag, "Accept-Ranges": "bytes"}},
		{"range", http.Header{"Range": {"bytes=10-19"}}, 206, map[string]string{"Content-Length": "10", "Content-Range": "bytes 10-19/1000"}},
		{"suffix", http.Header{"Range": {"bytes=-5"}}, 206, map[string]string{"Content-Length": "5", "Content-Range": "bytes 995-999/1000"}},
		{"past the end", http.Header{"Range": {"bytes=1000-"}}, 416, map[string]string{"Content-Range": "bytes */1000", "Content-Length": "", "Etag": ""}},
		{"not modified", http.Header{"If-None-Match": {etag}}, 304, map[string]string{"Etag": etag, "Content-Length": "", "Content-Type": ""}},
		{"precondition", http.Header{"If-Match": {`"other"`}}, 412, map[string]string{"Content-Length": ""}},
		{"two ranges are ignored", http.Header{"Range": {"bytes=0-1,5-6"}}, 200, map[string]string{"Content-Length": "1000"}},
	} {
		a, e := s.readEntry(context.Background(), ent, true, c.cond)
		if e != nil {
			t.Fatalf("%s: %v", c.name, e)
		}
		if a.Status != c.status {
			t.Errorf("%s: status %d, want %d", c.name, a.Status, c.status)
		}
		for k, v := range c.want {
			if got := a.Header.Get(k); got != v {
				t.Errorf("%s: %s is %q, want %q", c.name, k, got, v)
			}
		}
		if a.Root != ent.Root {
			t.Errorf("%s: the root is not the entry's", c.name)
		}
		if n, err := a.WriteTo(nil); n != 0 || err != nil {
			t.Errorf("%s: a head wrote %d bytes, %v", c.name, n, err)
		}
		if err := a.Close(); err != nil {
			t.Errorf("%s: close: %v", c.name, err)
		}
	}
	if len(s.reads) != 0 {
		t.Fatal("a head took a read slot")
	}
}

// A read with every slot taken is refused for now, and takes nothing.
func TestReadEntryShedsWhenEverySlotIsTaken(t *testing.T) {
	s := &Server{reads: make(chan struct{}, 1), metrics: newMetrics()}
	nameSlots(s)
	s.reads <- struct{}{}
	_, e := s.readEntry(context.Background(), embedEntry(), false, nil)
	if e != errSlowReads {
		t.Fatalf("got %v, want the reads bound's SlowDown", e)
	}
	if len(s.reads) != 1 {
		t.Fatal("the refused read changed the slots")
	}
}

// A bucket the configuration does not name is an error that says so, and a
// key no object can have is a 404: neither asks a host.
func TestReadObjectUnknownBucketAndBadKey(t *testing.T) {
	s := &Server{buckets: map[string]*bucket{"b": {name: "b"}}, metrics: newMetrics()}
	if _, err := s.ReadObject(context.Background(), "other", "k", false, nil); !errors.Is(err, ErrNoBucket) {
		t.Fatalf("unknown bucket: %v", err)
	}
	a, err := s.ReadObject(context.Background(), "b", "a\nb", true, nil)
	if err != nil || a.Status != http.StatusNotFound {
		t.Fatalf("bad key: %v, %v", a, err)
	}
	if a.Close() != nil {
		t.Fatal("close")
	}
}

// A reader that goes away is its own failure, not the gateway shedding
// load; a second Close, or two at once, gives the slot back once.
func TestAnswerCloseOnceAndErrorMapping(t *testing.T) {
	s := &Server{reads: make(chan struct{}, 2), metrics: newMetrics()}
	s.reads <- struct{}{}
	a := &ObjectAnswer{s: s, release: func() { <-s.reads }}
	done := make(chan struct{})
	for range 2 {
		go func() { a.Close(); done <- struct{}{} }()
	}
	<-done
	<-done
	a.Close()
	if len(s.reads) != 0 {
		t.Fatal("the slot did not come back")
	}
	if err := callError(readError(errors.New("no host")), nil); !errors.Is(err, ErrShed) {
		t.Errorf("nothing proves the key: %v, want ErrShed", err)
	}
	if err := callError(readError(reader.ErrContentHash), reader.ErrContentHash); !errors.Is(err, ErrRefused) || errors.Is(err, ErrShed) {
		t.Errorf("a content hash that fails: %v, want ErrRefused", err)
	}
	if err := callError(errNoSuchBucket, nil); errors.Is(err, ErrShed) || errors.Is(err, ErrRefused) {
		t.Errorf("another error took a sentinel: %v", err)
	}
}
