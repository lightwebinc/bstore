package s3d

import (
	"slices"
	"strings"
	"sync"
	"testing"
)

// A bucket added at run time is served from then on, in order, held to the
// configuration's rules; the same chain again is no change, another chain
// under its S3 name is refused; and requests may read while it is added.
func TestAddBucket(t *testing.T) {
	c := minimalConfig()
	if err := c.Check(); err != nil {
		t.Fatal(err)
	}
	first := c.Buckets[0]
	writer := &Server{cfg: c, buckets: map[string]*bucket{}}
	if _, err := writer.AddBucket(first); err == nil || !strings.Contains(err.Error(), "read_only") {
		t.Fatalf("a writing gateway took a bucket: %v", err)
	}
	c.Providers, c.ReadOnlyGateway = nil, true
	if err := c.Check(); err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: c, buckets: map[string]*bucket{}}
	n, err := s.AddBucket(first)
	if err != nil || n != "photos-qzvmkwerty" || s.bucket(n) == nil {
		t.Fatalf("%q %v", n, err)
	}
	other := BucketConfig{Bucket: "album_abcdefghij", Owner: first.Owner, ID: strings.Repeat("cd", 32)}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = s.bucket("album-abcdefghij")
					_ = s.BucketNames()
				}
			}
		}()
	}
	if n, err = s.AddBucket(other); err != nil || n != "album-abcdefghij" {
		t.Fatalf("%q %v", n, err)
	}
	close(stop)
	wg.Wait()
	if got := s.BucketNames(); !slices.Equal(got, []string{"album-abcdefghij", "photos-qzvmkwerty"}) {
		t.Fatalf("names %v", got)
	}
	if _, err := s.AddBucket(first); err != nil {
		t.Fatalf("the same chain again: %v", err)
	}
	clash := first
	clash.ID = strings.Repeat("ef", 32)
	if _, err := s.AddBucket(clash); err == nil || !strings.Contains(err.Error(), "another chain") {
		t.Fatalf("another chain under the name: %v", err)
	}
	bad := BucketConfig{Bucket: "Not_A_Bucket", Owner: first.Owner, ID: first.ID}
	if _, err := s.AddBucket(bad); err == nil {
		t.Fatal("a bad bucket identifier was added")
	}
	if len(s.BucketNames()) != 2 {
		t.Fatal("a refused bucket was added")
	}
}
