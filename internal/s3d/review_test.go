package s3d

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// A declared decoded length bounds an aws-chunked body, zero included.
func TestChunkedDecodedLengthBounds(t *testing.T) {
	a := &Auth{Seed: strings.Repeat("0", 64)}
	body := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", 5, "hello")
	for _, max := range []int64{0, 4} {
		c := newChunkReader(strings.NewReader(body), a, false, false)
		c.max = max
		if _, err := io.ReadAll(c); !errors.Is(err, errChunk) {
			t.Fatalf("a body over a declared length of %d: %v", max, err)
		}
	}
	c := newChunkReader(strings.NewReader(body), a, false, false)
	c.max = 5
	if got, err := io.ReadAll(c); err != nil || string(got) != "hello" {
		t.Fatalf("a body of its declared length: %v", err)
	}
}

// One key fills only its own share of the replay cache.
func TestReplayQuotaPerKey(t *testing.T) {
	now := time.Now()
	c := newReplayCache(func() time.Time { return now }, 3)
	for i := 0; i < 3; i++ {
		if e, _, _ := c.begin(fmt.Sprint("a", i), "a", now.Add(time.Hour)); e == nil {
			t.Fatal("refused under the quota")
		}
	}
	if e, _, _ := c.begin("a9", "a", now.Add(time.Hour)); e != nil {
		t.Fatal("a key went past its quota")
	}
	if e, _, _ := c.begin("b0", "b", now.Add(time.Hour)); e == nil {
		t.Fatal("one key's full share stopped another key")
	}
	now = now.Add(2 * time.Hour)
	if e, _, _ := c.begin("a9", "a", now.Add(time.Hour)); e == nil {
		t.Fatal("expired signatures still count")
	}
}

func TestPublicAddr(t *testing.T) {
	for _, s := range []string{"127.0.0.1", "10.1.2.3", "192.168.0.1", "169.254.1.1", "100.64.0.1", "0.0.0.1", "198.18.0.1",
		"::1", "fe80::1", "fc00::1", "64:ff9b::a00:1", "2002:a00:1::1", "::ffff:127.0.0.1", "192.0.2.1", "2001:db8::1"} {
		if publicAddr(net.ParseIP(s)) {
			t.Errorf("%s counted public", s)
		}
	}
	for _, s := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !publicAddr(net.ParseIP(s)) {
			t.Errorf("%s counted not public", s)
		}
	}
}

// A token that names a pending entry's commitment drops it, and the
// log replays to the same state after a restart.
func TestPendingNamedAndReplayed(t *testing.T) {
	dir := t.TempDir()
	now := func() time.Time { return time.Now() }
	s, err := openState(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	e1 := &storerec.Entry{Bucket: "b", Key: "k1", Kind: storerec.KindPut, Created: 1}
	e2 := &storerec.Entry{Bucket: "b", Key: "k2", Kind: storerec.KindPut, Created: 2}
	c1, c2 := [32]byte{1}, [32]byte{2}
	if err := s.add(e1, []byte{1}, c1); err != nil {
		t.Fatal(err)
	}
	if err := s.add(e2, []byte{2}, c2); err != nil {
		t.Fatal(err)
	}
	s.named(&storerec.Bucket{Consumed: [][32]byte{c1}})
	if s.newest("b", "k1") != nil || s.newest("b", "k2") == nil {
		t.Fatal("named did not drop exactly the named entry")
	}
	s2, err := openState(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	if s2.newest("b", "k1") != nil || s2.newest("b", "k2") == nil || !bytes.Equal(s2.newest("b", "k2").Record, []byte{2}) {
		t.Fatal("the log did not replay")
	}
}
