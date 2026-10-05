package s3d

import (
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func TestParseRange(t *testing.T) {
	const size = 10
	cases := []struct {
		h           string
		size        uint64
		off, n      uint64
		partial     bool
		unsatisfied bool
	}{
		{"", size, 0, 10, false, false},
		{"bytes=0-0", size, 0, 1, true, false},
		{"bytes=0-9", size, 0, 10, true, false},
		{"bytes=2-", size, 2, 8, true, false},
		{"bytes=5-100", size, 5, 5, true, false},
		{"bytes=-3", size, 7, 3, true, false},
		{"bytes=-30", size, 0, 10, true, false},
		{"bytes=9-9", size, 9, 1, true, false},
		{"bytes=10-", size, 0, 0, false, true},
		{"bytes=10-20", size, 0, 0, false, true},
		{"bytes=-0", size, 0, 0, false, true},
		{"bytes=0-", 0, 0, 0, false, true},
		{"bytes=5-2", size, 0, 10, false, false},     // last before first: ignored
		{"bytes=0-1,4-5", size, 0, 10, false, false}, // several ranges: ignored
		{"items=0-1", size, 0, 10, false, false},     // another unit: ignored
		{"bytes=x-1", size, 0, 10, false, false},     // garbage: ignored
		{"bytes=18446744073709551615-", size, 0, 0, false, true},
		{"bytes=0-18446744073709551616", size, 0, 10, false, false}, // overflows: ignored
	}
	for _, c := range cases {
		off, n, partial, err := parseRange(c.h, c.size)
		if c.unsatisfied {
			if err == nil || err.Status != http.StatusRequestedRangeNotSatisfiable {
				t.Errorf("%q: want 416, got %d %d %v", c.h, off, n, err)
			}
			continue
		}
		if err != nil || off != c.off || n != c.n || partial != c.partial {
			t.Errorf("%q: got %d %d %v %v, want %d %d %v", c.h, off, n, partial, err, c.off, c.n, c.partial)
		}
	}
}

func TestMaxAfterBoundsEveryKeyUnderAPrefix(t *testing.T) {
	for _, p := range []string{"a/", "photos/2024/", strings.Repeat("x", 1020), strings.Repeat("x", 1021) + "/", strings.Repeat("y", 1022) + "/", strings.Repeat("z", 1023) + "/"} {
		m := maxAfter(p)
		if len(m) > storerec.MaxObjectKey || !utf8.ValidString(m) || !storerec.OneLine(m) || !strings.HasPrefix(m, p) {
			t.Fatalf("maxAfter(%d bytes) is not a key under the prefix", len(p))
		}
		room := storerec.MaxObjectKey - len(p)
		// The greatest keys under p of every length fit below it.
		for _, tail := range []string{"", "~", "߿", "￿", "\U0010ffff", strings.Repeat("\U0010ffff", room/4)} {
			k := p + tail
			if len(k) > storerec.MaxObjectKey {
				continue
			}
			if k > m {
				t.Fatalf("a key under %q sorts after the cursor", p[:min(len(p), 12)])
			}
		}
	}
}

func TestEntryMeta(t *testing.T) {
	h := http.Header{}
	h.Set("Content-Type", "Text/Plain; charset=utf-8")
	h.Set("X-Amz-Meta-Color", "blue")
	h.Set("X-Amz-Meta-A-B", "x")
	ct, meta, e := entryMeta(h)
	if e != nil || ct != "text/plain" || len(meta) != 2 || meta[0].Key != "a-b" || meta[1].Key != "color" {
		t.Fatalf("%q %v %v", ct, meta, e)
	}
	h = http.Header{}
	h.Set("Content-Type", "nonsense")
	if ct, _, _ := entryMeta(h); ct != "application/octet-stream" {
		t.Fatal(ct)
	}
	h.Set("X-Amz-Meta-Bstore-Root", "x")
	if _, _, e := entryMeta(h); e == nil || e.Code != "InvalidArgument" {
		t.Fatal("bstore- metadata accepted")
	}
	h = http.Header{}
	h.Set("X-Amz-Meta-Under_score", "x")
	if _, _, e := entryMeta(h); e == nil {
		t.Fatal("a metadata key outside a-z, 0-9 and - accepted")
	}
	h = http.Header{}
	h.Set("X-Amz-Meta-K", "line\nbreak")
	if _, _, e := entryMeta(h); e == nil {
		t.Fatal("a two-line value accepted")
	}
}

func TestRefuseUnsupported(t *testing.T) {
	for _, hv := range [][2]string{{"If-None-Match", "*"}, {"If-Match", `"abc"`}, {"X-Amz-Server-Side-Encryption", "AES256"},
		{"X-Amz-Object-Lock-Mode", "GOVERNANCE"}, {"X-Amz-Tagging", "a=b"}, {"X-Amz-Acl", "public-read"},
		{"X-Amz-Storage-Class", "GLACIER"}, {"X-Amz-Grant-Read", "id=x"}} {
		h := http.Header{}
		h.Set(hv[0], hv[1])
		if e := refuseUnsupported(h); e == nil || e.Status != http.StatusNotImplemented {
			t.Errorf("%s accepted", hv[0])
		}
	}
	h := http.Header{}
	h.Set("X-Amz-Acl", "private")
	h.Set("X-Amz-Storage-Class", "STANDARD")
	if e := refuseUnsupported(h); e != nil {
		t.Fatal(e)
	}
}

func TestURLEncodeKey(t *testing.T) {
	if got := urlEncodeKey("a b/c+d/é"); got != "a+b/c%2Bd/%C3%A9" {
		t.Fatal(got)
	}
}
