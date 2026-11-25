package reader

import (
	"crypto/sha256"
	"testing"
	"time"
)

// A closure's objects begin at different advertisers, each always at the
// same one, and an advertiser that failed lately is asked last whichever
// object asks.
func TestOrderSpreadsObjectsAcrossAdvertisers(t *testing.T) {
	src := []Source{{Advertiser: "a"}, {Advertiser: "b"}, {Advertiser: "c"}}
	var b badSources
	now := time.Unix(1_800_000_000, 0)
	first := map[string]int{}
	for i := 0; i < 300; i++ {
		var h [32]byte
		h[7], h[6] = byte(i), byte(i>>8)
		got := b.order(src, h, now)
		if len(got) != 3 {
			t.Fatalf("object %d: %d advertisers, want 3", i, len(got))
		}
		if again := b.order(src, h, now); again[0] != got[0] {
			t.Fatalf("object %d began at %s, then at %s", i, got[0].Advertiser, again[0].Advertiser)
		}
		// Going round keeps the list's order from the first one asked.
		at := map[string]int{"a": 0, "b": 1, "c": 2}[got[0].Advertiser]
		for k, s := range got {
			if s != src[(at+k)%3] {
				t.Fatalf("object %d: order %v does not go round from %s", i, got, got[0].Advertiser)
			}
		}
		first[got[0].Advertiser]++
	}
	for _, a := range []string{"a", "b", "c"} {
		if first[a] != 100 {
			t.Errorf("advertiser %s was first for %d of 300 objects, want 100", a, first[a])
		}
	}
	// Real object hashes spread too: none of three advertisers is left out
	// or takes most of 3,000 objects.
	real := map[string]int{}
	for i := 0; i < 3000; i++ {
		h := sha256.Sum256([]byte{byte(i), byte(i >> 8), 'x'})
		real[b.order(src, h, now)[0].Advertiser]++
	}
	for _, a := range []string{"a", "b", "c"} {
		if real[a] < 850 || real[a] > 1150 {
			t.Errorf("advertiser %s was first for %d of 3,000 hashed objects, want about 1,000", a, real[a])
		}
	}
	b.mark("b", now)
	for i := 0; i < 300; i++ {
		var h [32]byte
		h[7] = byte(i)
		got := b.order(src, h, now.Add(time.Minute))
		if got[2].Advertiser != "b" {
			t.Fatalf("object %d: the advertiser that failed is at %v, want last", i, got)
		}
	}
	if got := b.order(nil, [32]byte{}, now); len(got) != 0 {
		t.Errorf("no advertisers: %v", got)
	}
	if got := b.order(src[1:2], [32]byte{}, now); len(got) != 1 || got[0].Advertiser != "b" {
		t.Errorf("only a failed advertiser: %v, want it asked", got)
	}
}
