package storerec_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// seeds are every record of hex-v1.json and refusal-v1.json, small ones.
func seeds(f *testing.F, kind string) {
	var h struct{ Records []struct{ Kind, Hex string } }
	b, _ := os.ReadFile(filepath.Join("..", "..", "testdata", "vectors", "hex-v1.json"))
	_ = json.Unmarshal(b, &h)
	for _, r := range h.Records {
		if r.Kind == kind {
			x, _ := hex.DecodeString(r.Hex)
			f.Add(x)
		}
	}
	var rf struct{ Cases []struct{ Kind, Hex string } }
	b, _ = os.ReadFile(filepath.Join("..", "..", "testdata", "vectors", "refusal-v1.json"))
	_ = json.Unmarshal(b, &rf)
	for _, r := range rf.Cases {
		if r.Kind == kind && len(r.Hex) < 4096 {
			x, _ := hex.DecodeString(r.Hex)
			f.Add(x)
		}
	}
}

type encoder interface{ Encode() ([]byte, error) }

// roundTrip holds a decoder to canonical encoding: whatever it admits
// re-encodes to exactly the bytes it read, and never panics.
func roundTrip(t *testing.T, raw []byte, dec func([]byte) (encoder, error)) {
	r, err := dec(raw)
	if err != nil {
		if storerec.Reason(err) == "other" {
			t.Fatalf("a refusal with no label: %v", err)
		}
		return
	}
	again, err := r.Encode()
	if err != nil {
		t.Fatalf("admitted, does not re-encode: %v", err)
	}
	if !bytes.Equal(again, raw) {
		t.Fatalf("admitted %x, re-encodes to %x", raw, again)
	}
}

func FuzzBucket(f *testing.F) {
	seeds(f, "bucket")
	f.Fuzz(func(t *testing.T, raw []byte) {
		roundTrip(t, raw, func(b []byte) (encoder, error) { return storerec.DecodeBucket(b) })
	})
}

func FuzzEntry(f *testing.F) {
	seeds(f, "entry")
	f.Fuzz(func(t *testing.T, raw []byte) {
		roundTrip(t, raw, func(b []byte) (encoder, error) { return storerec.DecodeEntry(b) })
	})
}

func FuzzOutcome(f *testing.F) {
	seeds(f, "outcome")
	f.Fuzz(func(t *testing.T, raw []byte) {
		roundTrip(t, raw, func(b []byte) (encoder, error) { return storerec.DecodeOutcome(b) })
	})
}

func FuzzFront(f *testing.F) {
	seeds(f, "front")
	f.Fuzz(func(t *testing.T, raw []byte) {
		roundTrip(t, raw, func(b []byte) (encoder, error) { return storerec.DecodeFront(b) })
	})
}

func FuzzRequest(f *testing.F) {
	seeds(f, "request")
	f.Fuzz(func(t *testing.T, raw []byte) {
		roundTrip(t, raw, func(b []byte) (encoder, error) { return storerec.DecodeRequest(b) })
	})
}

func FuzzContexts(f *testing.F) {
	f.Add([]byte{0x82, 0x00, 0x80})
	f.Add([]byte{0x80})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if c, err := storerec.DecodeEntryContext(raw); err == nil {
			again, _ := c.Encode()
			if !bytes.Equal(again, raw) {
				t.Fatalf("entry context %x re-encodes to %x", raw, again)
			}
		}
		if ls, err := storerec.DecodeTokenContext(raw); err == nil {
			again, _ := storerec.EncodeTokenContext(ls)
			if !bytes.Equal(again, raw) {
				t.Fatalf("token context %x re-encodes to %x", raw, again)
			}
		}
	})
}

func FuzzAdvertisement(f *testing.F) {
	var s struct{ Cases []struct{ Script string } }
	b, _ := os.ReadFile(filepath.Join("..", "..", "testdata", "vectors", "script-v1.json"))
	_ = json.Unmarshal(b, &s)
	for i, c := range s.Cases {
		if i%25 == 0 {
			x, _ := hex.DecodeString(c.Script)
			f.Add(x)
		}
	}
	f.Fuzz(func(t *testing.T, script []byte) {
		ad, err := storerec.DecodeAdvertisement(script)
		if err == nil && ad.Expiry == 0 {
			t.Fatal("an advertisement admitted with expiry 0")
		}
		_ = storerec.Classify(script)
	})
}
