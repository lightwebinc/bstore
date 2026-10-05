package storerec_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func load(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "vectors", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type fillJSON struct {
	At   int  `json:"at"`
	Byte byte `json:"byte"`
	Len  int  `json:"len"`
}

func expand(t *testing.T, h string, f *fillJSON) []byte {
	b := unhex(t, h)
	if f == nil {
		return b
	}
	out := append([]byte{}, b[:f.At]...)
	out = append(out, bytes.Repeat([]byte{f.Byte}, f.Len)...)
	return append(out, b[f.At:]...)
}

func decodeKind(kind string, raw []byte) error {
	var err error
	switch kind {
	case "bucket":
		_, err = storerec.DecodeBucket(raw)
	case "entry":
		_, err = storerec.DecodeEntry(raw)
	case "piece":
		_, err = storerec.DecodePiece(raw)
	case "front":
		_, err = storerec.DecodeFront(raw)
	case "outcome":
		_, err = storerec.DecodeOutcome(raw)
	case "request":
		_, err = storerec.DecodeRequest(raw)
	case "body":
		_, err = storerec.DecodeBody(raw)
	}
	return err
}

// TestRefusalVectors reads refusal-v1.json back through the codec, so that a
// change to a decoder fails here as well as in vectors-check.
func TestRefusalVectors(t *testing.T) {
	var v struct {
		Cases []struct {
			Kind, Name, Hex, Label, Rule string
			Fill                         *fillJSON
		}
	}
	load(t, "refusal-v1.json", &v)
	if len(v.Cases) < 500 {
		t.Fatalf("only %d refusal cases", len(v.Cases))
	}
	for _, c := range v.Cases {
		err := decodeKind(c.Kind, expand(t, c.Hex, c.Fill))
		if got := storerec.Reason(err); got != c.Label {
			t.Errorf("%s %q: %s (%v), want %s", c.Kind, c.Name, got, err, c.Label)
		}
		if got := storerec.Rule(err); got != c.Rule {
			t.Errorf("%s %q: rule %q, want %q", c.Kind, c.Name, got, c.Rule)
		}
	}
}

// TestEveryLabelHasAVector holds the vectors to every label of the contract:
// each appears in some vector file.
func TestEveryLabelHasAVector(t *testing.T) {
	var all []byte
	files, _ := filepath.Glob(filepath.Join("..", "..", "testdata", "vectors", "*.json"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		all = append(all, b...)
	}
	for _, l := range []string{
		// Record refusals (spec sections 3.2, 4.2, 8.1, 8.2).
		"too-large", "cbor", "key-type", "magic", "missing", "type", "bucket", "identity", "seq", "writers", "changes", "count",
		"created", "consumed", "first", "objkey", "kind", "size", "profile", "content-type", "etag", "meta", "hold", "empty",
		"request", "outcome", "response-signature", "url", "piece",
		// Transactions (12.1).
		"shape", "beef", "mineable", "unlock", "lock", "signature", "funding", "unmined", "token-input", "bucket-id", "too-many",
		"not-bstore",
		// The fold (12.2), a reader (15.2), a buyer (7.4), an answerer (7.2).
		"map-mismatch", "map", "rollback", "entry", "writer", "root", "closure", "content", "proof-root",
		"fetch", "segment", "path", "seed", "unknown-root", "query",
	} {
		if !bytes.Contains(all, []byte(`"`+l+`"`)) {
			t.Errorf("no vector carries the label %q", l)
		}
	}
}

// TestBcommonCHIRPVectors runs BRC-167's own vectors, bcommon's chirp-v1.json
// at the tag go.mod pins, through the CHIRP codec bstore uses: every golden
// root builds to its hash, every closure verifies or is refused as bcommon
// says.
func TestBcommonCHIRPVectors(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/lightwebinc/bcommon").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(out)), "testdata", "vectors", "chirp-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Golden []struct {
			Name, ContentHex, RootHashHex string
			Extensions                    []any
		}
		Closures []struct {
			Name, RootHashHex, Reason string
			Objects                   []struct{ HashHex, Hex string }
			MaxReferences             int
			MaxLength                 uint64
		}
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	for _, g := range v.Golden {
		if len(g.Extensions) != 0 {
			continue
		}
		built, err := storerec.Build(unhex(t, g.ContentHex))
		if err != nil || hex.EncodeToString(built.RootHash[:]) != g.RootHashHex {
			t.Errorf("golden %s: %v", g.Name, err)
		}
	}
	for _, c := range v.Closures {
		objs := map[[32]byte][]byte{}
		for _, o := range c.Objects {
			objs[[32]byte(unhex(t, o.HashHex))] = unhex(t, o.Hex)
		}
		_, err := chirp.Verify([32]byte(unhex(t, c.RootHashHex)), func(h [32]byte) ([]byte, error) {
			if o, ok := objs[h]; ok {
				return o, nil
			}
			return nil, errors.New("not served")
		}, chirp.Limits{MaxReferences: c.MaxReferences, MaxLength: c.MaxLength})
		got, _ := chirp.Reason(err)
		if got != c.Reason {
			t.Errorf("closure %s: %q, want %q", c.Name, got, c.Reason)
		}
	}
}
