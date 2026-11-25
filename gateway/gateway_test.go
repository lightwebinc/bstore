package gateway

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A configuration that is not there, or does not parse, is refused before
// anything is opened.
func TestOpenRefusesABadConfiguration(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(context.Background(), filepath.Join(dir, "none.json"), Options{}); err == nil {
		t.Fatal("a missing configuration opened")
	}
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"listen": 5}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), bad, Options{}); err == nil {
		t.Fatal("a configuration that does not parse opened")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("a refused configuration left files: %s", strings.Join(names, ", "))
	}
}
