package storerec

import (
	"os"
	"strings"
	"testing"
)

// TestDetectionTablesMatchSpec holds spec section 7.6's two tables to the
// formula, cell by cell.
func TestDetectionTablesMatchSpec(t *testing.T) {
	spec, err := os.ReadFile("../../docs/spec.md")
	if err != nil {
		t.Fatal(err)
	}
	a, s := DetectionTables()
	for _, table := range []string{a, s} {
		if !strings.Contains(string(spec), table) {
			t.Errorf("spec section 7.6 does not hold the generated table:\n%s", table)
		}
	}
}
