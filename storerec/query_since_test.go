package storerec

import (
	"strings"
	"testing"
)

// TestSinceQuery: the class amendment A2 adds is parsed beside the classes
// of the first publish, with its value rule; Classes itself is unchanged.
func TestSinceQuery(t *testing.T) {
	for _, c := range Classes {
		if c.Name == Since.Name {
			t.Fatal("Classes names since: the query-v1 vectors pin it without")
		}
	}
	txid := strings.Repeat("ab", 32)
	pos := "0001798761600000:" + txid + ":0000000003"
	for raw, want := range map[string]string{
		`{"bucket":"photos_qzvmkwerty","since":""}`:                                          "since",
		`{"bucket":"photos_qzvmkwerty","since":"` + pos + `"}`:                               "since",
		`{"bucket":"photos_qzvmkwerty","since":"` + pos[1:] + `"}`:                           "",
		`{"bucket":"photos_qzvmkwerty","since":"` + strings.ToUpper(pos) + `"}`:              "",
		`{"bucket":"photos_qzvmkwerty","since":"` + strings.Replace(pos, ":", ".", 1) + `"}`: "",
		`{"bucket":"photos_qzvmkwerty","since":"1798761600000:` + txid + `:3"}`:              "",
		`{"bucket":"photos_qzvmkwerty","since":"` + pos + `","after":""}`:                    "",
		`{"bucket":"photos_qzvmkwerty","since":7}`:                                           "",
		`{"since":""}`: "",
	} {
		q, err := ParseQuery([]byte(raw), nil, nil)
		got := ""
		if err == nil {
			got = q.Class
		}
		if got != want {
			t.Errorf("%s: class %q (%v), want %q", raw, got, err, want)
		}
	}
	if !SincePosition(pos) || SincePosition("") || SincePosition(pos+"0") {
		t.Fatal("SincePosition")
	}
}
