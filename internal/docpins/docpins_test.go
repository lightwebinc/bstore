// Package docpins holds the versions the documentation names to what the
// repository pins: go.mod for the Go modules and the Go release,
// host/package.json for the SDK the host module is built with and Node. A
// version written into a page goes stale silently the first time a pin
// moves; this test fails instead.
package docpins

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const root = "../.."

// pages are every document a reader is pointed at.
func pages(t *testing.T) []string {
	t.Helper()
	ps := []string{filepath.Join(root, "README.md"), filepath.Join(root, "QUICKSTART.md")}
	more, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	ps = append(ps, more...)
	var out []string
	for _, p := range ps {
		if _, err := os.Stat(p); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// goMod reads the required version of each module, and the go line.
func goMod(t *testing.T) (map[string]string, string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	req := map[string]string{}
	goVer := ""
	for _, l := range strings.Split(string(raw), "\n") {
		f := strings.Fields(strings.TrimPrefix(strings.TrimSpace(l), "require "))
		switch {
		case len(f) >= 2 && f[0] == "go":
			goVer = f[1]
		case len(f) >= 2 && strings.Contains(f[0], "/") && strings.HasPrefix(f[1], "v"):
			req[f[0]] = f[1]
		}
	}
	return req, goVer
}

// A mention is a version written after a name; ignore says which mentions
// are of something else (another project's release, quoted as history).
type pin struct {
	name   string
	re     *regexp.Regexp
	want   string
	ignore func(before string) bool
}

func TestDocumentedVersionsMatchThePins(t *testing.T) {
	req, goVer := goMod(t)
	var pkg struct {
		Dependencies map[string]string `json:"dependencies"`
		Engines      map[string]string `json:"engines"`
	}
	raw, err := os.ReadFile(filepath.Join(root, "host", "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &pkg); err != nil {
		t.Fatal(err)
	}
	node := regexp.MustCompile(`>=(\d+)`).FindStringSubmatch(pkg.Engines["node"])
	if node == nil {
		t.Fatalf("host/package.json engines.node %q", pkg.Engines["node"])
	}
	goMinor := regexp.MustCompile(`^\d+\.\d+`).FindString(goVer)
	pins := []pin{
		{name: "go-sdk", re: regexp.MustCompile(`go-sdk\x60? (v\d+\.\d+\.\d+)`), want: req["github.com/bsv-blockchain/go-sdk"]},
		// A pin may be a pseudo-version between tags; the document names it whole.
		{name: "bcommon", re: regexp.MustCompile(`bcommon\x60? (v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)`), want: req["github.com/lightwebinc/bcommon"]},
		{name: "@bsv/sdk", re: regexp.MustCompile(`@bsv/sdk\x60? (\d+\.\d+\.\d+)`), want: pkg.Dependencies["@bsv/sdk"],
			// The advertisement check was ported from ts-stack's release,
			// a fixed point of history, not the SDK this repository pins.
			ignore: func(before string) bool {
				return strings.Contains(before, "ts-stack") || strings.Contains(before, "as of")
			}},
		{name: "Go", re: regexp.MustCompile(`\bGo (1\.\d+)\b`), want: goMinor},
		{name: "Node", re: regexp.MustCompile(`\bNode (\d+)\b`), want: node[1]},
	}
	seen := map[string]int{}
	for _, p := range pages(t) {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, pn := range pins {
			if pn.want == "" {
				t.Fatalf("%s: no pin found", pn.name)
			}
			for _, m := range pn.re.FindAllStringSubmatchIndex(text, -1) {
				got := text[m[2]:m[3]]
				before := text[max(0, m[0]-40):m[0]]
				if pn.ignore != nil && pn.ignore(before) {
					continue
				}
				seen[pn.name]++
				if got != pn.want {
					line := strings.Count(text[:m[0]], "\n") + 1
					t.Errorf("%s:%d names %s %s; the pin is %s", filepath.Base(p), line, pn.name, got, pn.want)
				}
			}
		}
	}
	for _, n := range []string{"go-sdk", "bcommon", "Go", "Node"} {
		if seen[n] == 0 {
			t.Errorf("no page names the %s version; the README should", n)
		}
	}
}
