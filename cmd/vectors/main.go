// Command vectors writes the golden vectors under testdata/vectors.
//
//	GOWORK=off go run ./cmd/vectors            # write
//	GOWORK=off go run ./cmd/vectors -check     # compare, write nothing
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lightwebinc/bstore/internal/vectors"
)

func main() {
	dir := flag.String("dir", "testdata/vectors", "vector directory")
	check := flag.Bool("check", false, "compare with the files on disk and write nothing")
	flag.Parse()
	files, err := vectors.Generate()
	if err != nil {
		fmt.Fprintln(os.Stderr, "vectors:", err)
		os.Exit(1)
	}
	failed := false
	if *check {
		// A file on disk that nothing generates is stale too.
		have, _ := filepath.Glob(filepath.Join(*dir, "*.json"))
		for _, p := range have {
			if _, ok := files[filepath.Base(p)]; !ok {
				fmt.Fprintln(os.Stderr, "not generated:", p)
				failed = true
			}
		}
	}
	for _, name := range vectors.Names(files) {
		path := filepath.Join(*dir, name)
		if *check {
			have, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(have, files[name]) {
				fmt.Fprintln(os.Stderr, "differs:", path)
				failed = true
			}
			continue
		}
		if err := os.WriteFile(path, files[name], 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "vectors:", err)
			os.Exit(1)
		}
	}
	if failed {
		os.Exit(1)
	}
}
