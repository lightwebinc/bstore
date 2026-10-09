// Command vectors writes the golden vectors under testdata/vectors, or with
// -check compares them byte for byte and writes nothing (bcommon devkit's
// vectors runner over this repository's generator).
//
//	GOWORK=off go run ./cmd/vectors            # write
//	GOWORK=off go run ./cmd/vectors -check     # compare, write nothing
package main

import (
	"os"

	devvectors "github.com/lightwebinc/bcommon/devkit/vectors"

	"github.com/lightwebinc/bstore/internal/vectors"
)

func main() {
	files, err := vectors.Generate()
	os.Exit(devvectors.Main(os.Args[1:], files, err, devvectors.Options{Strict: true}, os.Stderr))
}
