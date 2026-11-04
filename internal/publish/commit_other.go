//go:build !linux

package publish

import "os"

// platformCommit is no syncfs (FreeBSD has none): a group fsyncs each file
// and each distinct directory, at once; and auto groups nothing, since
// grouping by fsyncs measured slower than a sync per write.
func platformCommit() (func(*os.File) error, func(*os.File) (uint64, error), func(string) bool) {
	return nil, nil, nil
}
