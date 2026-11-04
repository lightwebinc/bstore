//go:build !linux

package s3d

import "os"

// writeback is nothing where there is no sync_file_range (FreeBSD): the
// part's fsyncs at its end write its blobs out, as before.
func writeback(*os.File) error { return nil }
