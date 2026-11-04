//go:build linux

package s3d

import (
	"os"
	"syscall"
)

// sync_file_range(2) flags.
const (
	syncFileRangeWrite     = 2
	syncFileRangeWaitAfter = 4
)

// writeback writes a blob file's pages to the drive and waits for them
// (sync_file_range), without the flush and the journal commit an fsync
// adds; the part's fsyncs at its end find the data written and have
// only those left, and share them.
func writeback(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := rc.Control(func(fd uintptr) {
		for {
			serr = syscall.SyncFileRange(int(fd), 0, 0, syncFileRangeWrite|syncFileRangeWaitAfter)
			if serr != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return err
	}
	return serr
}
