//go:build linux

package publish

import (
	"os"
	"runtime"
	"syscall"
)

// sysSyncfs is syncfs(2)'s number on the architectures bstore is built
// for (the syscall package's tables predate it, and golang.org/x/sys is not
// a dependency); zero elsewhere, which falls back to fsyncs.
func sysSyncfs() uintptr {
	switch runtime.GOARCH {
	case "amd64":
		return 306
	case "arm64", "riscv64", "loong64":
		return 267
	}
	return 0
}

// xfsMagic is XFS's statfs f_type.
const xfsMagic = 0x58465342

// platformCommit is syncfs(2), which makes every file and directory of a
// filesystem durable in one call (since Linux 5.8 it also reports
// writeback errors), the filesystem a file is on, and whether auto groups
// a directory's writes: on XFS, where grouping measured faster.
func platformCommit() (func(*os.File) error, func(*os.File) (uint64, error), func(string) bool) {
	nr := sysSyncfs()
	if nr == 0 {
		return nil, nil, nil
	}
	syncfs := func(f *os.File) error {
		rc, err := f.SyscallConn()
		if err != nil {
			return err
		}
		var serr error
		if err := rc.Control(func(fd uintptr) {
			for {
				_, _, e := syscall.Syscall(nr, fd, 0, 0)
				if e == syscall.EINTR {
					continue
				}
				if e != 0 {
					serr = &os.SyscallError{Syscall: "syncfs", Err: e}
				}
				return
			}
		}); err != nil {
			return err
		}
		return serr
	}
	devOf := func(f *os.File) (uint64, error) {
		info, err := f.Stat()
		if err != nil {
			return 0, err
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			return uint64(st.Dev), nil
		}
		return 0, nil
	}
	groups := func(dir string) bool {
		var st syscall.Statfs_t
		return syscall.Statfs(dir, &st) == nil && st.Type == xfsMagic
	}
	return syncfs, devOf, groups
}
