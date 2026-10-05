package publish

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrInUse is an identity directory another process has open: two
// processes writing one identity's state would each overwrite the other's
// record of what it spent.
var ErrInUse = errors.New("publish: the identity is in use by another process")

// dirLock is an exclusive advisory lock on an identity's directory, held
// while a Publisher is open and released when the process ends.
type dirLock struct{ f *os.File }

func lockDir(dir string) (*dirLock, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrInUse, dir)
		}
		return nil, err
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) release() {
	if l != nil && l.f != nil {
		_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
		_ = l.f.Close()
		l.f = nil
	}
}
