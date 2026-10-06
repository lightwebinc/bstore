package publish

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
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

// lockDirWait is lockDir, waiting up to wait for another process to
// release the directory: a gateway restarted while a command has its
// identity open starts once the command ends.
func lockDirWait(ctx context.Context, dir string, wait time.Duration) (*dirLock, error) {
	end := time.Now().Add(wait)
	for {
		l, err := lockDir(dir)
		if err == nil || !errors.Is(err, ErrInUse) || !time.Now().Before(end) {
			return l, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(500 * time.Millisecond):
		}
	}
}
