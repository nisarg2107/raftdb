package cluster

import (
	"io"
	"os"

	"golang.org/x/sys/windows"
)

// Avoid the legacy helper's GENERIC_ALL access and incorrect lock-violation
// handling. Closing the file releases this nonblocking exclusive byte lock.
func tryStorageLock(path string) (io.Closer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{}); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
