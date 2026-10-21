//go:build !windows

package cluster

import (
	"io"
	"os"

	"github.com/coreos/etcd/pkg/fileutil"
)

func tryStorageLock(path string) (io.Closer, error) {
	return fileutil.TryLockFile(path, os.O_WRONLY|os.O_CREATE, 0600)
}
