package cluster

import (
	"path/filepath"
	"testing"
)

func TestStorageLockExclusionAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raftdb.lock")
	first, err := tryStorageLock(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := tryStorageLock(path)
	if err == nil {
		_ = second.Close()
		_ = first.Close()
		t.Fatal("second directory owner acquired the lock")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := tryStorageLock(path)
	if err != nil {
		t.Fatal("could not reopen after close", err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
}
