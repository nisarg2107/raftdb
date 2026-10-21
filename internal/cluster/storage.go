package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	wal "github.com/hashicorp/raft-wal"
	"go.etcd.io/bbolt"
)

type raftStorage interface {
	raft.LogStore
	raft.StableStore
	io.Closer
}
type storageRef struct {
	Backend   string `json:"backend"`
	Directory string `json:"directory"`
}
type lockedStorage struct {
	raftStorage
	lock io.Closer
}

// Preserve the optional storage capability through our lifetime-lock wrapper.
// Raft must clear old logs when installing a snapshot into a contiguous WAL.
func (s *lockedStorage) IsMonotonic() bool {
	store, ok := s.raftStorage.(raft.MonotonicLogStore)
	return ok && store.IsMonotonic()
}

func (s *lockedStorage) Close() error { return errors.Join(s.raftStorage.Close(), s.lock.Close()) }

func rawStorage(dir, backend string) (raftStorage, error) {
	if backend == "wal" {
		if runtime.GOOS == "windows" {
			return nil, errors.New("WAL storage requires Linux; use Docker or native Windows bolt storage")
		}
		return wal.Open(dir, wal.WithLogger(hclog.NewNullLogger()))
	}
	return boltdb.New(boltdb.Options{Path: filepath.Join(dir, "raft.db"), BoltOptions: &bbolt.Options{Timeout: time.Second}})
}

// The active marker selects a fully copied generation. The old source remains
// intact if copying fails or the process crashes before the marker is replaced.
// A lifetime directory lock prevents concurrent opens racing a backend switch.
func openStorage(dir, backend string) (result raftStorage, err error) {
	if backend == "" {
		backend = "bolt"
	}
	if backend != "bolt" && backend != "wal" {
		return nil, fmt.Errorf("unknown storage backend %q", backend)
	}
	lock, err := tryStorageLock(filepath.Join(dir, "raftdb.lock"))
	if err != nil {
		return nil, fmt.Errorf("lock data directory: %w", err)
	}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	marker := filepath.Join(dir, "storage.json")
	var active storageRef
	data, readErr := os.ReadFile(marker)
	switch {
	case readErr == nil:
		if err = json.Unmarshal(data, &active); err != nil {
			return nil, err
		}
		if (active.Backend != "bolt" && active.Backend != "wal") || (active.Directory != "." && (filepath.Base(active.Directory) != active.Directory || len(active.Directory) < 8 || active.Directory[:8] != "storage-")) || (active.Directory == "." && active.Backend != "bolt") {
			return nil, errors.New("invalid active storage marker")
		}
		// An active store must exist; never silently replace lost data with a new one.
		filename := "raft.db"
		if active.Backend == "wal" {
			filename = "wal-meta.db"
		}
		if _, err = os.Stat(filepath.Join(dir, active.Directory, filename)); err != nil {
			return nil, fmt.Errorf("active storage missing: %w", err)
		}
	case !errors.Is(readErr, os.ErrNotExist):
		return nil, readErr
	default:
		if _, statErr := os.Stat(filepath.Join(dir, "raft.db")); statErr == nil {
			active = storageRef{Backend: "bolt", Directory: "."}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, statErr
		}
	}
	var source raftStorage
	if active.Backend != "" {
		source, err = rawStorage(filepath.Join(dir, active.Directory), active.Backend)
		if err != nil {
			return nil, err
		}
		if active.Backend == backend {
			return &lockedStorage{source, lock}, nil
		}
	}
	targetDir, err := os.MkdirTemp(dir, "storage-"+backend+"-")
	if err != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	target, err := rawStorage(targetDir, backend)
	if err != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, err
	}
	if source != nil {
		err = copyRaftState(source, target)
		closeErr := source.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_ = target.Close()
			return nil, fmt.Errorf("copy Raft storage: %w", err)
		}
	}
	// Close and reopen before publishing, exercising the target's durable recovery.
	if err = target.Close(); err != nil {
		return nil, err
	}
	target, err = rawStorage(targetDir, backend)
	if err != nil {
		return nil, err
	}
	ref := storageRef{Backend: backend, Directory: filepath.Base(targetDir)}
	if err = writeStorageRef(dir, ref); err != nil {
		_ = target.Close()
		return nil, err
	}
	return &lockedStorage{target, lock}, nil
}
