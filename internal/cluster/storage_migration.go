package cluster

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

// These are all StableStore keys used by the pinned HashiCorp Raft 1.7.3.
// Use typed access: Bolt and WAL encode uint64 values with different byte orders.
func copyRaftState(source, target raftStorage) error {
	for _, key := range []string{"CurrentTerm", "LastVoteTerm"} {
		value, err := source.GetUint64([]byte(key))
		if errors.Is(err, boltdb.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := target.SetUint64([]byte(key), value); err != nil {
			return err
		}
	}
	candidate, err := source.Get([]byte("LastVoteCand"))
	if err != nil && !errors.Is(err, boltdb.ErrKeyNotFound) {
		return err
	}
	if candidate != nil {
		if err := target.Set([]byte("LastVoteCand"), candidate); err != nil {
			return err
		}
	}
	first, err := source.FirstIndex()
	if err != nil {
		return err
	}
	last, err := source.LastIndex()
	if err != nil {
		return err
	}
	if first == 0 {
		return nil
	}
	batch := make([]*raft.Log, 0, 256)
	size := 0
	for index := first; index <= last; index++ {
		entry := new(raft.Log)
		if err := source.GetLog(index, entry); err != nil {
			return err
		}
		batch = append(batch, entry)
		size += len(entry.Data)
		if len(batch) == 256 || size >= 4*1024*1024 || index == last {
			if err := target.StoreLogs(batch); err != nil {
				return err
			}
			batch = batch[:0]
			size = 0
		}
	}
	return nil
}

func writeStorageRef(dir string, ref storageRef) error {
	f, err := os.CreateTemp(dir, ".storage-marker-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err := json.NewEncoder(f).Encode(ref); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(dir, "storage.json")); err != nil {
		return err
	}
	// Windows does not support syncing directory handles. Docker runs Linux.
	if runtime.GOOS == "windows" {
		return nil
	}
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}
