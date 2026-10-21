package cluster

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

func TestStorageMarkerValidationPreservesAcceptedFormats(t *testing.T) {
	cases := []struct {
		backend   string
		directory string
		valid     bool
	}{
		{"bolt", ".", true},
		{"wal", ".", false},
		{"bolt", "storage-", true},
		{"bolt", "storage-bolt-123", true},
		{"wal", "storage-wal-123", true},
		{"unknown", "storage-wal-123", false},
		{"", "storage-wal-123", false},
		{"WAL", "storage-wal-123", false},
		{"wal", "", false},
		{"wal", "storage", false},
		{"wal", "other-123", false},
		{"wal", "..", false},
		{"wal", "../storage-wal-123", false},
		{"wal", filepath.Join("storage-parent", "storage-child"), false},
		{"wal", filepath.Join(string(filepath.Separator), "storage-absolute"), false},
	}
	for _, test := range cases {
		ref := storageRef{Backend: test.backend, Directory: test.directory}
		directory := t.TempDir()
		marker, err := json.Marshal(ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "storage.json"), marker, 0600); err != nil {
			t.Fatal(err)
		}
		opened, err := openStorage(directory, "bolt")
		if err == nil {
			_ = opened.Close()
			t.Fatalf("%+v: silently recreated missing active data", ref)
		}
		if test.valid {
			// A valid marker reaches the missing-file check; it never creates
			// replacement data, including for WAL markers on native Windows.
			if !strings.Contains(err.Error(), "active storage missing:") {
				t.Fatalf("%+v: valid marker rejected: %v", ref, err)
			}
			continue
		}
		if err != nil && err.Error() != "invalid active storage marker" {
			t.Fatalf("marker error changed: %v", err)
		}
	}
}

func TestStorageMigrationPreservesLogsAndVotingState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("WAL requires Linux directory synchronization; covered by the Linux race suite")
	}
	dir := t.TempDir()
	legacy, err := boltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"CurrentTerm", "LastVoteTerm"} {
		if err := legacy.SetUint64([]byte(key), 0x1020304050607080); err != nil {
			t.Fatal(err)
		}
	}
	if err := legacy.Set([]byte("LastVoteCand"), []byte("node3")); err != nil {
		t.Fatal(err)
	}
	for index := uint64(17); index <= 280; index++ {
		if err := legacy.StoreLog(&raft.Log{Index: index, Term: 9, Data: []byte("committed")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"wal", "wal", "bolt", "bolt", "wal"} {
		store, err := openStorage(dir, backend)
		if err != nil {
			t.Fatal(backend, err)
		}
		if store.(*lockedStorage).IsMonotonic() != (backend == "wal") {
			t.Fatal("lost monotonic-store capability")
		}
		if other, err := openStorage(dir, backend); err == nil {
			_ = other.Close()
			t.Fatal("concurrent directory ownership allowed")
		}
		first, _ := store.FirstIndex()
		last, _ := store.LastIndex()
		if first != 17 || last != 280 {
			t.Fatal(backend, first, last)
		}
		for _, index := range []uint64{17, 200, 280} {
			var entry raft.Log
			if err := store.GetLog(index, &entry); err != nil || entry.Term != 9 || !bytes.Equal(entry.Data, []byte("committed")) {
				t.Fatal(backend, entry, err)
			}
		}
		for _, key := range []string{"CurrentTerm", "LastVoteTerm"} {
			if term, err := store.GetUint64([]byte(key)); err != nil || term != 0x1020304050607080 {
				t.Fatal(backend, key, term, err)
			}
		}
		if candidate, err := store.Get([]byte("LastVoteCand")); err != nil || string(candidate) != "node3" {
			t.Fatal(candidate, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStorageMarkerRejectsInvalidOrMissingActiveData(t *testing.T) {
	dir := t.TempDir()
	if store, err := openStorage(dir, "unknown"); err == nil {
		_ = store.Close()
		t.Fatal("accepted backend")
	}
	for _, ref := range []storageRef{{Backend: "wal", Directory: "../outside"}, {Backend: "wal", Directory: "storage-missing"}} {
		data, _ := json.Marshal(ref)
		if err := os.WriteFile(filepath.Join(dir, "storage.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		if store, err := openStorage(dir, "wal"); err == nil {
			_ = store.Close()
			t.Fatal("silently initialized invalid/missing active data")
		}
	}
}
