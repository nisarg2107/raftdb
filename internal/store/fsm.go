// Package store implements only deterministic state changes; Raft owns consensus.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"

	"raftdb/internal/telemetry"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func (c Command) Validate() error {
	if c.Op == "fence" && c.Key == "" && c.Value == "" {
		return nil
	}
	if c.Op != "put" && c.Op != "delete" {
		return errors.New("unknown operation")
	}
	if err := ValidateKey(c.Key); err != nil {
		return err
	}
	if len(c.Value) > 256*1024 {
		return errors.New("value exceeds 256 KiB")
	}
	return nil
}

type FSM struct {
	Telemetry *telemetry.Metrics
	mu        sync.RWMutex
	values    map[string]string
	created   atomic.Uint64
	restored  atomic.Uint64
	applied   atomic.Uint64
	changed   chan struct{}
}

func New() *FSM                                 { return &FSM{values: make(map[string]string), changed: make(chan struct{})} }
func (f *FSM) SnapshotCounts() (uint64, uint64) { return f.created.Load(), f.restored.Load() }

// Apply is called by Raft only after commitment, in log order on every node.
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := f.applyLocked(log)
	f.publish(log.Index)
	return result
}

// ApplyBatch also observes configuration entries. Raft omits no-ops/barriers;
// the read fence explicitly skips those entries when selecting its target.
func (f *FSM) ApplyBatch(logs []*raft.Log) []interface{} {
	results := make([]interface{}, len(logs))
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, log := range logs {
		results[i] = f.applyLocked(log)
	}
	if len(logs) > 0 {
		f.publish(logs[len(logs)-1].Index)
	}
	return results
}

func (f *FSM) applyLocked(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}
	commands, err := DecodeBatch(log.Data)
	if err != nil {
		return fmt.Errorf("invalid committed command: %w", err)
	}
	for _, c := range commands {
		switch c.Op {
		case "put":
			f.values[c.Key] = c.Value
		case "delete":
			delete(f.values, c.Key)
		}
	}
	return nil
}

func (f *FSM) publish(index uint64) {
	f.applied.Store(index)
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *FSM) AppliedIndex() uint64 { return f.applied.Load() }

// WaitApplied observes application completion, not Raft's asynchronous enqueue.
func (f *FSM) WaitApplied(ctx context.Context, index uint64) error {
	for f.applied.Load() < index {
		f.mu.RLock()
		changed := f.changed
		ready := f.applied.Load() >= index
		f.mu.RUnlock()
		if ready {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}
func (f *FSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.values[key]
	return v, ok
}

type snapshot struct {
	metrics      *telemetry.Metrics
	Version      int               `json:"version"`
	Values       map[string]string `json:"values"`
	AppliedIndex uint64            `json:"applied_index,omitempty"`
}

// Snapshot copies under the lock; serialization and disk IO happen after release.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	start := time.Now()
	if f.Telemetry != nil {
		defer func() { f.Telemetry.Snapshots.WithLabelValues("capture").Observe(time.Since(start).Seconds()) }()
	}
	f.mu.RLock()
	values := make(map[string]string, len(f.values))
	for k, v := range f.values {
		values[k] = v
	}
	index := f.applied.Load()
	f.mu.RUnlock()
	f.created.Add(1)
	slog.Info("snapshot captured", "keys", len(values))
	return &snapshot{Version: 1, Values: values, AppliedIndex: index, metrics: f.Telemetry}, nil
}

// Persist closes only complete snapshots; cancellation prevents using partial data.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	start := time.Now()
	if s.metrics != nil {
		defer func() { s.metrics.Snapshots.WithLabelValues("persist").Observe(time.Since(start).Seconds()) }()
	}
	if err := json.NewEncoder(sink).Encode(s); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}
func (s *snapshot) Release() {}

// Restore validates before replacing state so corrupt snapshots fail closed.
func (f *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	var s snapshot
	d := json.NewDecoder(reader)
	if err := d.Decode(&s); err != nil {
		return err
	}
	var extra interface{}
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing snapshot data")
	}
	if s.Version != 1 || s.Values == nil {
		return errors.New("unsupported or invalid snapshot")
	}
	for k, v := range s.Values {
		if err := (Command{Op: "put", Key: k, Value: v}).Validate(); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.values = s.Values
	f.publish(s.AppliedIndex)
	f.mu.Unlock()
	f.restored.Add(1)
	slog.Info("snapshot restored", "keys", len(s.Values))
	return nil
}
