package cluster

import (
	"time"

	"github.com/hashicorp/raft"

	"raftdb/internal/telemetry"
)

type measuredStorage struct {
	raftStorage
	metrics *telemetry.Metrics
}

func (s *measuredStorage) StoreLogs(logs []*raft.Log) error {
	start := time.Now()
	err := s.raftStorage.StoreLogs(logs)
	s.metrics.Storage.WithLabelValues("store_logs").Observe(time.Since(start).Seconds())
	return err
}
func (s *measuredStorage) StoreLog(log *raft.Log) error { return s.StoreLogs([]*raft.Log{log}) }
func (s *measuredStorage) DeleteRange(min, max uint64) error {
	start := time.Now()
	err := s.raftStorage.DeleteRange(min, max)
	s.metrics.Storage.WithLabelValues("delete_range").Observe(time.Since(start).Seconds())
	return err
}
func (s *measuredStorage) IsMonotonic() bool {
	monotonic, ok := s.raftStorage.(raft.MonotonicLogStore)
	return ok && monotonic.IsMonotonic()
}
