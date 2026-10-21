// Package telemetry holds batch/storage measurements shared by node and FSM.
package telemetry

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Storage         *prometheus.HistogramVec
	Snapshots       *prometheus.HistogramVec
	BatchOperations prometheus.Histogram
	BatchBytes      prometheus.Histogram
}

func New() *Metrics {
	return &Metrics{
		Storage:         prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "raftdb_storage_duration_seconds", Help: "Durable storage calls, including unsuccessful calls.", Buckets: prometheus.ExponentialBuckets(.0001, 4, 9)}, []string{"operation"}),
		Snapshots:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "raftdb_snapshot_duration_seconds", Help: "Snapshot capture and persistence duration, including unsuccessful calls.", Buckets: prometheus.ExponentialBuckets(.0001, 4, 9)}, []string{"phase"}),
		BatchOperations: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "raftdb_committed_batch_operations", Help: "Logical operations per successfully committed application batch.", Buckets: []float64{1, 16, 32, 64, 128, 256, 512, 1024}}),
		BatchBytes:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "raftdb_committed_batch_bytes", Help: "Encoded bytes per successfully committed application batch.", Buckets: prometheus.ExponentialBuckets(64, 4, 8)}),
	}
}

func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{m.Storage, m.Snapshots, m.BatchOperations, m.BatchBytes}
}
