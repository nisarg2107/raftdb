package metrics

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc/codes"

	"raftdb/internal/cluster"
)

type Metrics struct {
	Registry   *prometheus.Registry
	requests   *prometheus.CounterVec
	latency    *prometheus.HistogramVec
	operations *prometheus.CounterVec
	node       *cluster.Node
	changes    prometheus.Counter
	mu         sync.Mutex
	previous   string
	cache      map[string]cachedMethod
}

type cachedMethod struct {
	ok         prometheus.Counter
	operations prometheus.Counter
	latency    prometheus.Observer
}

func New(n *cluster.Node) *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(), node: n}
	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftdb_requests_total", Help: "Completed RPCs by operation and gRPC code."}, []string{"method", "code"})
	m.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "raftdb_request_duration_seconds", Help: "End-to-end server RPC latency.", Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}}, []string{"method"})
	m.changes = prometheus.NewCounter(prometheus.CounterOpts{Name: "raftdb_leadership_changes_total", Help: "Changes in locally observed leader identity, including unknown."})
	m.cache = make(map[string]cachedMethod)
	m.operations = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftdb_operations_total", Help: "Successfully acknowledged logical operations; batch operations are distinct from RPCs."}, []string{"method"})
	for _, method := range []string{"Get", "Put", "BatchGet", "BatchPut", "Delete", "GetLocal", "Status", "Health", "Snapshot"} {
		m.cache[method] = cachedMethod{ok: m.requests.WithLabelValues(method, codes.OK.String()), latency: m.latency.WithLabelValues(method), operations: m.operations.WithLabelValues(method)}
	}
	m.Registry.MustRegister(m.requests, m.operations, m.latency, m.changes, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), m)
	if n.Telemetry != nil {
		m.Registry.MustRegister(n.Telemetry.Collectors()...)
	}
	return m
}

func (m *Metrics) ObserveOperations(method string, code codes.Code, count int) {
	if code != codes.OK || count == 0 {
		return
	}
	method = method[strings.LastIndex(method, "/")+1:]
	if cached, ok := m.cache[method]; ok {
		cached.operations.Add(float64(count))
	}
}
func (m *Metrics) Observe(method string, code codes.Code, d time.Duration) {
	method = method[strings.LastIndex(method, "/")+1:]
	if cached, ok := m.cache[method]; ok {
		if code == codes.OK {
			cached.ok.Inc()
		} else {
			m.requests.WithLabelValues(method, code.String()).Inc()
		}
		cached.latency.Observe(d.Seconds())
		return
	}
	m.requests.WithLabelValues(method, code.String()).Inc()
	m.latency.WithLabelValues(method).Observe(d.Seconds())
}

var statNames = map[string]string{"term": "raftdb_term", "last_log_index": "raftdb_last_log_index", "applied_index": "raftdb_applied_index", "commit_index": "raftdb_commit_index", "num_peers": "raftdb_peer_count", "last_snapshot_index": "raftdb_last_snapshot_index", "last_snapshot_term": "raftdb_last_snapshot_term", "fsm_pending": "raftdb_fsm_pending"}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, labels, nil)
}
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, name := range statNames {
		ch <- desc(name, "HashiCorp Raft public statistic.")
	}
	ch <- desc("raftdb_read_rounds_total", "Fresh quorum and actual FSM application fence rounds.")
	ch <- desc("raftdb_state", "Current Raft role.", "state")
	ch <- desc("raftdb_leader_info", "Known leader identity; empty when unknown.", "leader_id")
	ch <- desc("raftdb_first_log_index", "Oldest retained log index.")
	ch <- desc("raftdb_snapshots_total", "FSM captures and restores since process start.", "event")
}
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	created, restored := m.node.FSM.SnapshotCounts()
	ch <- prometheus.MustNewConstMetric(desc("raftdb_snapshots_total", "FSM captures and restores since process start.", "event"), prometheus.CounterValue, float64(created), "created")
	ch <- prometheus.MustNewConstMetric(desc("raftdb_snapshots_total", "FSM captures and restores since process start.", "event"), prometheus.CounterValue, float64(restored), "restored")
	ch <- prometheus.MustNewConstMetric(desc("raftdb_read_rounds_total", "Fresh quorum and actual FSM application fence rounds."), prometheus.CounterValue, float64(m.node.ReadRounds.Load()))
	stats := m.node.Raft.Stats()
	for key, name := range statNames {
		if v, err := strconv.ParseFloat(stats[key], 64); err == nil {
			ch <- prometheus.MustNewConstMetric(desc(name, "HashiCorp Raft public statistic."), prometheus.GaugeValue, v)
		}
	}
	currentState := m.node.Raft.State().String()
	for _, state := range []string{"Follower", "Candidate", "Leader", "Shutdown"} {
		v := 0.
		if currentState == state {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(desc("raftdb_state", "Current Raft role.", "state"), prometheus.GaugeValue, v, state)
	}
	id, _ := m.node.Leader()
	ch <- prometheus.MustNewConstMetric(desc("raftdb_leader_info", "Known leader identity; empty when unknown.", "leader_id"), prometheus.GaugeValue, 1, id)
	m.mu.Lock()
	if id != m.previous {
		m.changes.Inc()
		m.previous = id
	}
	m.mu.Unlock()
	if first, err := m.node.Logs.FirstIndex(); err == nil {
		ch <- prometheus.MustNewConstMetric(desc("raftdb_first_log_index", "Oldest retained log index."), prometheus.GaugeValue, float64(first))
	}
}
