package metrics

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"

	"raftdb/internal/cluster"
	"raftdb/internal/config"
	"raftdb/internal/store"
)

func TestMetricsReflectRealOperations(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	c := config.Default()
	c.DataDir = t.TempDir()
	c.RaftAddress = address
	c.Peers = "node1=" + address + "=127.0.0.1:50051"
	c.Bootstrap = true
	c.Heartbeat = 100 * time.Millisecond
	c.Election = 100 * time.Millisecond
	c.Lease = 50 * time.Millisecond
	c.LogLevel = "ERROR"
	n, err := cluster.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	until := time.Now().Add(5 * time.Second)
	for n.Raft.State() != raft.Leader && time.Now().Before(until) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := n.Write(context.Background(), store.Command{Op: "put", Key: "key", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	m := New(n)
	m.Observe("/raftdb.v1.Database/Put", codes.OK, time.Millisecond)
	if err := n.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, family := range families {
		switch family.GetName() {
		case "raftdb_requests_total":
			var total float64
			for _, metric := range family.Metric {
				total += metric.GetCounter().GetValue()
			}
			if total != 1 {
				t.Fatal("wrong request counter")
			}
			found[family.GetName()] = true
		case "raftdb_request_duration_seconds":
			var total uint64
			for _, metric := range family.Metric {
				total += metric.GetHistogram().GetSampleCount()
			}
			if total != 1 {
				t.Fatal("wrong latency count")
			}
			found[family.GetName()] = true
		case "raftdb_last_log_index":
			if family.Metric[0].GetGauge().GetValue() != float64(n.Raft.LastIndex()) {
				t.Fatal("wrong log index")
			}
			found[family.GetName()] = true
		case "raftdb_snapshots_total":
			var count float64
			for _, metric := range family.Metric {
				count += metric.GetCounter().GetValue()
			}
			if count < 1 {
				t.Fatal("snapshot not counted")
			}
			found[family.GetName()] = true
		}
	}
	if len(found) != 4 {
		t.Fatal("missing metric families", found)
	}
}
