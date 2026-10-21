package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"golang.org/x/sync/semaphore"

	"raftdb/internal/config"
	"raftdb/internal/store"
)

// In-memory transports deterministically cut peer links while retaining real
// Raft, durable Bbolt logs and the production read/write paths. Docker scripts
// separately exercise Linux packet filtering against real TCP transports.
func TestPartitionDivergentUncommittedLog(t *testing.T) {
	const count = 5
	nodes := make([]*Node, count)
	transports := make([]*raft.InmemTransport, count)
	addresses := make([]raft.ServerAddress, count)
	var servers []raft.Server
	for i := 0; i < count; i++ {
		addresses[i], transports[i] = raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("n%d", i)))
		servers = append(servers, raft.Server{ID: raft.ServerID(fmt.Sprintf("n%d", i)), Address: addresses[i], Suffrage: raft.Voter})
	}
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j {
				transports[i].Connect(addresses[j], transports[j])
			}
		}
	}
	for i := 0; i < count; i++ {
		logs, err := boltdb.NewBoltStore(filepath.Join(t.TempDir(), "raft.db"))
		if err != nil {
			t.Fatal(err)
		}
		rc := raft.DefaultConfig()
		rc.BatchApplyCh = true
		rc.LocalID = servers[i].ID
		rc.HeartbeatTimeout = 200 * time.Millisecond
		rc.ElectionTimeout = 200 * time.Millisecond
		rc.LeaderLeaseTimeout = 150 * time.Millisecond
		rc.LogLevel = "ERROR"
		fsm := store.New()
		r, err := raft.NewRaft(rc, fsm, logs, logs, raft.NewInmemSnapshotStore(), transports[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &Node{Raft: r, FSM: fsm, Logs: logs, Config: config.Config{OperationTimeout: 500 * time.Millisecond, ReadBatchWindow: 200 * time.Microsecond}, slots: semaphore.NewWeighted(64)}
		node := nodes[i]
		t.Cleanup(func() { _ = node.Close() })
	}
	if err := nodes[0].Raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatal(err)
	}
	wait := func(condition func() bool) {
		t.Helper()
		until := time.Now().Add(6 * time.Second)
		for time.Now().Before(until) {
			if condition() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("partition condition timed out")
	}
	leader := -1
	wait(func() bool {
		for i, n := range nodes {
			if n.Raft.State() == raft.Leader {
				leader = i
				return true
			}
		}
		return false
	})
	if err := nodes[leader].Write(context.Background(), store.Command{Op: "put", Key: "key", Value: "committed"}); err != nil {
		t.Fatal(err)
	}
	// Initialize the term fence before isolation: later reads must still obtain
	// a fresh quorum rather than relying on the cached committed initialization.
	if _, _, err := nodes[leader].Read(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}
	minority := map[int]bool{leader: true, (leader + 1) % count: true}
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if minority[i] != minority[j] {
				transports[i].Disconnect(addresses[j])
			}
		}
	}
	// The old leader has a short lease window in which it can append, but cannot
	// commit. Observe its durable index to prove we constructed a divergent log.
	prior := nodes[leader].Raft.LastIndex()
	pending := nodes[leader].Raft.Apply([]byte(`{"op":"put","key":"key","value":"uncommitted"}`), time.Second)
	wait(func() bool { return nodes[leader].Raft.LastIndex() > prior })
	if err := pending.Error(); err == nil {
		t.Fatal("minority acknowledged uncommitted entry")
	}
	if _, _, err := nodes[leader].Read(context.Background(), "key"); err == nil {
		t.Fatal("minority served linearizable read")
	}
	next := -1
	wait(func() bool {
		for i, n := range nodes {
			if !minority[i] && n.Raft.State() == raft.Leader {
				next = i
				return true
			}
		}
		return false
	})
	if err := nodes[next].Write(context.Background(), store.Command{Op: "put", Key: "key", Value: "majority"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j {
				transports[i].Connect(addresses[j], transports[j])
			}
		}
	}
	wait(func() bool {
		for _, n := range nodes {
			if v, _ := n.FSM.Get("key"); v != "majority" {
				return false
			}
		}
		return true
	})
	if value, _, err := nodes[next].Read(context.Background(), "key"); err != nil || value != "majority" {
		t.Fatal(value, err)
	}
}
