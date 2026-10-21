package integration

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftdb/api"
	"raftdb/internal/cluster"
	"raftdb/internal/config"
	"raftdb/internal/server"
)

type member struct {
	node   *cluster.Node
	rpc    api.DatabaseClient
	conn   *grpc.ClientConn
	server *grpc.Server
	cfg    config.Config
}

type group struct {
	t       *testing.T
	members []*member
}

func address(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func start(t *testing.T, count int) *group { return startWithWindow(t, count, 5*time.Millisecond) }

func startWithWindow(t *testing.T, count int, window time.Duration, pipeline ...int) *group {
	t.Helper()
	if testing.Short() {
		t.Skip("integration cluster skipped in short mode")
	}
	g := &group{t: t}
	dir := t.TempDir()
	var peers []string
	for i := 0; i < count; i++ {
		c := config.Default()
		if runtime.GOOS != "windows" {
			c.Storage = "wal"
		}
		if len(pipeline) > 0 {
			c.WritePipeline = pipeline[0]
		}
		c.ReadBatchWindow = window
		c.BatchApply = true
		c.ID = fmt.Sprintf("n%d", i)
		c.RaftAddress = address(t)
		c.GRPCAddress = address(t)
		c.DataDir = filepath.Join(dir, c.ID)
		c.Heartbeat = time.Second
		c.Election = time.Second
		c.Lease = 500 * time.Millisecond
		c.OperationTimeout = 3 * time.Second
		c.LogLevel = "ERROR"
		c.SnapshotInterval = time.Hour
		c.SnapshotThreshold = 32
		c.TrailingLogs = 4
		c.Bootstrap = i == 0
		g.members = append(g.members, &member{cfg: c})
		peers = append(peers, fmt.Sprintf("%s=%s=%s", c.ID, c.RaftAddress, c.GRPCAddress))
	}
	t.Cleanup(func() {
		for _, m := range g.members {
			g.stop(m)
		}
	})
	for _, m := range g.members {
		m.cfg.Peers = strings.Join(peers, ",")
		g.open(m)
	}
	g.leader()
	return g
}

func (g *group) open(m *member) {
	g.t.Helper()
	n, err := cluster.Open(m.cfg)
	if err != nil {
		g.t.Fatal(err)
	}
	m.node = n
	l, err := net.Listen("tcp", m.cfg.GRPCAddress)
	if err != nil {
		g.t.Fatal(err)
	}
	m.server = grpc.NewServer()
	api.RegisterDatabaseServer(m.server, &server.Server{Node: n})
	go func() { _ = m.server.Serve(l) }()
	m.conn, err = grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		g.t.Fatal(err)
	}
	m.rpc = api.NewDatabaseClient(m.conn)
}

func (g *group) stop(m *member) {
	if m.node == nil {
		return
	}
	m.server.Stop()
	_ = m.conn.Close()
	if err := m.node.Close(); err != nil {
		g.t.Error(err)
	}
	m.node = nil
}

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not converge")
}

func (g *group) leader() *member {
	g.t.Helper()
	var leader *member
	eventually(g.t, func() bool {
		for _, m := range g.members {
			if m.node != nil && m.node.Raft.State() == raft.Leader {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := m.node.Run(ctx, func(context.Context) error { return m.node.Raft.Barrier(time.Second).Error() })
				cancel()
				if err == nil {
					leader = m
					return true
				}
			}
		}
		return false
	})
	return leader
}

func put(t *testing.T, m *member, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := m.rpc.Put(ctx, &api.PutRequest{Key: key, Value: value}); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, m *member, key string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := m.rpc.Get(ctx, &api.Key{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	return v.Value
}

func (g *group) converged(key, value string) {
	eventually(g.t, func() bool {
		for _, m := range g.members {
			if m.node != nil {
				v, ok := m.node.FSM.Get(key)
				if !ok || v != value {
					return false
				}
			}
		}
		return true
	})
}
