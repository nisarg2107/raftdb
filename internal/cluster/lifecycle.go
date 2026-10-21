package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/semaphore"

	"raftdb/internal/config"
	"raftdb/internal/store"
	"raftdb/internal/telemetry"
)

// Open reuses durable metadata and bootstraps only the designated first node,
// with the full static voter set. Fresh followers learn membership from Raft.
func Open(c config.Config) (_ *Node, err error) {
	peers, err := c.Validate()
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(c.DataDir, 0700); err != nil {
		return nil, err
	}
	logs, err := openStorage(c.DataDir, c.Storage)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = logs.Close()
		}
	}()
	logger := hclog.New(&hclog.LoggerOptions{Name: c.ID, Level: hclog.LevelFromString(c.LogLevel), JSONFormat: true})
	snapshots, err := raft.NewFileSnapshotStoreWithLogger(c.DataDir, 3, logger)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", c.RaftAddress)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransportWithLogger(c.RaftAddress, addr, 8, 2*time.Second, logger)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = transport.Close()
		}
	}()
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(c.ID)
	// Let Raft group queued log appends into durable transactions. Buffered
	// Apply loses its processing-timeout guarantee; Run still bounds the RPC,
	// and success still waits for the committed/apply future.
	rc.BatchApplyCh = c.BatchApply
	rc.HeartbeatTimeout = c.Heartbeat
	rc.ElectionTimeout = c.Election
	rc.LeaderLeaseTimeout = c.Lease
	rc.SnapshotInterval = c.SnapshotInterval
	rc.SnapshotThreshold = c.SnapshotThreshold
	rc.TrailingLogs = c.TrailingLogs
	rc.Logger = logger
	exists, err := raft.HasExistingState(logs, logs, snapshots)
	if err != nil {
		return nil, err
	}
	observations := telemetry.New()
	logs = &measuredStorage{raftStorage: logs, metrics: observations}
	fsm := store.New()
	fsm.Telemetry = observations
	cache, err := raft.NewLogCache(256, logs)
	if err != nil {
		return nil, err
	}
	r, err := raft.NewRaft(rc, fsm, cache, logs, snapshots, transport)
	if err != nil {
		return nil, err
	}
	n := &Node{
		Raft:      r,
		FSM:       fsm,
		Transport: transport,
		Logs:      logs,
		Config:    c,
		Peers:     peers,
		slots:     semaphore.NewWeighted(MaxOutstandingOperations),
		Telemetry: observations,
	}
	if c.Bootstrap && !exists {
		var servers []raft.Server
		for _, p := range peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Raft), Suffrage: raft.Voter})
		}
		if err = r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			_ = r.Shutdown().Error()
			return nil, err
		}
		slog.Info("cluster bootstrapped", "voters", len(servers))
	}
	return n, nil
}

func (n *Node) Close() error {
	n.initWork()
	// Exclude enqueueing while closing the signal used to drain pending writes.
	n.stopOnce.Do(func() {
		n.enqueueMu.Lock()
		close(n.stop)
		n.enqueueMu.Unlock()
	})
	err := n.Raft.Shutdown().Error()
	if n.Transport != nil {
		_ = n.Transport.Close()
	}
	if e := n.Logs.Close(); err == nil {
		err = e
	}
	return err
}

func (n *Node) Snapshot(ctx context.Context) error {
	return n.Run(ctx, func(context.Context) error {
		err := n.Raft.Snapshot().Error()
		if errors.Is(err, raft.ErrNothingNewToSnapshot) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		return nil
	})
}
