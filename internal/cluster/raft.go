package cluster

import (
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
	"golang.org/x/sync/semaphore"

	"raftdb/internal/config"
	"raftdb/internal/store"
	"raftdb/internal/telemetry"
)

type Node struct {
	Raft      *raft.Raft
	FSM       *store.FSM
	Transport *raft.NetworkTransport
	Logs      raftStorage
	Config    config.Config
	Peers     []config.Peer
	// Admission belongs to underlying work until it resolves, even after RPC cancellation.
	slots     *semaphore.Weighted
	Telemetry *telemetry.Metrics
	// readMu protects pendingRead; detached groups perform fresh quorum checks.
	readMu         sync.Mutex
	pendingRead    *readBatch
	ReadRounds     atomic.Uint64
	initOnce       sync.Once
	stopOnce       sync.Once
	stop           chan struct{}
	writeQueue     chan *writeRequest
	readRounds     chan struct{}
	leaderFenceMu  sync.Mutex
	readLeaderTerm uint64
	// enqueueMu prevents enqueueing after shutdown has started draining the queue.
	enqueueMu sync.RWMutex
}

func (n *Node) Leader() (string, string) {
	_, id := n.Raft.LeaderWithID()
	for _, p := range n.Peers {
		if p.ID == string(id) {
			return string(id), p.GRPC
		}
	}
	return string(id), ""
}
