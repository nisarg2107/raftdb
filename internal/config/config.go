package config

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type Peer struct{ ID, Raft, GRPC string }
type Config struct {
	ID             string
	DataDir        string
	RaftAddress    string
	GRPCAddress    string
	MetricsAddress string
	Peers          string
	Storage        string
	Bootstrap      bool
	BatchApply     bool
	WritePipeline  int

	Heartbeat         time.Duration
	Election          time.Duration
	Lease             time.Duration
	OperationTimeout  time.Duration
	SnapshotInterval  time.Duration
	ReadBatchWindow   time.Duration
	SnapshotThreshold uint64
	TrailingLogs      uint64
	LogLevel          string
}

func Default() Config {
	return Config{
		ID: "node1", DataDir: "data/node1", Storage: "bolt", WritePipeline: 8,
		RaftAddress: "127.0.0.1:7001", GRPCAddress: "127.0.0.1:50051", MetricsAddress: "127.0.0.1:9091",
		Peers:     "node1=127.0.0.1:7001=127.0.0.1:50051",
		Heartbeat: time.Second, Election: time.Second, Lease: 500 * time.Millisecond,
		OperationTimeout: 3 * time.Second, SnapshotInterval: 60 * time.Second,
		SnapshotThreshold: 65536, TrailingLogs: 8192, LogLevel: "INFO",
	}
}

// Validate is also called by cluster.Open for callers that construct Config
// directly instead of using the command-line parser. It never opens storage.
func (c Config) Validate() ([]Peer, error) {
	peers, err := ParsePeers(c.Peers)
	if err != nil {
		return nil, err
	}
	found := false
	for _, peer := range peers {
		if peer.ID == c.ID {
			found = true
			if peer.Raft != c.RaftAddress {
				return nil, errors.New("local raft address differs from peer configuration")
			}
		}
	}
	if !found {
		return nil, errors.New("node absent from peers")
	}
	if c.ReadBatchWindow < 0 || c.ReadBatchWindow > 10*time.Millisecond {
		return nil, errors.New("read batch window must be between zero and 10ms")
	}
	if c.WritePipeline < 0 || c.WritePipeline > 32 {
		return nil, errors.New("write pipeline must be 0..32; zero selects 8")
	}
	if c.OperationTimeout <= 0 {
		return nil, errors.New("operation timeout must be positive")
	}
	if c.Storage != "" && c.Storage != "bolt" && c.Storage != "wal" {
		return nil, fmt.Errorf("unknown storage backend %q", c.Storage)
	}
	return peers, nil
}

// ParsePeers rejects ambiguous identities before a node can form a voter set.
func ParsePeers(raw string) ([]Peer, error) {
	var out []Peer
	ids := map[string]bool{}
	addresses := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		fields := strings.Split(strings.TrimSpace(part), "=")
		if len(fields) != 3 || fields[0] == "" {
			return nil, errors.New("peers must be id=raft-host:port=grpc-host:port separated by commas")
		}
		if ids[fields[0]] || addresses[fields[1]] {
			return nil, errors.New("duplicate peer id or raft address")
		}
		ids[fields[0]] = true
		addresses[fields[1]] = true
		for _, addr := range fields[1:] {
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return nil, fmt.Errorf("invalid peer address %q: %w", addr, err)
			}
		}
		out = append(out, Peer{fields[0], fields[1], fields[2]})
	}
	return out, nil
}
