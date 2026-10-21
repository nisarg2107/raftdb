package cluster

import (
	"context"
	"strconv"

	"github.com/hashicorp/raft"
)

// Status is database state, independent of its protobuf representation.
type Status struct {
	ID, State, LeaderID, LeaderGRPC                              string
	Term, FirstLogIndex, LastLogIndex, AppliedIndex, CommitIndex uint64
	Members                                                      []raft.Server
}

func (n *Node) Status(ctx context.Context) (Status, error) {
	out := make(chan Status, 1)
	err := n.Run(ctx, func(context.Context) error {
		r := n.Raft
		stats := r.Stats()
		id, leader := n.Leader()
		term, _ := strconv.ParseUint(stats["term"], 10, 64)
		first, _ := n.Logs.FirstIndex()
		info := Status{
			ID: n.Config.ID, State: r.State().String(), Term: term,
			LeaderID: id, LeaderGRPC: leader, FirstLogIndex: first,
			LastLogIndex: r.LastIndex(), AppliedIndex: r.AppliedIndex(), CommitIndex: r.CommitIndex(),
		}
		configuration := r.GetConfiguration()
		if err := configuration.Error(); err != nil {
			return err
		}
		info.Members = configuration.Configuration().Servers
		out <- info
		return nil
	})
	if err != nil {
		return Status{}, err
	}
	return <-out, nil
}

// LocalRead is explicitly stale; callers needing linearizability use Read.
func (n *Node) LocalRead(key string) (string, bool) { return n.FSM.Get(key) }

// Serving reports process liveness, without promising quorum or read readiness.
func (n *Node) Serving() bool { return n.Raft.State() != raft.Shutdown }
