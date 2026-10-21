package cluster

import (
	"context"
	"time"

	"github.com/hashicorp/raft"

	"raftdb/internal/store"
)

// Several callers wait on the same done channel for one fresh quorum check.
// This shares coordination; it does not cache an old authority check.
type readBatch struct {
	done chan struct{}
	err  error
	term uint64
}

// A committed application entry once per leader term establishes that this
// leader knows the committed prefix, including legacy snapshot state. Subsequent
// reads need no new log entry. Term changes invalidate this cached initialization.
func (n *Node) ensureLeaderFence(term uint64) error {
	n.leaderFenceMu.Lock()
	defer n.leaderFenceMu.Unlock()
	if n.Raft.State() != raft.Leader || n.Raft.CurrentTerm() != term {
		return raft.ErrNotLeader
	}
	if n.readLeaderTerm == term {
		return nil
	}
	data, _ := store.Encode(store.Command{Op: "fence"})
	future := n.Raft.Apply(data, n.Config.OperationTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	if err, ok := future.Response().(error); ok {
		return err
	}
	if n.Raft.State() != raft.Leader || n.Raft.CurrentTerm() != term {
		return raft.ErrLeadershipLost
	}
	n.readLeaderTerm = term
	return nil
}

func (n *Node) verifyAndDrain(ctx context.Context) (uint64, error) {
	n.ReadRounds.Add(1)
	term := n.Raft.CurrentTerm()
	if err := n.ensureLeaderFence(term); err != nil {
		return 0, err
	}
	// Capture BEFORE the fresh quorum check, after this group's requests arrived.
	target := n.Raft.CommitIndex()
	if err := n.Raft.VerifyLeader().Error(); err != nil {
		return 0, err
	}
	if n.Raft.CurrentTerm() != term || n.Raft.State() != raft.Leader {
		return 0, raft.ErrLeadershipLost
	}
	// BatchingFSM observes commands/configurations, but not no-ops or barriers.
	// Skip only those non-mutating suffix entries. Fail closed on a missing log
	// unless concurrent application already covers the required prefix.
	for target > n.FSM.AppliedIndex() {
		var log raft.Log
		if err := n.Logs.GetLog(target, &log); err != nil {
			if n.FSM.AppliedIndex() >= target {
				break
			}
			return 0, err
		}
		if log.Type == raft.LogCommand || log.Type == raft.LogConfiguration {
			break
		}
		target--
	}
	if err := n.FSM.WaitApplied(ctx, target); err != nil {
		return 0, err
	}
	return term, nil
}

func (n *Node) readFence(ctx context.Context) (uint64, error) {
	n.readMu.Lock()
	batch := n.pendingRead
	if batch == nil {
		batch = &readBatch{done: make(chan struct{})}
		n.pendingRead = batch
		go n.completeReadBatch(batch)
	}
	n.readMu.Unlock()
	select {
	case <-batch.done:
		return batch.term, batch.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stop:
		return 0, raft.ErrRaftShutdown
	}
}

func (n *Node) completeReadBatch(batch *readBatch) {
	ctx, cancel := context.WithTimeout(context.Background(), n.Config.OperationTimeout)
	defer cancel()
	if window := n.Config.ReadBatchWindow; window > 0 {
		timer := time.NewTimer(window)
		select {
		case <-timer.C:
		case <-n.stop:
			timer.Stop()
		}
	}
	// Canceled callers cannot leave more than four unresolved quorum rounds.
	select {
	case n.readRounds <- struct{}{}:
		defer func() { <-n.readRounds }()
	case <-ctx.Done():
		batch.err = ctx.Err()
	case <-n.stop:
		batch.err = raft.ErrRaftShutdown
	}
	// Detach BEFORE verification. Later requests must use a fresh quorum round.
	n.readMu.Lock()
	n.pendingRead = nil
	n.readMu.Unlock()
	if batch.err == nil {
		batch.term, batch.err = n.verifyAndDrain(ctx)
	}
	close(batch.done)
}

// Reads share a fresh quorum check and wait for actual FSM application. Only
// the first read in a leader term needs a committed application fence.
func (n *Node) Read(ctx context.Context, key string) (string, bool, error) {
	n.initWork()
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	if err := n.acquire(ctx, 1); err != nil {
		return "", false, err
	}
	defer n.slots.Release(1)
	term, err := n.readFence(ctx)
	if err != nil {
		return "", false, err
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	v, ok := n.FSM.Get(key)
	if n.Raft.State() != raft.Leader || n.Raft.CurrentTerm() != term {
		return "", false, raft.ErrLeadershipLost
	}
	return v, ok, nil
}

func (n *Node) ReadMany(ctx context.Context, keys []string) ([]store.Lookup, error) {
	if err := store.ValidateReadBatch(keys); err != nil {
		return nil, err
	}
	n.initWork()
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	weight := int64(len(keys))
	if err := n.acquire(ctx, weight); err != nil {
		return nil, err
	}
	defer n.slots.Release(weight)
	term, err := n.readFence(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results, err := n.FSM.GetMany(keys)
	if err != nil {
		return nil, err
	}
	if n.Raft.State() != raft.Leader || n.Raft.CurrentTerm() != term {
		return nil, raft.ErrLeadershipLost
	}
	return results, nil
}
