package cluster

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
)

const MaxOutstandingOperations = 2048

func noCancel() {}

// Incoming gRPC deadlines commonly already precede our operation deadline.
func (n *Node) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= n.Config.OperationTimeout {
		return parent, noCancel
	}
	return context.WithTimeout(parent, n.Config.OperationTimeout)
}

func (n *Node) acquire(ctx context.Context, weight int64) error {
	select {
	case <-n.stop:
		return raft.ErrRaftShutdown
	default:
	}
	if err := n.slots.Acquire(ctx, weight); err != nil {
		return err
	}
	select {
	case <-n.stop:
		n.slots.Release(weight)
		return raft.ErrRaftShutdown
	default:
		return nil
	}
}

// Run bounds both the response and outstanding waits. Raft enqueue timeouts do
// not bound Future.Error(). A timed-out write has an unknown outcome and may
// later commit; callers must never interpret that timeout as a rollback.
func (n *Node) Run(parent context.Context, operation func(context.Context) error) error {
	n.initWork()
	ctx, cancel := n.operationContext(parent)
	defer cancel()
	if err := n.acquire(ctx, 1); err != nil {
		return err
	}
	result := make(chan error, 1)
	go func() { defer n.slots.Release(1); result <- operation(ctx) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
