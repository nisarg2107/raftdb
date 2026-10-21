package cluster

import (
	"context"

	"github.com/hashicorp/raft"
	"golang.org/x/sync/semaphore"

	"raftdb/internal/store"
)

const writeBatchBytes = 256 * 1024

// A request holds either one command (the unary fast path) or one caller batch.
// The commit waiter owns it after enqueueing, even if the RPC stops waiting.
// Writing err before closing done publishes the result safely to the caller.
type writeRequest struct {
	command  store.Command
	commands []store.Command
	done     chan struct{}
	err      error
}

func (r *writeRequest) weight() int64 {
	if len(r.commands) != 0 {
		return int64(len(r.commands))
	}
	return 1
}
func (r *writeRequest) appendTo(commands []store.Command) []store.Command {
	if len(r.commands) != 0 {
		return append(commands, r.commands...)
	}
	return append(commands, r.command)
}
func (r *writeRequest) encodedSize() int {
	if len(r.commands) == 0 {
		return 7 + len(r.command.Key) + len(r.command.Value)
	}
	size := 0
	for _, c := range r.commands {
		size += 7 + len(c.Key) + len(c.Value)
	}
	return size
}

// sync.Once starts one collector and creates the shared capacity/shutdown signals.
func (n *Node) initWork() {
	n.initOnce.Do(func() {
		if n.slots == nil {
			n.slots = semaphore.NewWeighted(MaxOutstandingOperations)
		}
		n.stop = make(chan struct{})
		n.writeQueue = make(chan *writeRequest, MaxOutstandingOperations)
		n.readRounds = make(chan struct{}, 4)
		go n.batchWrites()
	})
}

func (n *Node) finishWrites(batch []*writeRequest, err error) {
	for _, request := range batch {
		request.err = err
		close(request.done)
		n.slots.Release(request.weight())
	}
}

// One collector and a configurable bound on unresolved batch futures replace a future
// goroutine per write. Timed-out writes retain slots until their batches resolve.
func (n *Node) batchWrites() {
	pipeline := n.Config.WritePipeline
	if pipeline == 0 {
		pipeline = 8
	}
	inflight := make(chan struct{}, pipeline)
	var carried *writeRequest
	for {
		var first *writeRequest
		if carried != nil {
			first, carried = carried, nil
		} else {
			select {
			case first = <-n.writeQueue:
			case <-n.stop:
				for {
					select {
					case request := <-n.writeQueue:
						n.finishWrites([]*writeRequest{request}, raft.ErrRaftShutdown)
					default:
						return
					}
				}
			}
		}
		select {
		case inflight <- struct{}{}:
		case <-n.stop:
			n.finishWrites([]*writeRequest{first}, raft.ErrRaftShutdown)
			continue
		}
		batch := []*writeRequest{first}
		size := 6 + first.encodedSize()
		operations := int(first.weight())
	collect:
		for operations < store.MaxBatchCommands {
			select {
			case request := <-n.writeQueue:
				entrySize := request.encodedSize()
				if size+entrySize > writeBatchBytes || operations+int(request.weight()) > store.MaxBatchCommands {
					carried = request
					break collect
				}
				batch = append(batch, request)
				size += entrySize
				operations += int(request.weight())
			default:
				break collect
			case <-n.stop:
				break collect
			}
		}
		commands := make([]store.Command, 0, operations)
		for _, request := range batch {
			commands = request.appendTo(commands)
		}
		data, err := store.EncodeBatch(commands)
		if err != nil {
			n.finishWrites(batch, err)
			<-inflight
			continue
		}
		// Enqueue and wait asynchronously so collection overlaps disk/replication.
		// Concurrent batches may reorder; each batch preserves operation order.
		// Real-time order is preserved because replies still wait for apply.
		go func(batch []*writeRequest, data []byte) {
			future := n.Raft.Apply(data, n.Config.OperationTimeout)
			err := future.Error()
			if err == nil {
				err, _ = future.Response().(error)
			}
			if err == nil && n.Telemetry != nil {
				n.Telemetry.BatchOperations.Observe(float64(operations))
				n.Telemetry.BatchBytes.Observe(float64(len(data)))
			}
			n.finishWrites(batch, err)
			<-inflight
		}(batch, data)
	}
}

func (n *Node) Write(ctx context.Context, c store.Command) error {
	if err := c.Validate(); err != nil {
		return err
	}
	return n.enqueueWrite(ctx, &writeRequest{command: c, done: make(chan struct{})})
}

func (n *Node) WriteMany(ctx context.Context, commands []store.Command) error {
	if err := store.ValidateWriteBatch(commands); err != nil {
		return err
	}
	// The caller may return on cancellation before the durable future completes.
	owned := append([]store.Command(nil), commands...)
	return n.enqueueWrite(ctx, &writeRequest{commands: owned, done: make(chan struct{})})
}

func (n *Node) enqueueWrite(ctx context.Context, request *writeRequest) error {
	n.initWork()
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	if n.Raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	if err := n.acquire(ctx, request.weight()); err != nil {
		return err
	}
	n.enqueueMu.RLock()
	select {
	case <-n.stop:
		n.enqueueMu.RUnlock()
		n.slots.Release(request.weight())
		return raft.ErrRaftShutdown
	case <-ctx.Done():
		n.enqueueMu.RUnlock()
		n.slots.Release(request.weight())
		return ctx.Err()
	default:
	}
	// Queue capacity equals the outstanding slot bound, so our owned slot
	// guarantees space. The lock prevents enqueue after shutdown drains it.
	n.writeQueue <- request
	n.enqueueMu.RUnlock()
	select {
	case <-request.done:
		return request.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
