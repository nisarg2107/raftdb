package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"raftdb/internal/config"
	"raftdb/internal/store"
)

func TestWeightedAdmissionAndCanceledWriteLifetime(t *testing.T) {
	n := &Node{slots: semaphore.NewWeighted(128), Config: config.Config{OperationTimeout: time.Second}}
	n.initWork()
	defer close(n.stop)
	if err := n.acquire(context.Background(), 128); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := n.acquire(ctx, 128); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if n.slots.TryAcquire(1) {
		t.Fatal("cancellation released another batch's capacity")
	}
	commands := make([]store.Command, 128)
	r := &writeRequest{commands: commands, done: make(chan struct{})}
	// Completion, rather than RPC cancellation, owns release of submitted writes.
	n.finishWrites([]*writeRequest{r}, nil)
	if !n.slots.TryAcquire(128) {
		t.Fatal("whole weighted capacity was not released")
	}
	n.slots.Release(128)
}

func TestIncomingDeadlineIsReused(t *testing.T) {
	n := &Node{Config: config.Config{OperationTimeout: time.Second}}
	parent, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	child, release := n.operationContext(parent)
	defer release()
	if child != parent {
		t.Fatal("unnecessary child context")
	}
}
