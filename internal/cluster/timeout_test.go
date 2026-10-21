package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"raftdb/internal/config"
)

func TestRunDeadlineAndBoundedWaiters(t *testing.T) {
	n := &Node{Config: config.Config{OperationTimeout: 30 * time.Millisecond}, slots: semaphore.NewWeighted(1)}
	defer func() { close(n.stop) }()
	release := make(chan struct{})
	defer close(release)
	var starts atomic.Int32
	blocked := func(context.Context) error { starts.Add(1); <-release; return nil }
	if err := n.Run(context.Background(), blocked); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := n.Run(context.Background(), blocked); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if starts.Load() != 1 {
		t.Fatal("outstanding futures exceeded bound")
	}
}
