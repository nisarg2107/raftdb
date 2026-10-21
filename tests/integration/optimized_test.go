package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"raftdb/internal/store"
)

func TestReadIndexAndConcurrentWriteBatches(t *testing.T) {
	for _, pipeline := range []int{1, 8} {
		t.Run(fmt.Sprintf("pipeline-%d", pipeline), func(t *testing.T) { checkReadIndexAndBatches(t, pipeline) })
	}
}
func checkReadIndexAndBatches(t *testing.T, pipeline int) {
	g := startWithWindow(t, 3, 200*time.Microsecond, pipeline)
	leader := g.leader()
	var workers sync.WaitGroup
	for i := 0; i < 64; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			err := leader.node.Write(context.Background(), store.Command{Op: "put", Key: fmt.Sprint("batch-", i), Value: "committed"})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	workers.Wait()
	if _, _, err := leader.node.Read(context.Background(), "batch-0"); err != nil {
		t.Fatal(err)
	}
	before := leader.node.Raft.LastIndex()
	for i := 0; i < 64; i++ {
		value, found, err := leader.node.Read(context.Background(), fmt.Sprint("batch-", i))
		if err != nil || !found || value != "committed" {
			t.Fatal(value, found, err)
		}
	}
	if leader.node.Raft.LastIndex() != before {
		t.Fatal("read-only traffic appended new log entries")
	}
	// Public administrative barriers create index gaps not delivered to the FSM.
	if err := leader.node.Raft.Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := leader.node.Read(context.Background(), "batch-0"); err != nil {
		t.Fatal("read stalled on barrier gap", err)
	}
}
