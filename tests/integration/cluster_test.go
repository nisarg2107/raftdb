package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"raftdb/api"
)

func TestBaselineLinearizableRead(t *testing.T) {
	g := startWithWindow(t, 3, 0)
	leader := g.leader()
	put(t, leader, "baseline", "consistent")
	if get(t, leader, "baseline") != "consistent" {
		t.Fatal("baseline read mismatch")
	}
}

func TestConcurrentHistoryIsLinearizable(t *testing.T) {
	g := start(t, 3)
	leader := g.leader()
	put(t, leader, "register", "initial")
	var history []operation
	var mu sync.Mutex
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < 3; round++ {
				op := operation{start: time.Now(), write: round%2 == 0, value: fmt.Sprintf("%d-%d", worker, round)}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				var err error
				if op.write {
					_, err = leader.rpc.Put(ctx, &api.PutRequest{Key: "register", Value: op.value})
				} else {
					var v *api.Value
					v, err = leader.rpc.Get(ctx, &api.Key{Key: "register"})
					if err == nil {
						op.value = v.Value
					}
				}
				cancel()
				op.end = time.Now()
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
			}
		}(worker)
	}
	wg.Wait()
	if len(history) != 9 || !linearizable(history) {
		t.Fatal("non-linearizable history", history)
	}
	// The checker must reject an impossible completed-read result.
	if linearizable([]operation{{start: time.Now(), end: time.Now(), value: "never-written"}}) {
		t.Fatal("checker accepted impossible history")
	}
}

func TestFiveNodeFailuresAndSnapshotCatchup(t *testing.T) {
	g := start(t, 5)
	old := g.leader()
	put(t, old, "committed", "safe")
	g.converged("committed", "safe")
	var stopped []*member
	for _, m := range g.members {
		if m != old && len(stopped) < 2 {
			g.stop(m)
			stopped = append(stopped, m)
		}
	}
	put(t, old, "three", "quorum")
	g.stop(old) // two live voters cannot commit or read linearly.
	for _, m := range g.members {
		if m.node != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := m.rpc.Put(ctx, &api.PutRequest{Key: "unsafe", Value: "bad"})
			cancel()
			if err == nil {
				t.Fatal("minority acknowledged write")
			}
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			_, err = m.rpc.Get(ctx, &api.Key{Key: "committed"})
			cancel()
			if err == nil {
				t.Fatal("minority served read")
			}
		}
	}
	g.open(stopped[0])
	newLeader := g.leader()
	put(t, newLeader, "recovered", "yes")
	if get(t, newLeader, "committed") != "safe" {
		t.Fatal("committed data lost")
	}
	for i := 0; i < 96; i++ {
		put(t, newLeader, fmt.Sprintf("snap-%d", i), fmt.Sprint(i))
	}
	if err := newLeader.node.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := newLeader.node.Logs.FirstIndex()
	if err != nil || first <= 1 {
		t.Fatal("compaction did not advance", first, err)
	}
	g.open(old)
	g.open(stopped[1])
	g.converged("recovered", "yes")
	eventually(t, func() bool {
		for _, m := range g.members {
			for i := 0; i < 96; i++ {
				v, ok := m.node.FSM.Get(fmt.Sprintf("snap-%d", i))
				if !ok || v != fmt.Sprint(i) {
					return false
				}
			}
		}
		return true
	})
	for _, m := range g.members {
		if _, ok := m.node.FSM.Get("unsafe"); ok {
			t.Fatal("rejected minority write applied")
		}
	}
}

func TestLeaderCrashRecovery(t *testing.T) {
	g := start(t, 5)
	old := g.leader()
	put(t, old, "before", "safe")
	g.converged("before", "safe")
	g.stop(old)
	next := g.leader()
	put(t, next, "after", "safe")
	if get(t, next, "before") != "safe" {
		t.Fatal("lost data")
	}
	g.open(old)
	g.converged("after", "safe")
}

func TestSingleNodePersistenceAndAPI(t *testing.T) {
	g := start(t, 1)
	m := g.leader()
	put(t, m, "a", "b")
	if get(t, m, "a") != "b" {
		t.Fatal("bad read")
	}
	g.stop(m)
	g.open(m)
	g.leader()
	if get(t, m, "a") != "b" {
		t.Fatal("log-only restart lost value")
	}
	ctx := context.Background()
	if _, err := m.rpc.Get(ctx, &api.Key{Key: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatal(err)
	}
	if _, err := m.rpc.Put(ctx, &api.PutRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := m.rpc.Snapshot(ctx, &api.Empty{}); err != nil {
		t.Fatal(err)
	}
	g.stop(m)
	g.open(m)
	g.leader()
	if get(t, m, "a") != "b" {
		t.Fatal("lost persisted value")
	}
	if _, err := m.rpc.Delete(ctx, &api.Key{Key: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.rpc.Get(ctx, &api.Key{Key: "a"}); status.Code(err) != codes.NotFound {
		t.Fatal(err)
	}
}

func TestThreeNodeReplication(t *testing.T) {
	g := start(t, 3)
	m := g.leader()
	put(t, m, "a", "b")
	g.converged("a", "b")
	for _, f := range g.members {
		if f != m {
			if _, err := f.rpc.Get(context.Background(), &api.Key{Key: "a"}); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("follower served consistent read", err)
			}
		}
	}
}

// This exhaustive single-register checker respects real-time precedence and
// searches legal sequential orderings. Small histories keep it easy to audit.
func linearizable(history []operation) bool {
	var search func(uint64, string) bool
	search = func(done uint64, current string) bool {
		if done == (uint64(1)<<len(history))-1 {
			return true
		}
		for i, op := range history {
			if done&(1<<i) != 0 {
				continue
			}
			eligible := true
			for j, prior := range history {
				if done&(1<<j) == 0 && prior.end.Before(op.start) {
					eligible = false
					break
				}
			}
			if !eligible {
				continue
			}
			next := current
			if op.write {
				next = op.value
			} else if op.value != current {
				continue
			}
			if search(done|(1<<i), next) {
				return true
			}
		}
		return false
	}
	return search(0, "initial")
}

type operation struct {
	start, end time.Time
	write      bool
	value      string
}
