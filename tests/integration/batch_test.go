package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"raftdb/api"
)

func TestBatchRPCValidationAndAtomicity(t *testing.T) {
	g := startWithWindow(t, 3, 200*time.Microsecond)
	leader := g.leader()
	ctx := context.Background()
	put := func(entries ...*api.PutRequest) error {
		_, err := leader.rpc.BatchPut(ctx, &api.BatchPutRequest{Entries: entries})
		return err
	}
	if err := put(&api.PutRequest{Key: "duplicate", Value: "first"}, &api.PutRequest{Key: "duplicate", Value: "last"}); err != nil {
		t.Fatal(err)
	}
	response, err := leader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: []string{"duplicate", "absent", "duplicate"}})
	if err != nil || len(response.GetResults()) != 3 {
		t.Fatal(response, err)
	}
	if !response.Results[0].Found || response.Results[0].Value != "last" || response.Results[1].Found || response.Results[2].Value != "last" {
		t.Fatal("ordered/missing/duplicate lookup", response)
	}
	for _, entries := range [][]*api.PutRequest{nil, make([]*api.PutRequest, 129), {{Key: "must-not-appear", Value: "x"}, {Key: "", Value: "invalid"}}, {{Key: "too-big-a", Value: strings.Repeat("a", 133*1024)}, {Key: "too-big-b", Value: strings.Repeat("b", 133*1024)}}} {
		if code := status.Code(put(entries...)); code != codes.InvalidArgument {
			t.Fatal("invalid batch accepted", code)
		}
	}
	for _, keys := range [][]string{nil, make([]string, 129), {""}} {
		_, err := leader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: keys})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid read accepted", err)
		}
	}
	if result, err := leader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: []string{"must-not-appear", "too-big-a"}}); err != nil || result.Results[0].Found || result.Results[1].Found {
		t.Fatal("invalid write partially applied", result, err)
	}
	entries := make([]*api.PutRequest, 128)
	keys := make([]string, 128)
	for i := range entries {
		keys[i] = fmt.Sprint("boundary-", i)
		entries[i] = &api.PutRequest{Key: keys[i], Value: "ok"}
	}
	if err := put(entries...); err != nil {
		t.Fatal("maximum API batch", err)
	}
	result, err := leader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: keys})
	if err != nil || len(result.GetResults()) != 128 {
		t.Fatal(result, err)
	}
	for _, item := range result.Results {
		if !item.Found || item.Value != "ok" {
			t.Fatal(item)
		}
	}
	before := leader.node.Raft.LastIndex()
	for i := 0; i < 20; i++ {
		if _, err := leader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: keys}); err != nil {
			t.Fatal(err)
		}
	}
	if leader.node.Raft.LastIndex() != before {
		t.Fatal("batch reads grew Raft log")
	}
	largeKeys := make([]string, 5)
	for i := range largeKeys {
		largeKeys[i] = fmt.Sprint("large-", i)
		if _, err := leader.rpc.Put(ctx, &api.PutRequest{Key: largeKeys[i], Value: strings.Repeat("x", 220*1024)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := leader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: largeKeys}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("read response limit", err)
	}
	if err := put(&api.PutRequest{Key: "atomic-a", Value: "0"}, &api.PutRequest{Key: "atomic-b", Value: "0"}); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 60; i++ {
			v := fmt.Sprint(i)
			if err := put(&api.PutRequest{Key: "atomic-a", Value: v}, &api.PutRequest{Key: "atomic-b", Value: v}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			r, err := leader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: []string{"atomic-a", "atomic-b"}})
			if err != nil {
				t.Error(err)
				return
			}
			if r.Results[0].Value != r.Results[1].Value {
				t.Error("torn batch read", r)
				return
			}
		}
	}()
	workers.Wait()
	// Atomic batches survive snapshot/restart and a new elected leader.
	if _, err := leader.rpc.Snapshot(ctx, &api.Empty{}); err != nil {
		t.Fatal(err)
	}
	g.stop(leader)
	newLeader := g.leader()
	r, err := newLeader.rpc.BatchGet(ctx, &api.BatchGetRequest{Keys: []string{"atomic-a", "atomic-b"}})
	if err != nil || r.Results[0].Value != "59" || r.Results[1].Value != "59" {
		t.Fatal("new leader lost committed batch", r, err)
	}
	g.open(leader)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if value, ok := leader.node.FSM.Get("atomic-b"); ok && value == "59" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("restarted follower did not catch up")
}
