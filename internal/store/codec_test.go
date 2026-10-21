package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestMaximumBoundedBatch(t *testing.T) {
	commands := make([]Command, MaxBatchCommands)
	for i := range commands {
		commands[i] = Command{Op: "put", Key: fmt.Sprintf("key-%d", i), Value: "committed"}
	}
	data, err := EncodeBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	f := New()
	if err := f.Apply(&raft.Log{Index: 42, Data: data}); err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		if value, ok := f.Get(command.Key); !ok || value != command.Value {
			t.Fatal(command.Key, value, ok)
		}
	}
	if _, err := EncodeBatch(append(commands, commands[0])); err == nil {
		t.Fatal("unbounded command count accepted")
	}
}

func TestBatchCodecCompatibilityAndAtomicFailure(t *testing.T) {
	legacy := []byte(`{"op":"put","key":"legacy","value":"saved"}`)
	if c, err := Decode(legacy); err != nil || c.Value != "saved" {
		t.Fatal(c, err)
	}
	commands := []Command{{Op: "put", Key: "a", Value: "first"}, {Op: "delete", Key: "a"}, {Op: "put", Key: "b", Value: "last"}, {Op: "fence"}}
	data, err := EncodeBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatch(data)
	if err != nil || len(decoded) != len(commands) {
		t.Fatal(decoded, err)
	}
	for i := range commands {
		if commands[i] != decoded[i] {
			t.Fatal(decoded)
		}
	}
	f := New()
	if err := f.Apply(&raft.Log{Index: 7, Data: data}); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Get("a"); ok {
		t.Fatal("delete lost")
	}
	if v, _ := f.Get("b"); v != "last" {
		t.Fatal(v)
	}
	for length := 0; length < len(data); length++ {
		if _, err := DecodeBatch(data[:length]); err == nil {
			t.Fatalf("accepted truncated batch %d", length)
		}
	}
	if _, err := DecodeBatch(append(append([]byte{}, data...), 0)); err == nil {
		t.Fatal("accepted trailing data")
	}
	corrupt := append([]byte{}, data...)
	corrupt[len(corrupt)-7] = 99 // corrupt final fence, after otherwise valid commands
	g := New()
	if err := g.Apply(&raft.Log{Index: 1, Data: corrupt}); err == nil {
		t.Fatal("accepted corrupt operation")
	}
	if _, ok := g.Get("b"); ok {
		t.Fatal("partially applied invalid batch")
	}
}

func TestApplicationFenceAndSnapshotCompatibility(t *testing.T) {
	f := New()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.WaitApplied(ctx, 8) }()
	select {
	case <-done:
		t.Fatal("fence completed before application")
	case <-time.After(5 * time.Millisecond):
	}
	f.ApplyBatch([]*raft.Log{{Index: 8, Type: raft.LogConfiguration}})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	snap, _ := f.Snapshot()
	out := &sink{}
	if err := snap.Persist(out); err != nil {
		t.Fatal(err)
	}
	g := New()
	if err := g.Restore(io.NopCloser(bytes.NewReader(out.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if g.AppliedIndex() != 8 {
		t.Fatal("lost application progress")
	}
	if err := g.Restore(io.NopCloser(bytes.NewBufferString(`{"version":1,"values":{"old":"value"}}`))); err != nil {
		t.Fatal(err)
	}
	if g.AppliedIndex() != 0 {
		t.Fatal("legacy snapshot inherited an unrelated index")
	}
}
