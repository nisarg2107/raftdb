package store

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

type sink struct {
	bytes.Buffer
	closed, canceled bool
	fail             bool
}

func (s *sink) ID() string    { return "test" }
func (s *sink) Close() error  { s.closed = true; return nil }
func (s *sink) Cancel() error { s.canceled = true; return nil }
func (s *sink) Write(b []byte) (int, error) {
	if s.fail {
		return 0, io.ErrShortWrite
	}
	return s.Buffer.Write(b)
}
func apply(t *testing.T, f *FSM, c Command) {
	t.Helper()
	b, err := Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Apply(&raft.Log{Data: b}); err != nil {
		t.Fatal(err)
	}
}
func TestOperationsAndCommands(t *testing.T) {
	f := New()
	if _, ok := f.Get("missing"); ok {
		t.Fatal("missing found")
	}
	for _, v := range []string{"first", "overwritten", ""} {
		c := Command{Op: "put", Key: "key", Value: v}
		b, err := Encode(c)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(b)
		if err != nil || decoded != c {
			t.Fatal(decoded, err)
		}
		apply(t, f, c)
		if got, ok := f.Get("key"); !ok || got != v {
			t.Fatal(got, ok)
		}
	}
	apply(t, f, Command{Op: "delete", Key: "key"})
	if _, ok := f.Get("key"); ok {
		t.Fatal("delete failed")
	}
	for _, c := range []Command{{Op: "bad", Key: "a"}, {Op: "put"}, {Op: "put", Key: "x", Value: string(make([]byte, 256*1024+1))}} {
		if _, err := Encode(c); err == nil {
			t.Fatal("invalid accepted")
		}
	}
	if _, err := Decode([]byte("invalid")); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	if f.Apply(&raft.Log{Data: []byte(`{"op":"bad"}`)}) == nil {
		t.Fatal("corrupt log accepted")
	}
}
func TestSnapshotIsolationRestoreAndFailure(t *testing.T) {
	f := New()
	apply(t, f, Command{Op: "put", Key: "a", Value: "before"})
	s, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	apply(t, f, Command{Op: "put", Key: "a", Value: "after"})
	out := &sink{}
	if err := s.Persist(out); err != nil || !out.closed {
		t.Fatal(err)
	}
	g := New()
	apply(t, g, Command{Op: "put", Key: "extra"})
	if err := g.Restore(io.NopCloser(bytes.NewReader(out.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if v, _ := g.Get("a"); v != "before" {
		t.Fatal(v)
	}
	if _, ok := g.Get("extra"); ok {
		t.Fatal("restore merged instead of replacing")
	}
	second := &sink{}
	if err := s.Persist(second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), second.Bytes()) {
		t.Fatal("nondeterministic snapshot")
	}
	bad := &sink{fail: true}
	if err := s.Persist(bad); err == nil || !bad.canceled {
		t.Fatal("failed sink not canceled")
	}
	for _, raw := range []string{`{`, `{"version":2,"values":{}}`, `{"version":1,"values":null}`, `{"version":1,"values":{"":"bad"}}`, `{"version":1,"values":{}} junk`} {
		if err := g.Restore(io.NopCloser(bytes.NewBufferString(raw))); err == nil {
			t.Fatal("accepted", raw)
		}
	}
	if v, _ := g.Get("a"); v != "before" {
		t.Fatal("invalid restore mutated state")
	}
}
func TestConcurrentFSM(t *testing.T) {
	f := New()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				b, _ := Encode(Command{Op: "put", Key: "a", Value: "b"})
				f.Apply(&raft.Log{Data: b})
				f.Get("a")
				s, err := f.Snapshot()
				if err != nil {
					t.Error(err)
					return
				}
				s.Release()
			}
		}()
	}
	wg.Wait()
}
