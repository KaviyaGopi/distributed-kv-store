package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func applyCmd(t *testing.T, f *KVFSM, cmd Command) interface{} {
	t.Helper()
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return f.Apply(&raft.Log{Data: data})
}

func TestApplyPutAndDelete(t *testing.T) {
	f := NewKVFSM()

	if res := applyCmd(t, f, Command{Op: OpPut, Key: "a", Value: "1"}); res != nil {
		t.Fatalf("unexpected apply error: %v", res)
	}
	if v, ok := f.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", v, ok)
	}

	if res := applyCmd(t, f, Command{Op: OpPut, Key: "a", Value: "2"}); res != nil {
		t.Fatalf("unexpected apply error: %v", res)
	}
	if v, _ := f.Get("a"); v != "2" {
		t.Fatalf("Get(a) after overwrite = %q; want 2", v)
	}

	if res := applyCmd(t, f, Command{Op: OpDelete, Key: "a"}); res != nil {
		t.Fatalf("unexpected apply error: %v", res)
	}
	if _, ok := f.Get("a"); ok {
		t.Fatalf("Get(a) after delete: found, want not found")
	}
}

func TestApplyUnknownOp(t *testing.T) {
	f := NewKVFSM()
	res := applyCmd(t, f, Command{Op: "BOGUS", Key: "a"})
	if res == nil {
		t.Fatal("expected error for unknown op, got nil")
	}
	if _, ok := res.(error); !ok {
		t.Fatalf("expected error type, got %T", res)
	}
}

func TestApplyBadPayload(t *testing.T) {
	f := NewKVFSM()
	res := f.Apply(&raft.Log{Data: []byte("not json")})
	if res == nil {
		t.Fatal("expected error for malformed payload, got nil")
	}
}

func TestSnapshotRestore(t *testing.T) {
	f := NewKVFSM()
	applyCmd(t, f, Command{Op: OpPut, Key: "a", Value: "1"})
	applyCmd(t, f, Command{Op: OpPut, Key: "b", Value: "2"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var buf bytes.Buffer
	sink := &fakeSink{Buffer: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if !sink.closed {
		t.Fatal("expected sink to be closed")
	}

	restored := NewKVFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if v, ok := restored.Get("a"); !ok || v != "1" {
		t.Fatalf("restored Get(a) = %q, %v; want 1, true", v, ok)
	}
	if v, ok := restored.Get("b"); !ok || v != "2" {
		t.Fatalf("restored Get(b) = %q, %v; want 2, true", v, ok)
	}

	// Original FSM must be unaffected by mutations made after Snapshot().
	applyCmd(t, f, Command{Op: OpPut, Key: "c", Value: "3"})
	if _, ok := restored.Get("c"); ok {
		t.Fatal("restored FSM should not see writes made to the original after snapshot")
	}
}

func TestSnapshotIsIndependentOfLiveState(t *testing.T) {
	f := NewKVFSM()
	applyCmd(t, f, Command{Op: OpPut, Key: "a", Value: "1"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Mutate live FSM after taking the snapshot handle.
	applyCmd(t, f, Command{Op: OpPut, Key: "a", Value: "mutated"})

	var buf bytes.Buffer
	if err := snap.Persist(&fakeSink{Buffer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	var persisted map[string]string
	if err := json.Unmarshal(buf.Bytes(), &persisted); err != nil {
		t.Fatalf("unmarshal persisted snapshot: %v", err)
	}
	if persisted["a"] != "1" {
		t.Fatalf("persisted snapshot should reflect state at Snapshot() time, got %q", persisted["a"])
	}
}

type fakeSink struct {
	*bytes.Buffer
	closed bool
}

func (s *fakeSink) ID() string    { return "test-snapshot" }
func (s *fakeSink) Cancel() error { return nil }
func (s *fakeSink) Close() error  { s.closed = true; return nil }
