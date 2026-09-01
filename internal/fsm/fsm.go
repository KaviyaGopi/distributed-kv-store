// Package fsm implements the Raft finite state machine for the key-value
// store: applying committed commands to an in-memory map and providing
// snapshot/restore for log compaction.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// KVFSM is a raft.FSM backed by an in-memory map. Apply is invoked by the
// raft library once a log entry has been committed by a quorum.
type KVFSM struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVFSM returns an empty KVFSM.
func NewKVFSM() *KVFSM {
	return &KVFSM{data: make(map[string]string)}
}

// Get returns the current value for key and whether it is present.
func (f *KVFSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	return v, ok
}

// Apply implements raft.FSM. It is called once per committed log entry, in
// log order, on every node in the Raft group (leader and followers alike).
func (f *KVFSM) Apply(log *raft.Log) interface{} {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		return fmt.Errorf("fsm: decode command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		f.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(f.data, cmd.Key)
	default:
		return fmt.Errorf("fsm: unknown op %q", cmd.Op)
	}
	return nil
}

// Snapshot implements raft.FSM.
func (f *KVFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	clone := make(map[string]string, len(f.data))
	for k, v := range f.data {
		clone[k] = v
	}
	return &kvSnapshot{data: clone}, nil
}

// Restore implements raft.FSM. It replaces the FSM's state wholesale from a
// previously captured snapshot.
func (f *KVFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string]string
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("fsm: restore decode: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = data
	return nil
}

type kvSnapshot struct {
	data map[string]string
}

func (s *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		enc := json.NewEncoder(sink)
		if err := enc.Encode(s.data); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *kvSnapshot) Release() {}
