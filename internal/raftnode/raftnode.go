// Package raftnode constructs and wires a single raft.Raft instance for one
// shard group, and provides the bootstrap/join operations used to form and
// grow a cluster.
package raftnode

import (
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
)

// Deps bundles the raft.Raft dependencies for a single shard's group. Real
// nodes populate this with TCP transport + BoltDB stores; tests populate it
// with raft's in-memory equivalents.
type Deps struct {
	FSM           *fsm.KVFSM
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Transport     raft.Transport
	LocalID       raft.ServerID
	LogOutput     io.Writer
}

// New constructs and starts a raft.Raft instance for one shard group. It
// does not bootstrap or join a cluster; callers do that separately via
// Bootstrap or AddVoter so the same constructor works for both the first
// node and nodes that join later.
func New(deps Deps) (*raft.Raft, error) {
	if deps.LocalID == "" {
		return nil, fmt.Errorf("raftnode: LocalID is required")
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = deps.LocalID
	if deps.LogOutput != nil {
		cfg.LogOutput = deps.LogOutput
	}
	// Portfolio/demo cluster: shorten election timing so failover in the
	// demo/tests is fast, while staying well above realistic RTT.
	cfg.HeartbeatTimeout = 200 * time.Millisecond
	cfg.ElectionTimeout = 200 * time.Millisecond
	cfg.LeaderLeaseTimeout = 100 * time.Millisecond
	cfg.CommitTimeout = 50 * time.Millisecond

	r, err := raft.NewRaft(cfg, deps.FSM, deps.LogStore, deps.StableStore, deps.SnapshotStore, deps.Transport)
	if err != nil {
		return nil, fmt.Errorf("raftnode: new raft: %w", err)
	}
	return r, nil
}

// Bootstrap initializes a brand-new single-node cluster consisting solely of
// self. It must be called at most once per shard's log store, before any
// other node has joined -- calling it against a non-empty log store is an
// error.
func Bootstrap(r *raft.Raft, localID raft.ServerID, localAddr raft.ServerAddress) error {
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       localID,
				Address:  localAddr,
			},
		},
	}
	return r.BootstrapCluster(cfg).Error()
}

// AddVoter adds a new voting member to the Raft group. It must be called
// against the current leader.
func AddVoter(leader *raft.Raft, id raft.ServerID, addr raft.ServerAddress, timeout time.Duration) error {
	return leader.AddVoter(id, addr, 0, timeout).Error()
}

// WaitForLeader blocks until the Raft group has elected a leader (which may
// or may not be r) or the timeout elapses.
func WaitForLeader(r *raft.Raft, timeout time.Duration) (raft.ServerAddress, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr, _ := r.LeaderWithID(); addr != "" {
			return addr, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", fmt.Errorf("raftnode: no leader elected within %s", timeout)
}
