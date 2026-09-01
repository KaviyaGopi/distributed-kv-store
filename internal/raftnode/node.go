package raftnode

import (
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
)

// ShardPeer identifies one shard-group's Raft transport within a node.
type ShardPeer struct {
	Raft  *raft.Raft
	FSM   *fsm.KVFSM
	Store *OnDiskStores
}

// Node owns one raft.Raft instance per shard, all sharing the same physical
// node ID but listening on distinct raft-transport ports.
type Node struct {
	ID     string
	Shards []*ShardPeer
}

// ShardConfig describes the per-shard listen/advertise wiring for one node.
type ShardConfig struct {
	ShardID       string
	BindAddr      string
	AdvertiseAddr string
}

// NewNode constructs one raft.Raft instance per entry in shardConfigs,
// each with its own on-disk BoltDB store (under dataDir/<shardID>/) and TCP
// transport. It does not bootstrap or join any cluster.
func NewNode(nodeID, dataDir string, shardConfigs []ShardConfig, logOutput io.Writer) (*Node, error) {
	n := &Node{ID: nodeID}

	for _, sc := range shardConfigs {
		stores, err := NewOnDiskStores(dataDir, sc.ShardID, logOutput)
		if err != nil {
			return nil, fmt.Errorf("raftnode: shard %s stores: %w", sc.ShardID, err)
		}

		transport, err := NewTCPTransport(sc.BindAddr, sc.AdvertiseAddr, logOutput)
		if err != nil {
			return nil, fmt.Errorf("raftnode: shard %s transport: %w", sc.ShardID, err)
		}

		f := fsm.NewKVFSM()
		r, err := New(Deps{
			FSM:           f,
			LogStore:      stores.LogStore,
			StableStore:   stores.StableStore,
			SnapshotStore: stores.SnapshotStore,
			Transport:     transport,
			LocalID:       raft.ServerID(nodeID),
			LogOutput:     logOutput,
		})
		if err != nil {
			return nil, fmt.Errorf("raftnode: shard %s raft: %w", sc.ShardID, err)
		}

		n.Shards = append(n.Shards, &ShardPeer{Raft: r, FSM: f, Store: stores})
	}

	return n, nil
}

// BootstrapAll bootstraps every shard's Raft group with this node as the
// sole initial voter. Call this only on the very first node of a brand-new
// cluster, and only when no shard has existing on-disk state.
func (n *Node) BootstrapAll(advertiseAddrs []string) error {
	if len(advertiseAddrs) != len(n.Shards) {
		return fmt.Errorf("raftnode: bootstrap: got %d advertise addrs for %d shards", len(advertiseAddrs), len(n.Shards))
	}
	for i, s := range n.Shards {
		if err := Bootstrap(s.Raft, raft.ServerID(n.ID), raft.ServerAddress(advertiseAddrs[i])); err != nil {
			return fmt.Errorf("raftnode: bootstrap shard %d: %w", i, err)
		}
	}
	return nil
}

// Close shuts down every shard's Raft instance and closes its store.
func (n *Node) Close() error {
	var firstErr error
	for _, s := range n.Shards {
		if err := s.Raft.Shutdown().Error(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := s.Store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// WaitForAllLeaders blocks until every shard's Raft group has an elected
// leader, or the timeout elapses.
func (n *Node) WaitForAllLeaders(timeout time.Duration) error {
	for i, s := range n.Shards {
		if _, err := WaitForLeader(s.Raft, timeout); err != nil {
			return fmt.Errorf("raftnode: shard %d: %w", i, err)
		}
	}
	return nil
}
