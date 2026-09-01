package raftnode

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// OnDiskStores holds the BoltDB log/stable store and file snapshot store
// for one shard's Raft group, rooted at dataDir/<shardID>/.
type OnDiskStores struct {
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	boltStore     *raftboltdb.BoltStore
}

// Close releases the underlying BoltDB file handle.
func (s *OnDiskStores) Close() error {
	if s.boltStore == nil {
		return nil
	}
	return s.boltStore.Close()
}

// NewOnDiskStores creates (or opens) the BoltDB-backed log/stable store and
// a file-based snapshot store for a shard, under dataDir/<shardID>/.
func NewOnDiskStores(dataDir, shardID string, logOutput io.Writer) (*OnDiskStores, error) {
	shardDir := filepath.Join(dataDir, shardID)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return nil, fmt.Errorf("raftnode: create data dir %s: %w", shardDir, err)
	}

	boltPath := filepath.Join(shardDir, "raft.db")
	bolt, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, fmt.Errorf("raftnode: open bolt store %s: %w", boltPath, err)
	}

	snaps, err := raft.NewFileSnapshotStore(shardDir, 2, logOutput)
	if err != nil {
		bolt.Close()
		return nil, fmt.Errorf("raftnode: new file snapshot store: %w", err)
	}

	return &OnDiskStores{
		LogStore:      bolt,
		StableStore:   bolt,
		SnapshotStore: snaps,
		boltStore:     bolt,
	}, nil
}

// HasExistingState reports whether shardDir already contains Raft log
// entries, i.e. whether this node has previously bootstrapped or joined a
// cluster for this shard. It is used to avoid re-bootstrapping or
// re-joining after a restart.
func HasExistingState(logStore raft.LogStore) (bool, error) {
	first, err := logStore.FirstIndex()
	if err != nil {
		return false, fmt.Errorf("raftnode: read first index: %w", err)
	}
	last, err := logStore.LastIndex()
	if err != nil {
		return false, fmt.Errorf("raftnode: read last index: %w", err)
	}
	return last >= first && last > 0, nil
}
