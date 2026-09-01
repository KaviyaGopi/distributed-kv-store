package raftnode

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
)

// freePort asks the OS for an available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestOnDiskTwoNodeClusterBootstrapAndJoin exercises the real (non-inmem)
// on-disk BoltDB store + TCP transport path for a single shard: node1
// bootstraps a one-node cluster, node2 joins via AddVoter, and a write made
// through node1 replicates to node2's FSM. This validates the exact
// bootstrap/join wiring that cmd/kvnode uses, isolated to one shard before
// it gets parameterized across three.
func TestOnDiskTwoNodeClusterBootstrapAndJoin(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	addr1 := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	addr2 := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	node1, close1 := newOnDiskTestNode(t, "node1", dir1, addr1)
	defer close1()
	node2, close2 := newOnDiskTestNode(t, "node2", dir2, addr2)
	defer close2()

	if err := Bootstrap(node1.raft, "node1", raft.ServerAddress(addr1)); err != nil {
		t.Fatalf("Bootstrap node1: %v", err)
	}

	if _, err := WaitForLeader(node1.raft, 5*time.Second); err != nil {
		t.Fatalf("WaitForLeader after bootstrap: %v", err)
	}
	if node1.raft.State() != raft.Leader {
		t.Fatalf("expected node1 to be leader after bootstrapping alone, got %s", node1.raft.State())
	}

	if err := AddVoter(node1.raft, "node2", raft.ServerAddress(addr2), 5*time.Second); err != nil {
		t.Fatalf("AddVoter node2: %v", err)
	}

	cmd := fsm.Command{Op: fsm.OpPut, Key: "k", Value: "v"}
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := node1.raft.Apply(data, 5*time.Second).Error(); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, ok := node2.fsm.Get("k"); ok && v == "v" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("write did not replicate to joined node2 within timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type onDiskTestNode struct {
	raft *raft.Raft
	fsm  *fsm.KVFSM
}

func newOnDiskTestNode(t *testing.T, id, dataDir, advertiseAddr string) (*onDiskTestNode, func()) {
	t.Helper()

	stores, err := NewOnDiskStores(dataDir, "shard-0", io.Discard)
	if err != nil {
		t.Fatalf("NewOnDiskStores(%s): %v", id, err)
	}

	transport, err := NewTCPTransport(advertiseAddr, advertiseAddr, io.Discard)
	if err != nil {
		stores.Close()
		t.Fatalf("NewTCPTransport(%s): %v", id, err)
	}

	f := fsm.NewKVFSM()
	r, err := New(Deps{
		FSM:           f,
		LogStore:      stores.LogStore,
		StableStore:   stores.StableStore,
		SnapshotStore: stores.SnapshotStore,
		Transport:     transport,
		LocalID:       raft.ServerID(id),
	})
	if err != nil {
		stores.Close()
		transport.Close()
		t.Fatalf("New(%s): %v", id, err)
	}

	closeFn := func() {
		_ = r.Shutdown().Error()
		_ = stores.Close()
		_ = transport.Close()
	}
	return &onDiskTestNode{raft: r, fsm: f}, closeFn
}
