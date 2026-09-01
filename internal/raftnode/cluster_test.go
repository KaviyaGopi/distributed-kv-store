package raftnode

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
)

// testNode bundles one shard-group peer built entirely on raft's in-memory
// transport and stores, so the whole cluster runs in-process with no real
// network ports or disk -- suitable for plain `go test`.
type testNode struct {
	id        raft.ServerID
	addr      raft.ServerAddress
	fsm       *fsm.KVFSM
	raft      *raft.Raft
	transport *raft.InmemTransport
}

func newTestCluster(t *testing.T, n int) []*testNode {
	t.Helper()

	nodes := make([]*testNode, n)
	transports := make(map[raft.ServerID]*raft.InmemTransport, n)

	for i := 0; i < n; i++ {
		id := raft.ServerID(fmt.Sprintf("node%d", i+1))
		addr, transport := raft.NewInmemTransport("")

		f := fsm.NewKVFSM()
		r, err := New(Deps{
			FSM:           f,
			LogStore:      raft.NewInmemStore(),
			StableStore:   raft.NewInmemStore(),
			SnapshotStore: raft.NewInmemSnapshotStore(),
			Transport:     transport,
			LocalID:       id,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}

		nodes[i] = &testNode{id: id, addr: addr, fsm: f, raft: r, transport: transport}
		transports[id] = transport
	}

	// Wire every transport to every other transport so peers can dial one
	// another, mirroring what a real TCP transport does over the network.
	for _, n1 := range nodes {
		for _, n2 := range nodes {
			if n1.id == n2.id {
				continue
			}
			n1.transport.Connect(n2.addr, n2.transport)
		}
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown().Error()
		}
	})

	return nodes
}

func bootstrapCluster(t *testing.T, nodes []*testNode) {
	t.Helper()

	servers := make([]raft.Server, len(nodes))
	for i, n := range nodes {
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: n.id, Address: n.addr}
	}
	cfg := raft.Configuration{Servers: servers}

	// BootstrapCluster with the full server set must be called on exactly
	// one node; raft replicates the initial configuration to the rest once
	// they start receiving RPCs from the elected leader.
	if err := nodes[0].raft.BootstrapCluster(cfg).Error(); err != nil {
		t.Fatalf("BootstrapCluster: %v", err)
	}
}

func leaderOf(nodes []*testNode) *testNode {
	for _, n := range nodes {
		if n.raft.State() == raft.Leader {
			return n
		}
	}
	return nil
}

func waitForLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := leaderOf(nodes); l != nil {
			return l
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func applyPut(t *testing.T, leader *testNode, key, value string) {
	t.Helper()
	cmd := fsm.Command{Op: fsm.OpPut, Key: key, Value: value}
	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	future := leader.raft.Apply(data, 2*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply(%s=%s) on %s: %v", key, value, leader.id, err)
	}
	if res := future.Response(); res != nil {
		t.Fatalf("Apply(%s=%s) unexpected FSM error: %v", key, value, res)
	}
}

func waitForReplication(t *testing.T, nodes []*testNode, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		allMatch := true
		for _, n := range nodes {
			if v, ok := n.fsm.Get(key); !ok || v != want {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		if time.Now().After(deadline) {
			for _, n := range nodes {
				v, ok := n.fsm.Get(key)
				t.Logf("node %s: Get(%s) = %q, %v", n.id, key, v, ok)
			}
			t.Fatalf("replication of %s=%s did not converge within %s", key, want, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestThreeNodeClusterElectsLeaderReplicatesAndSurvivesFailover is the core
// proof of the resume claim: a 3-node Raft group elects a leader, replicates
// committed writes to every follower, and continues to make progress (via a
// newly elected leader) after the original leader is shut down.
func TestThreeNodeClusterElectsLeaderReplicatesAndSurvivesFailover(t *testing.T) {
	nodes := newTestCluster(t, 3)
	bootstrapCluster(t, nodes)

	leader := waitForLeader(t, nodes, 5*time.Second)
	t.Logf("elected initial leader: %s", leader.id)

	applyPut(t, leader, "foo", "bar")
	waitForReplication(t, nodes, "foo", "bar", 2*time.Second)

	// Shut down the leader to force a new election, simulating a node
	// failure / network partition of the leader.
	if err := leader.raft.Shutdown().Error(); err != nil {
		t.Fatalf("Shutdown leader %s: %v", leader.id, err)
	}

	remaining := make([]*testNode, 0, 2)
	for _, n := range nodes {
		if n.id != leader.id {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 5*time.Second)
	if newLeader.id == leader.id {
		t.Fatalf("expected a new leader distinct from %s", leader.id)
	}
	t.Logf("elected new leader after failover: %s", newLeader.id)

	applyPut(t, newLeader, "baz", "qux")
	waitForReplication(t, remaining, "baz", "qux", 2*time.Second)

	// The original write must still be present after failover.
	for _, n := range remaining {
		if v, ok := n.fsm.Get("foo"); !ok || v != "bar" {
			t.Fatalf("node %s lost pre-failover write: Get(foo) = %q, %v", n.id, v, ok)
		}
	}
}

func TestWaitForLeaderTimesOutWithNoQuorum(t *testing.T) {
	nodes := newTestCluster(t, 1)
	// Deliberately do not bootstrap: a lone, unbootstrapped node never
	// becomes a candidate, so WaitForLeader must time out rather than hang.
	if _, err := WaitForLeader(nodes[0].raft, 200*time.Millisecond); err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}
