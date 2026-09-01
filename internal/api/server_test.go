package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
	"github.com/KaviyaGopi/distributed-kv-store/internal/raftnode"
	"github.com/KaviyaGopi/distributed-kv-store/internal/shard"
)

type fakeAddrBook struct{}

func (fakeAddrBook) HTTPAddrFor(nodeID string) (string, bool) {
	return "http://" + nodeID + ":8080", nodeID != ""
}

func (fakeAddrBook) Set(nodeID, httpAddr string) {}

func (fakeAddrBook) All() map[string]string { return nil }

type recordingPublisher struct {
	events []publishedEvent
}

type publishedEvent struct {
	Op        fsm.Op
	Key       string
	Value     string
	ShardID   int
	RaftIndex int
}

func (p *recordingPublisher) Publish(op fsm.Op, key, value string, shardID, raftIndex int) {
	p.events = append(p.events, publishedEvent{op, key, value, shardID, raftIndex})
}

// newSingleShardTestServer builds a Server backed by one single-node,
// self-bootstrapped Raft group (shard count 1) using in-memory transport
// and stores, so handler tests exercise the real Apply/leader-check path
// without needing real ports or disk.
func newSingleShardTestServer(t *testing.T) (*Server, *recordingPublisher) {
	t.Helper()

	addr, transport := raft.NewInmemTransport("")
	f := fsm.NewKVFSM()
	r, err := raftnode.New(raftnode.Deps{
		FSM:           f,
		LogStore:      raft.NewInmemStore(),
		StableStore:   raft.NewInmemStore(),
		SnapshotStore: raft.NewInmemSnapshotStore(),
		Transport:     transport,
		LocalID:       "node1",
	})
	if err != nil {
		t.Fatalf("raftnode.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	if err := raftnode.Bootstrap(r, "node1", addr); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if _, err := raftnode.WaitForLeader(r, 5*time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	node := &raftnode.Node{ID: "node1", Shards: []*raftnode.ShardPeer{{Raft: r, FSM: f}}}
	router := shard.NewRouter(1)
	pub := &recordingPublisher{}
	return NewServer(node, router, fakeAddrBook{}, pub), pub
}

func doRequest(t *testing.T, srv *Server, method, path, body string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec.Result()
}

func decodeJSON(t *testing.T, resp *http.Response, v interface{}) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func TestPutThenGet(t *testing.T) {
	srv, pub := newSingleShardTestServer(t)

	resp := doRequest(t, srv, http.MethodPut, "/kv/foo", "bar")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	var putResp map[string]interface{}
	decodeJSON(t, resp, &putResp)
	if putResp["key"] != "foo" {
		t.Fatalf("PUT response key = %v, want foo", putResp["key"])
	}

	resp = doRequest(t, srv, http.MethodGet, "/kv/foo", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	var getResp map[string]interface{}
	decodeJSON(t, resp, &getResp)
	if getResp["value"] != "bar" {
		t.Fatalf("GET response value = %v, want bar", getResp["value"])
	}

	if len(pub.events) != 1 || pub.events[0].Key != "foo" || pub.events[0].Value != "bar" {
		t.Fatalf("expected one publish event for foo=bar, got %+v", pub.events)
	}
}

func TestGetMissingKeyReturns404(t *testing.T) {
	srv, _ := newSingleShardTestServer(t)

	resp := doRequest(t, srv, http.MethodGet, "/kv/missing", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", resp.StatusCode)
	}
}

func TestDelete(t *testing.T) {
	srv, _ := newSingleShardTestServer(t)

	doRequest(t, srv, http.MethodPut, "/kv/foo", "bar")

	resp := doRequest(t, srv, http.MethodDelete, "/kv/foo", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status = %d, want 200", resp.StatusCode)
	}

	resp = doRequest(t, srv, http.MethodGet, "/kv/foo", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want 404", resp.StatusCode)
	}
}

func TestClusterStatus(t *testing.T) {
	srv, _ := newSingleShardTestServer(t)

	resp := doRequest(t, srv, http.MethodGet, "/cluster/status", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]interface{}
	decodeJSON(t, resp, &body)
	if body["node_id"] != "node1" {
		t.Fatalf("node_id = %v, want node1", body["node_id"])
	}
	shards, ok := body["shards"].([]interface{})
	if !ok || len(shards) != 1 {
		t.Fatalf("shards = %v, want a single-element array", body["shards"])
	}
}

func TestHealthz(t *testing.T) {
	srv, _ := newSingleShardTestServer(t)
	resp := doRequest(t, srv, http.MethodGet, "/healthz", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestJoinRejectsIncompleteRequest(t *testing.T) {
	srv, _ := newSingleShardTestServer(t)

	resp := doRequest(t, srv, http.MethodPost, "/admin/join", `{"node_id":"node2"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestNotLeaderResponseShape(t *testing.T) {
	// Build a lone, never-bootstrapped raft instance: it never becomes
	// leader, so every write must return the not-leader shape.
	_, transport := raft.NewInmemTransport("")
	f := fsm.NewKVFSM()
	r, err := raftnode.New(raftnode.Deps{
		FSM:           f,
		LogStore:      raft.NewInmemStore(),
		StableStore:   raft.NewInmemStore(),
		SnapshotStore: raft.NewInmemSnapshotStore(),
		Transport:     transport,
		LocalID:       "node1",
	})
	if err != nil {
		t.Fatalf("raftnode.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Shutdown().Error() })

	node := &raftnode.Node{ID: "node1", Shards: []*raftnode.ShardPeer{{Raft: r, FSM: f}}}
	srv := NewServer(node, shard.NewRouter(1), fakeAddrBook{}, nil)

	resp := doRequest(t, srv, http.MethodPut, "/kv/foo", "bar")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	var body errorResponse
	decodeJSON(t, resp, &body)
	if body.Error != "not leader" {
		t.Fatalf("error = %q, want %q", body.Error, "not leader")
	}
}
