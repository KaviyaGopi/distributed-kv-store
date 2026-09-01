// Package api implements the HTTP KV and cluster-admin surface for a
// kvnode: GET/PUT/DELETE on keys (routed to the owning shard and forwarded
// to that shard's leader when necessary), cluster status, and join
// handling.
package api

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/hashicorp/raft"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
	"github.com/KaviyaGopi/distributed-kv-store/internal/raftnode"
	"github.com/KaviyaGopi/distributed-kv-store/internal/shard"
)

// EventPublisher is the subset of the Kafka producer the API layer needs.
// Defined here (rather than depending on internal/events directly) so the
// API package doesn't need to know about Kafka wiring details; a nil
// EventPublisher disables publishing entirely.
type EventPublisher interface {
	Publish(op fsm.Op, key, value string, shardID, raftIndex int)
}

// AddrBook resolves a node ID to the HTTP address other nodes/clients can
// reach it on, used to turn a Raft leader's ID into a leader-hint URL, and
// records new mappings as nodes join the cluster.
type AddrBook interface {
	HTTPAddrFor(nodeID string) (string, bool)
	Set(nodeID, httpAddr string)
	All() map[string]string
}

// Server serves the KV and admin HTTP API for one node.
type Server struct {
	node     *raftnode.Node
	router   *shard.Router
	addrs    AddrBook
	events   EventPublisher
	applyTTL time.Duration

	mux *http.ServeMux
}

// NewServer builds a Server for node, using router to map keys to shards
// and addrs to resolve leader hints. events may be nil to disable Kafka
// publishing.
func NewServer(node *raftnode.Node, router *shard.Router, addrs AddrBook, events EventPublisher) *Server {
	s := &Server{
		node:     node,
		router:   router,
		addrs:    addrs,
		events:   events,
		applyTTL: 5 * time.Second,
	}
	s.mux = http.NewServeMux()
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /kv/{key}", s.handleGet)
	s.mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	s.mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	s.mux.HandleFunc("GET /cluster/status", s.handleClusterStatus)
	s.mux.HandleFunc("POST /admin/join", s.handleJoin)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) shardFor(key string) *raftnode.ShardPeer {
	idx := s.router.ShardFor(key)
	return s.node.Shards[idx]
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		if err := json.NewEncoder(w).Encode(v); err != nil {
			log.Printf("api: encode response: %v", err)
		}
	}
}

type errorResponse struct {
	Error  string `json:"error"`
	Leader string `json:"leader,omitempty"`
}

// notLeaderResponse writes a 409 with a leader hint (an HTTP address, if
// resolvable) so the caller can retry against the right node.
func (s *Server) notLeaderResponse(w http.ResponseWriter, peer *raftnode.ShardPeer) {
	resp := errorResponse{Error: "not leader"}
	if _, id := peer.Raft.LeaderWithID(); id != "" {
		if addr, ok := s.addrs.HTTPAddrFor(string(id)); ok {
			resp.Leader = addr
		}
	}
	writeJSON(w, http.StatusConflict, resp)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	peer := s.shardFor(key)

	v, ok := peer.FSM.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "key not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":   key,
		"value": v,
		"shard": s.router.ShardFor(key),
	})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "failed to read body"})
		return
	}

	s.apply(w, key, fsm.Command{Op: fsm.OpPut, Key: key, Value: string(body)})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	s.apply(w, key, fsm.Command{Op: fsm.OpDelete, Key: key})
}

func (s *Server) apply(w http.ResponseWriter, key string, cmd fsm.Command) {
	shardIdx := s.router.ShardFor(key)
	peer := s.node.Shards[shardIdx]

	if peer.Raft.State() != raft.Leader {
		s.notLeaderResponse(w, peer)
		return
	}

	data, err := cmd.Encode()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to encode command"})
		return
	}

	future := peer.Raft.Apply(data, s.applyTTL)
	if err := future.Error(); err != nil {
		if err == raft.ErrNotLeader || err == raft.ErrLeadershipLost {
			s.notLeaderResponse(w, peer)
			return
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if res := future.Response(); res != nil {
		if fsmErr, ok := res.(error); ok {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: fsmErr.Error()})
			return
		}
	}

	raftIndex := int(future.Index())
	if s.events != nil {
		s.events.Publish(cmd.Op, cmd.Key, cmd.Value, shardIdx, raftIndex)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":        key,
		"shard":      shardIdx,
		"raft_index": raftIndex,
	})
}

type shardStatus struct {
	Shard        int    `json:"shard"`
	State        string `json:"state"`
	Leader       string `json:"leader,omitempty"`
	Term         uint64 `json:"term"`
	AppliedIndex uint64 `json:"applied_index"`
	LastIndex    uint64 `json:"last_index"`
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	statuses := make([]shardStatus, len(s.node.Shards))
	for i, peer := range s.node.Shards {
		_, leaderID := peer.Raft.LeaderWithID()
		stats := peer.Raft.Stats()

		term := parseUint(stats["term"])
		applied := parseUint(stats["applied_index"])
		last := parseUint(stats["last_log_index"])

		statuses[i] = shardStatus{
			Shard:        i,
			State:        peer.Raft.State().String(),
			Leader:       string(leaderID),
			Term:         term,
			AppliedIndex: applied,
			LastIndex:    last,
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"node_id": s.node.ID,
		"shards":  statuses,
	})
}

type joinRequest struct {
	NodeID    string   `json:"node_id"`
	HTTPAddr  string   `json:"http_addr"`
	RaftAddrs []string `json:"raft_addrs"`
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}
	if req.NodeID == "" || req.HTTPAddr == "" || len(req.RaftAddrs) != len(s.node.Shards) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "node_id, http_addr, and one raft_addr per shard are required"})
		return
	}

	for i, peer := range s.node.Shards {
		if peer.Raft.State() != raft.Leader {
			s.notLeaderResponse(w, peer)
			return
		}
		if err := raftnode.AddVoter(peer.Raft, raft.ServerID(req.NodeID), raft.ServerAddress(req.RaftAddrs[i]), s.applyTTL); err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
	}

	s.addrs.Set(req.NodeID, req.HTTPAddr)
	writeJSON(w, http.StatusOK, joinResponse{Status: "joined", Peers: s.addrs.All()})
}

type joinResponse struct {
	Status string            `json:"status"`
	Peers  map[string]string `json:"peers"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func parseUint(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
