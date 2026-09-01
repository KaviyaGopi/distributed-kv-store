// Package events publishes a downstream change-feed of committed KV writes
// to Kafka. This is explicitly NOT part of Raft consensus or replication --
// Raft alone guarantees linearizability and durability. Kafka publishing
// happens only after a write has already been committed via raft.Apply,
// and is best-effort: a Kafka outage must never block or fail a write.
package events

import (
	"encoding/json"
	"time"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
)

// KeyChanged is the JSON payload published to Kafka for every committed
// write, keyed by Key so per-key ordering matches Kafka partition ordering.
type KeyChanged struct {
	Op        fsm.Op    `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	Shard     int       `json:"shard"`
	RaftIndex int       `json:"raft_index"`
	Timestamp time.Time `json:"timestamp"`
}

func (e KeyChanged) encode() ([]byte, error) {
	return json.Marshal(e)
}
