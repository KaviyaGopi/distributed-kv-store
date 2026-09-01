// Package metrics defines the Prometheus collectors exposed by a kvnode
// process. Label sets are kept small (shard, op, status) -- never the KV
// key itself -- to avoid unbounded cardinality.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles every collector registered by a kvnode process.
type Metrics struct {
	RaftState           *prometheus.GaugeVec
	RaftLeaderChanges   *prometheus.CounterVec
	RaftCommitLatency   *prometheus.HistogramVec
	RaftAppliedIndex    *prometheus.GaugeVec
	RaftLastIndex       *prometheus.GaugeVec
	KVRequests          *prometheus.CounterVec
	KVRequestDuration   *prometheus.HistogramVec
	KafkaPublishTotal   *prometheus.CounterVec
	KafkaPublishLatency prometheus.Histogram
}

// New creates and registers all collectors against reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		RaftState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_state",
			Help: "Current Raft role per shard: 0=follower, 1=candidate, 2=leader, 3=shutdown.",
		}, []string{"shard"}),
		RaftLeaderChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raft_leader_changes_total",
			Help: "Number of times this node observed a new Raft leader, per shard.",
		}, []string{"shard"}),
		RaftCommitLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "raft_commit_latency_seconds",
			Help:    "Time from raft.Apply() call to commit completion, per shard.",
			Buckets: prometheus.DefBuckets,
		}, []string{"shard"}),
		RaftAppliedIndex: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_applied_index",
			Help: "Last log index applied to the FSM, per shard.",
		}, []string{"shard"}),
		RaftLastIndex: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_last_index",
			Help: "Last log index present in the Raft log, per shard.",
		}, []string{"shard"}),
		KVRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_requests_total",
			Help: "KV API requests, by operation and outcome.",
		}, []string{"op", "shard", "status"}),
		KVRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kv_request_duration_seconds",
			Help:    "KV API request latency, by operation.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op", "shard"}),
		KafkaPublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_publish_total",
			Help: "Kafka event publish attempts, by outcome.",
		}, []string{"status"}),
		KafkaPublishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafka_publish_latency_seconds",
			Help:    "Kafka publish call latency.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	reg.MustRegister(
		m.RaftState,
		m.RaftLeaderChanges,
		m.RaftCommitLatency,
		m.RaftAppliedIndex,
		m.RaftLastIndex,
		m.KVRequests,
		m.KVRequestDuration,
		m.KafkaPublishTotal,
		m.KafkaPublishLatency,
	)
	return m
}
