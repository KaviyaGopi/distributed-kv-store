package metrics

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

// PollRaftStats periodically updates the Raft gauges and leader-change
// counter for one shard, until stop is closed.
func PollRaftStats(m *Metrics, shardIdx int, r *raft.Raft, interval time.Duration, stop <-chan struct{}) {
	shardLabel := fmt.Sprintf("%d", shardIdx)
	var lastLeader raft.ServerAddress

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			m.RaftState.WithLabelValues(shardLabel).Set(float64(r.State()))

			stats := r.Stats()
			m.RaftAppliedIndex.WithLabelValues(shardLabel).Set(float64(parseUint(stats["applied_index"])))
			m.RaftLastIndex.WithLabelValues(shardLabel).Set(float64(parseUint(stats["last_log_index"])))

			if leader, _ := r.LeaderWithID(); leader != lastLeader {
				if lastLeader != "" {
					m.RaftLeaderChanges.WithLabelValues(shardLabel).Inc()
				}
				lastLeader = leader
			}
		}
	}
}

func parseUint(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
