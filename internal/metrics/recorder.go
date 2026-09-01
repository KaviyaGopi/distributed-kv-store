package metrics

import "time"

// ObserveRequest implements api.RequestRecorder.
func (m *Metrics) ObserveRequest(op, shard, status string, duration time.Duration) {
	m.KVRequests.WithLabelValues(op, shard, status).Inc()
	m.KVRequestDuration.WithLabelValues(op, shard).Observe(duration.Seconds())
}

// ObserveKafkaPublish records the outcome and latency of one Kafka publish
// attempt. status is "success" or "failure".
func (m *Metrics) ObserveKafkaPublish(status string, duration time.Duration) {
	m.KafkaPublishTotal.WithLabelValues(status).Inc()
	m.KafkaPublishLatency.Observe(duration.Seconds())
}
