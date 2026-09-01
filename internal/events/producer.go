package events

import (
	"context"
	"errors"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
)

// PublishRecorder observes the outcome and latency of Kafka publish
// attempts. A nil PublishRecorder (the default) disables observation.
type PublishRecorder interface {
	ObserveKafkaPublish(status string, duration time.Duration)
}

// Producer publishes KeyChanged events to Kafka, keyed by the KV key so
// per-key ordering matches Kafka's per-partition ordering guarantee.
type Producer struct {
	writer   *kafka.Writer
	timeout  time.Duration
	recorder PublishRecorder
}

// NewProducer returns a Producer publishing to topic on the given brokers.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
			BatchTimeout:           50 * time.Millisecond,
		},
		timeout: 10 * time.Second,
	}
}

// WithMetrics attaches a PublishRecorder that observes every publish
// attempt.
func (p *Producer) WithMetrics(r PublishRecorder) *Producer {
	p.recorder = r
	return p
}

// Publish asynchronously sends a KeyChanged event for a committed write.
// Publishing is best-effort: a Kafka error is logged, never returned or
// allowed to affect the caller, since Kafka is a downstream change-feed and
// must never become a dependency of the write path.
func (p *Producer) Publish(op fsm.Op, key, value string, shardID, raftIndex int) {
	go func() {
		evt := KeyChanged{
			Op:        op,
			Key:       key,
			Value:     value,
			Shard:     shardID,
			RaftIndex: raftIndex,
			Timestamp: time.Now().UTC(),
		}
		data, err := evt.encode()
		if err != nil {
			log.Printf("events: encode %s %s: %v", op, key, err)
			return
		}

		publishStart := time.Now()
		err = p.publishWithRetry(key, data)

		status := "success"
		if err != nil {
			status = "failure"
			log.Printf("events: publish %s %s: %v", op, key, err)
		}
		if p.recorder != nil {
			p.recorder.ObserveKafkaPublish(status, time.Since(publishStart))
		}
	}()
}

// publishWithRetry retries the write a few times on kafka.UnknownTopicOrPartition,
// which kafka-go's own Writer.MaxAttempts does not cover: it comes from the
// partition-count metadata lookup made before any batch is produced, so it
// fails outright rather than through the writer's per-batch retry path. This
// only matters for a brand-new, not-yet-created topic (e.g. right after
// AllowAutoTopicCreation creates it, or a broker still electing a leader for
// it) -- a small bounded retry here is enough to ride that out.
func (p *Producer) publishWithRetry(key string, value []byte) error {
	const maxAttempts = 10
	backoff := 200 * time.Millisecond

	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		err = p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value})
		cancel()

		if err == nil || !errors.Is(err, kafka.UnknownTopicOrPartition) || attempt == maxAttempts {
			return err
		}
		time.Sleep(backoff)
	}
	return err
}

// Close flushes and closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
