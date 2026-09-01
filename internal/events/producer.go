package events

import (
	"context"
	"errors"
	"fmt"
	"log"
	"syscall"
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
// It ensures, in the background, that the topic exists with numPartitions
// partitions -- relying on the writer's AllowAutoTopicCreation instead
// would create it with the broker's default of a single partition,
// undermining the per-shard parallelism the change-feed is meant to have.
// This runs asynchronously (and tolerates the broker not being reachable
// yet, retrying for a while) so a slow-starting Kafka never delays node
// startup; any write that lands before the topic is ready is still carried
// by publishWithRetry.
func NewProducer(brokers []string, topic string, numPartitions int) *Producer {
	go ensureTopic(brokers, topic, numPartitions)

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

// ensureTopic creates topic with numPartitions if it doesn't already exist.
// Errors are logged, not returned: the publishWithRetry path in Publish
// still rides out a topic that isn't ready yet, so a failure here is not
// fatal to the node -- but dialing the broker is retried for a while first,
// since in Compose/Kubernetes the broker container commonly hasn't finished
// starting yet when this runs (depends_on only waits for "started", not
// "accepting connections").
func ensureTopic(brokers []string, topic string, numPartitions int) {
	if len(brokers) == 0 {
		return
	}

	const maxAttempts = 15
	backoff := 1 * time.Second

	var conn *kafka.Conn
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err = kafka.Dial("tcp", brokers[0])
		if err == nil {
			break
		}
		if attempt == maxAttempts {
			log.Printf("events: dial %s to create topic %q: %v", brokers[0], topic, err)
			return
		}
		time.Sleep(backoff)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Printf("events: find controller for topic %q: %v", topic, err)
		return
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		log.Printf("events: dial controller for topic %q: %v", topic, err)
		return
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	})
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		log.Printf("events: create topic %q: %v", topic, err)
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
// must never become a dependency of the write path. A consequence of that
// design: the retry in publishWithRetry only survives transient broker
// unavailability while this process stays alive. If the node is killed
// while a publish is still retrying (e.g. moments after a write, before
// Kafka has finished starting), that event is dropped -- the write itself
// is unaffected since it was already durably committed via Raft on other
// nodes. Guaranteeing delivery across a node crash would need a
// write-ahead outbox table replayed on restart, which is deliberately out
// of scope here.
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

// publishWithRetry retries the write on errors from the partition-count
// metadata lookup WriteMessages makes before producing any batch: an
// unready topic (kafka.UnknownTopicOrPartition, e.g. right after
// AllowAutoTopicCreation creates it, or a broker still electing a leader
// for it) or a broker that isn't accepting connections yet (a plain
// ECONNREFUSED, common right after `docker compose up` since depends_on
// only waits for the container to start, not for Kafka to be ready).
// kafka-go's own Writer.MaxAttempts does not cover either case: both come
// from that upfront lookup, not the per-batch produce retry path.
func (p *Producer) publishWithRetry(key string, value []byte) error {
	const maxAttempts = 30
	backoff := 500 * time.Millisecond

	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		err = p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value})
		cancel()

		if err == nil || attempt == maxAttempts {
			return err
		}
		if !errors.Is(err, kafka.UnknownTopicOrPartition) && !errors.Is(err, syscall.ECONNREFUSED) {
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
