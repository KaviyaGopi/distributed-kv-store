// Command kv-events-consumer tails the kv-events Kafka topic and prints
// each committed-write event, standing in for a downstream audit/analytics
// consumer of the KV store's change feed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	topic := flag.String("topic", "kv-events", "Kafka topic to consume")
	groupID := flag.String("group-id", "kv-events-consumer", "Kafka consumer group ID")
	flag.Parse()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(*brokers, ","),
		Topic:   *topic,
		GroupID: *groupID,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("kv-events-consumer: tailing topic %q on %s", *topic, *brokers)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("kv-events-consumer: shutting down")
				return
			}
			// Right after the cluster starts, the consumer group's
			// offsets topic and coordinator may not be ready yet; back
			// off briefly rather than busy-looping while that settles.
			log.Printf("kv-events-consumer: read error: %v", err)
			select {
			case <-ctx.Done():
				log.Printf("kv-events-consumer: shutting down")
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		var pretty map[string]interface{}
		if err := json.Unmarshal(msg.Value, &pretty); err != nil {
			log.Printf("kv-events-consumer: malformed event: %v", err)
			continue
		}
		out, _ := json.Marshal(pretty)
		log.Printf("event: partition=%d offset=%d key=%s value=%s", msg.Partition, msg.Offset, msg.Key, out)
	}
}
