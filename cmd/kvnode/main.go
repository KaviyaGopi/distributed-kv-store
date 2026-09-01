// Command kvnode runs one physical node of the distributed KV store: one
// Raft peer per shard, the HTTP KV/admin/metrics API, and (if configured) a
// Kafka event publisher.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/KaviyaGopi/distributed-kv-store/internal/api"
	"github.com/KaviyaGopi/distributed-kv-store/internal/config"
	"github.com/KaviyaGopi/distributed-kv-store/internal/events"
	"github.com/KaviyaGopi/distributed-kv-store/internal/metrics"
	"github.com/KaviyaGopi/distributed-kv-store/internal/raftnode"
	"github.com/KaviyaGopi/distributed-kv-store/internal/shard"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("kvnode: %v", err)
	}
}

func run() error {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		return err
	}

	shardConfigs := make([]raftnode.ShardConfig, config.NumShards)
	for i := 0; i < config.NumShards; i++ {
		shardConfigs[i] = raftnode.ShardConfig{
			ShardID:       fmt.Sprintf("shard-%d", i),
			BindAddr:      cfg.RaftBindAddrs[i],
			AdvertiseAddr: cfg.RaftAdvertiseAddrs[i],
		}
	}

	node, err := raftnode.NewNode(cfg.NodeID, cfg.DataDir, shardConfigs, os.Stderr)
	if err != nil {
		return fmt.Errorf("construct node: %w", err)
	}
	defer node.Close()

	hasState, err := raftnode.HasExistingState(node.Shards[0].Store.LogStore)
	if err != nil {
		return fmt.Errorf("check existing state: %w", err)
	}

	addrBook := api.NewMemAddrBook()
	addrBook.Set(cfg.NodeID, "http://"+cfg.HTTPAdvertiseAddr)

	switch {
	case cfg.Bootstrap && !hasState:
		log.Printf("kvnode: bootstrapping new cluster as %s", cfg.NodeID)
		if err := node.BootstrapAll(cfg.RaftAdvertiseAddrs[:]); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	case cfg.Bootstrap && hasState:
		log.Printf("kvnode: -bootstrap set but on-disk state already exists; resuming existing cluster state")
	case cfg.Join != "" && !hasState:
		log.Printf("kvnode: requesting to join cluster via %s", cfg.Join)
		peers, err := joinCluster(cfg)
		if err != nil {
			return fmt.Errorf("join cluster: %w", err)
		}
		for id, addr := range peers {
			addrBook.Set(id, addr)
		}
	case cfg.Join != "" && hasState:
		log.Printf("kvnode: -join set but on-disk state already exists; resuming existing cluster state")
	default:
		log.Printf("kvnode: resuming existing cluster state (no -bootstrap or -join)")
	}

	router := shard.NewRouter(config.NumShards)

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	stopPollers := make(chan struct{})
	defer close(stopPollers)
	for i, peer := range node.Shards {
		go metrics.PollRaftStats(m, i, peer.Raft, time.Second, stopPollers)
	}

	var publisher api.EventPublisher
	if brokers := cfg.KafkaBrokerList(); len(brokers) > 0 {
		producer := events.NewProducer(brokers, cfg.KafkaTopic, config.NumShards).WithMetrics(m)
		defer producer.Close()
		publisher = producer
		log.Printf("kvnode: publishing committed writes to Kafka topic %q on %v", cfg.KafkaTopic, brokers)
	} else {
		log.Printf("kvnode: Kafka publishing disabled (no -kafka-brokers configured)")
	}

	server := api.NewServer(node, router, addrBook, publisher).WithMetrics(m)

	mux := http.NewServeMux()
	mux.Handle("/", server)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("kvnode: HTTP API listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case sig := <-sigCh:
		log.Printf("kvnode: received %s, shutting down", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(ctx)
}

func joinCluster(cfg config.Config) (map[string]string, error) {
	return api.RequestJoin(
		cfg.Join,
		cfg.NodeID,
		"http://"+cfg.HTTPAdvertiseAddr,
		cfg.RaftAdvertiseAddrs[:],
		10*time.Second,
	)
}
