// Package config defines the flag/env-based startup configuration for a
// kvnode process.
package config

import (
	"flag"
	"fmt"
	"strings"
)

const NumShards = 3

// Config holds one node's startup parameters.
type Config struct {
	NodeID string

	// DataDir is the root directory for this node's per-shard Raft state.
	DataDir string

	// HTTPAddr is the address the KV/admin/metrics HTTP server listens on.
	HTTPAddr string

	// HTTPAdvertiseAddr is how other nodes/clients reach this node's HTTP
	// server (used in leader-hint responses). Defaults to HTTPAddr.
	HTTPAdvertiseAddr string

	// RaftBindAddrs/RaftAdvertiseAddrs are per-shard (len == NumShards)
	// addresses for each shard's Raft TCP transport.
	RaftBindAddrs      [NumShards]string
	RaftAdvertiseAddrs [NumShards]string

	// Bootstrap, if true, initializes a brand-new single-node cluster on
	// first startup. Mutually exclusive with Join.
	Bootstrap bool

	// Join, if set, is the HTTP address of an existing cluster member to
	// request membership from on first startup.
	Join string

	// KafkaBrokers is a comma-separated list of Kafka broker addresses.
	// Empty disables Kafka event publishing.
	KafkaBrokers string

	// KafkaTopic is the topic committed writes are published to.
	KafkaTopic string
}

// Parse builds a Config from command-line flags.
func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("kvnode", flag.ContinueOnError)

	nodeID := fs.String("node-id", "", "unique ID for this node (required)")
	dataDir := fs.String("data-dir", "data", "root directory for this node's Raft state")
	httpAddr := fs.String("http-addr", "0.0.0.0:8080", "address for the KV/admin/metrics HTTP server")
	httpAdvertiseAddr := fs.String("http-advertise-addr", "", "address other nodes/clients use to reach this node's HTTP server (defaults to http-addr)")
	raftBase := fs.String("raft-bind-base", "0.0.0.0:9001", "base bind address for shard 0's Raft transport; shards 1..N use consecutive ports")
	raftAdvertiseBase := fs.String("raft-advertise-base", "", "base advertise host:port for shard 0's Raft transport (defaults to raft-bind-base); shards 1..N use consecutive ports")
	bootstrap := fs.Bool("bootstrap", false, "bootstrap a brand-new single-node cluster")
	join := fs.String("join", "", "HTTP address of an existing cluster member to join through")
	kafkaBrokers := fs.String("kafka-brokers", "", "comma-separated Kafka broker addresses (empty disables Kafka)")
	kafkaTopic := fs.String("kafka-topic", "kv-events", "Kafka topic committed writes are published to")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if *nodeID == "" {
		return Config{}, fmt.Errorf("config: -node-id is required")
	}
	if *bootstrap && *join != "" {
		return Config{}, fmt.Errorf("config: -bootstrap and -join are mutually exclusive")
	}

	advertiseHTTP := *httpAdvertiseAddr
	if advertiseHTTP == "" {
		advertiseHTTP = *httpAddr
	}

	raftAdvBase := *raftAdvertiseBase
	if raftAdvBase == "" {
		raftAdvBase = *raftBase
	}

	bindAddrs, err := sequentialAddrs(*raftBase, NumShards)
	if err != nil {
		return Config{}, fmt.Errorf("config: raft-bind-base: %w", err)
	}
	advAddrs, err := sequentialAddrs(raftAdvBase, NumShards)
	if err != nil {
		return Config{}, fmt.Errorf("config: raft-advertise-base: %w", err)
	}

	cfg := Config{
		NodeID:            *nodeID,
		DataDir:           *dataDir,
		HTTPAddr:          *httpAddr,
		HTTPAdvertiseAddr: advertiseHTTP,
		Bootstrap:         *bootstrap,
		Join:              *join,
		KafkaBrokers:      *kafkaBrokers,
		KafkaTopic:        *kafkaTopic,
	}
	copy(cfg.RaftBindAddrs[:], bindAddrs)
	copy(cfg.RaftAdvertiseAddrs[:], advAddrs)
	return cfg, nil
}

// sequentialAddrs expands a "host:basePort" string into n addresses with
// consecutive port numbers, one per shard.
func sequentialAddrs(base string, n int) ([]string, error) {
	host, portStr, err := splitHostPort(base)
	if err != nil {
		return nil, err
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
	}

	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("%s:%d", host, port+i)
	}
	return addrs, nil
}

func splitHostPort(addr string) (host, port string, err error) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("address %q missing port", addr)
	}
	return addr[:idx], addr[idx+1:], nil
}

// KafkaBrokerList splits KafkaBrokers on commas, trimming whitespace, and
// omitting empty entries.
func (c Config) KafkaBrokerList() []string {
	if strings.TrimSpace(c.KafkaBrokers) == "" {
		return nil
	}
	parts := strings.Split(c.KafkaBrokers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
