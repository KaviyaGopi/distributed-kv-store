package config

import "testing"

func TestParseRequiresNodeID(t *testing.T) {
	if _, err := Parse([]string{}); err == nil {
		t.Fatal("expected error when -node-id is missing")
	}
}

func TestParseRejectsBootstrapAndJoinTogether(t *testing.T) {
	_, err := Parse([]string{"-node-id=n1", "-bootstrap", "-join=127.0.0.1:8080"})
	if err == nil {
		t.Fatal("expected error when both -bootstrap and -join are set")
	}
}

func TestParseDefaultsAndSequentialRaftAddrs(t *testing.T) {
	cfg, err := Parse([]string{"-node-id=n1"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.NodeID != "n1" {
		t.Fatalf("NodeID = %q, want n1", cfg.NodeID)
	}
	if cfg.HTTPAdvertiseAddr != cfg.HTTPAddr {
		t.Fatalf("HTTPAdvertiseAddr = %q, want to default to HTTPAddr %q", cfg.HTTPAdvertiseAddr, cfg.HTTPAddr)
	}

	want := [NumShards]string{"0.0.0.0:9001", "0.0.0.0:9002", "0.0.0.0:9003"}
	if cfg.RaftBindAddrs != want {
		t.Fatalf("RaftBindAddrs = %v, want %v", cfg.RaftBindAddrs, want)
	}
	if cfg.RaftAdvertiseAddrs != want {
		t.Fatalf("RaftAdvertiseAddrs = %v, want %v (should default to bind addrs)", cfg.RaftAdvertiseAddrs, want)
	}
}

func TestParseCustomRaftAdvertiseBase(t *testing.T) {
	cfg, err := Parse([]string{
		"-node-id=n1",
		"-raft-bind-base=0.0.0.0:9001",
		"-raft-advertise-base=node1:9001",
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := [NumShards]string{"node1:9001", "node1:9002", "node1:9003"}
	if cfg.RaftAdvertiseAddrs != want {
		t.Fatalf("RaftAdvertiseAddrs = %v, want %v", cfg.RaftAdvertiseAddrs, want)
	}
}

func TestKafkaBrokerList(t *testing.T) {
	cfg := Config{KafkaBrokers: " kafka:9092 , kafka2:9092,, "}
	got := cfg.KafkaBrokerList()
	want := []string{"kafka:9092", "kafka2:9092"}
	if len(got) != len(want) {
		t.Fatalf("KafkaBrokerList() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KafkaBrokerList()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestKafkaBrokerListEmpty(t *testing.T) {
	cfg := Config{KafkaBrokers: ""}
	if got := cfg.KafkaBrokerList(); got != nil {
		t.Fatalf("KafkaBrokerList() = %v, want nil", got)
	}
}
