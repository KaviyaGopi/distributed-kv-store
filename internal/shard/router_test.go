package shard

import (
	"fmt"
	"testing"
)

func TestShardForIsDeterministic(t *testing.T) {
	r := NewRouter(3)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		first := r.ShardFor(key)
		second := r.ShardFor(key)
		if first != second {
			t.Fatalf("ShardFor(%q) not deterministic: %d != %d", key, first, second)
		}
	}
}

func TestShardForInRange(t *testing.T) {
	r := NewRouter(3)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		s := r.ShardFor(key)
		if s < 0 || s >= r.Count() {
			t.Fatalf("ShardFor(%q) = %d, out of range [0,%d)", key, s, r.Count())
		}
	}
}

func TestShardDistributionIsReasonablyBalanced(t *testing.T) {
	r := NewRouter(3)
	counts := make([]int, r.Count())
	const n = 3000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		counts[r.ShardFor(key)]++
	}

	// With a decent hash and 3000 keys across 3 shards, each shard should
	// land reasonably close to the 1000-key average; allow generous slack
	// to avoid a flaky test while still catching a broken hash (e.g. one
	// that always returns 0).
	for i, c := range counts {
		if c < n/6 {
			t.Fatalf("shard %d got %d keys, suspiciously low (want roughly %d)", i, c, n/r.Count())
		}
	}
}

func TestNewRouterPanicsOnNonPositiveCount(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for count <= 0")
		}
	}()
	NewRouter(0)
}
