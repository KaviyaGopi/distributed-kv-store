// Package shard implements static key-to-shard routing for the partitioned
// key-value store. The number of shards is fixed at cluster-creation time;
// there is no dynamic resharding.
package shard

import "hash/fnv"

// Router maps keys to a fixed number of shards.
type Router struct {
	count int
}

// NewRouter returns a Router that distributes keys across count shards.
// count must be greater than zero.
func NewRouter(count int) *Router {
	if count <= 0 {
		panic("shard: count must be greater than zero")
	}
	return &Router{count: count}
}

// Count returns the number of shards.
func (r *Router) Count() int {
	return r.count
}

// ShardFor returns the shard index (in [0, Count())) that owns key.
func (r *Router) ShardFor(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(r.count))
}
