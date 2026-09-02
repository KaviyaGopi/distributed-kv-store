# distributed-kv-store

A partitioned, fault-tolerant key-value store in Go. Each partition is an
independent [Raft](https://raft.github.io/) consensus group
([hashicorp/raft](https://github.com/hashicorp/raft)), so writes are
linearizable and survive node failure and leader loss. Every committed
write is also published to Kafka as a downstream change feed, and cluster
health (leader state, commit latency, replication lag, Kafka publish
success) is exposed to Prometheus and visualized in a provisioned Grafana
dashboard.

```
                 ┌─────────────┐
   clients ──►   │  kvnode x3  │  HTTP API (GET/PUT/DELETE, cluster/status, admin/join)
                 │ node1 node2 │
                 │    node3    │
                 └──┬───┬───┬──┘
                    │   │   │   3 independent Raft groups (shard 0/1/2),
                    │   │   │   one voter per node per shard
                    ▼   ▼   ▼
              ┌───────────────────┐
              │   Kafka (KRaft)   │──► kv-events-consumer (demo change-feed consumer)
              └───────────────────┘
                    ▲
        /metrics ───┘
              │
        ┌─────▼─────┐      ┌─────────┐
        │ Prometheus │◄────│ Grafana │
        └───────────┘      └─────────┘
```

## Architecture

**Partitioning.** The keyspace is split across **3 fixed shards** by
`FNV-1a(key) % 3` (`internal/shard`). Each shard is its own Raft
consensus group with its own log, leader, and quorum — `hashicorp/raft`
gives you one replicated log per `raft.Raft` instance, so "partitioned"
here means running multiple independent instances, not one group with a
key-range concept bolted on. A quorum loss affecting one shard's voters
does not affect the other two shards. Every physical node hosts one Raft
peer per shard (3 nodes × 3 shards = 9 peers total, 3 per process).

**Consensus and the FSM.** `internal/fsm.KVFSM` applies `PUT`/`DELETE`
commands (JSON-encoded `internal/fsm.Command`) to an in-memory map, and
implements Raft's snapshot/restore for log compaction. `internal/raftnode`
wires a `raft.Raft` instance per shard from either in-memory stores (for
tests) or `raft-boltdb/v2` + a file snapshot store + a real TCP transport
(for actual processes), and provides the bootstrap/join operations used to
form and grow a cluster.

**Cluster formation.** The first node starts with `-bootstrap`, which
calls `raft.BootstrapCluster` once per shard with itself as the sole
voter. Every other node starts with `-join=<existing-node-http-addr>` and
calls that node's `POST /admin/join`, which runs `raft.AddVoter` against
all 3 shard leaders and returns the full known node-ID → HTTP-address map
so the joining node can resolve leader hints for shards it doesn't lead.
A write sent to a non-leader returns `409` with a `leader` hint; there is
no transparent internal proxying, by design (see Tradeoffs).

**KV API (HTTP/JSON).**

| Method   | Path              | Behavior                                                   |
|----------|-------------------|--------------------------------------------------------------|
| `GET`    | `/kv/{key}`       | 200 + value, or 404                                          |
| `PUT`    | `/kv/{key}`       | body = value; 200 + raft index, or 409 + leader hint          |
| `DELETE` | `/kv/{key}`       | 200, or 409 + leader hint                                     |
| `GET`    | `/cluster/status` | per-shard state, leader, term, applied/last log index         |
| `POST`   | `/admin/join`     | used by joining nodes; adds a voter to every shard's group     |
| `GET`    | `/healthz`        | liveness                                                       |
| `GET`    | `/metrics`        | Prometheus exposition format                                  |

**Kafka is a change feed, not part of consensus.** Raft alone guarantees
linearizability and durability. After a write is committed (`raft.Apply`
returns successfully on the leader), the HTTP handler asynchronously
publishes a `KeyChanged{op, key, value, shard, raft_index, timestamp}`
event to the `kv-events` topic, keyed by the KV key so per-key ordering
matches Kafka's per-partition ordering. The topic is created explicitly
with one partition per shard (Kafka's default auto-create gives you one
partition total, which would serialize every key). Publishing is
best-effort and fully asynchronous — a Kafka outage never blocks or fails
a write. `cmd/kv-events-consumer` is a small demo consumer that tails the
topic and prints each event, standing in for a downstream audit/analytics
consumer.

**Observability.** `internal/metrics` registers Prometheus collectors with
deliberately low-cardinality labels (`shard`, `op`, `status` — never the
KV key): `raft_state`, `raft_leader_changes_total`,
`raft_commit_latency_seconds` / `kv_request_duration_seconds`,
`raft_applied_index` / `raft_last_index` (replication lag = the
difference), `kv_requests_total`, `kafka_publish_total`. Prometheus
scrapes all 3 nodes on a static target list; Grafana's datasource and the
"KV Store Overview" dashboard are provisioned via bind-mounted config, so
`docker compose up` needs zero manual setup to see live panels.

## Running it

```sh
docker compose -f deploy/docker-compose.yml up -d --build
```

This starts Kafka (KRaft mode), a 3-node cluster (`node1` bootstraps;
`node2`/`node3` join through it), the events consumer, Prometheus, and
Grafana. Then either follow along by hand:

```sh
curl http://localhost:8081/cluster/status | python3 -m json.tool

curl -X PUT http://localhost:8081/kv/foo -d bar
curl http://localhost:8082/kv/foo                 # reads replicate to every node

docker kill kv-node1                              # kill the leader
curl http://localhost:8082/cluster/status         # a new leader is elected per shard
curl http://localhost:8083/kv/foo                 # prior writes survived
curl -X PUT http://localhost:8083/kv/baz -d qux   # the cluster still accepts writes

docker logs kv-events-consumer                    # see the Kafka change feed
```

Prometheus targets: http://localhost:9090/targets
Grafana dashboard: http://localhost:3000/d/kv-store-overview (anonymous
access is enabled for the demo; no login needed)

...or run the scripted version of the same walkthrough:

```sh
./scripts/demo.sh
```

Tear down with `docker compose -f deploy/docker-compose.yml down -v`.

### Running without Docker

```sh
go build -o bin/kvnode ./cmd/kvnode

bin/kvnode -node-id=node1 -data-dir=data1 -http-addr=127.0.0.1:8081 \
  -http-advertise-addr=127.0.0.1:8081 -raft-bind-base=127.0.0.1:9001 -bootstrap

bin/kvnode -node-id=node2 -data-dir=data2 -http-addr=127.0.0.1:8082 \
  -http-advertise-addr=127.0.0.1:8082 -raft-bind-base=127.0.0.1:9101 \
  -join=http://127.0.0.1:8081
```

Pass `-kafka-brokers=host:port` to enable event publishing.

## Testing

```sh
go test ./... -race
```

No Docker, ports, or external services required — `internal/raftnode`'s
cluster tests use `hashicorp/raft`'s in-memory transport and stores. The
main one, `TestThreeNodeClusterElectsLeaderReplicatesAndSurvivesFailover`,
is the actual proof behind the "linearizable consistency during network
partitions" claim: it bootstraps a 3-node group, replicates a write,
kills the leader, waits for re-election, and confirms both that the
cluster keeps accepting writes and that the pre-failover write survived.

## Design tradeoffs

- **3 fixed shards, no dynamic resharding.** A static shard count keeps
  routing, bootstrap, and join logic simple and testable. Real elastic
  resharding (splitting/merging shards, migrating key ranges) is a
  substantial project on its own and was deliberately cut.
- **HTTP/JSON, not gRPC.** Curl-testable with no codegen step, and the
  original scope named Raft/Kafka/Prometheus/Grafana specifically, not a
  particular RPC framework.
- **Leader-hint redirect, not transparent proxying.** A write to a
  non-leader gets a `409` naming the real leader rather than the node
  silently forwarding the request. This keeps the write path's failure
  modes obvious (you always know which node actually accepted your
  write) at the cost of one extra round trip from a naive client.
- **Kafka publish can be dropped across a node crash.** Publishing is
  asynchronous and best-effort by design (see `internal/events/producer.go`)
  so a Kafka outage never becomes a dependency of the write path. The
  flip side: if a node is killed while a publish for one of its writes is
  still retrying, that event is lost — the write itself is unaffected,
  since Raft had already committed and replicated it elsewhere. A
  write-ahead outbox replayed on restart would close this gap; it's out
  of scope here.
