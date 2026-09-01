#!/usr/bin/env bash
# Scripted end-to-end walkthrough of the distributed KV store running under
# docker-compose: cross-shard writes/reads, a leader-kill failover, the
# Kafka event feed, and pointers to Prometheus/Grafana.
set -euo pipefail

cd "$(dirname "$0")/.."

compose() { docker compose -f deploy/docker-compose.yml "$@"; }

echo "==> Building and starting the cluster (kafka, node1-3, prometheus, grafana, events-consumer)"
compose up -d --build

all_shards_have_leader() {
  # node1's own view of /cluster/status is enough to confirm every shard
  # has a leader; that doesn't yet guarantee node2/node3 have applied every
  # entry, so callers still poll for the value they expect afterward.
  curl -sf "$1/cluster/status" 2>/dev/null \
    | python3 -c 'import json,sys; s=json.load(sys.stdin); sys.exit(0 if all(sh.get("leader") for sh in s["shards"]) else 1)'
}

echo "==> Waiting for the 3-node cluster to form quorum on every shard..."
for i in $(seq 1 60); do
  if all_shards_have_leader http://localhost:8081; then
    break
  fi
  sleep 1
done
curl -s http://localhost:8081/cluster/status | python3 -m json.tool

echo
echo "==> Writing keys (these land on different shards)"
for k in alpha beta gamma delta epsilon; do
  echo "PUT $k -> $(curl -s -X PUT "http://localhost:8081/kv/$k" -d "val-$k")"
done

echo
echo "==> Reading keys back from a different node (node2), waiting briefly for replication"
sleep 1
for k in alpha beta gamma delta epsilon; do
  echo "GET $k -> $(curl -s "http://localhost:8082/kv/$k")"
done

echo
echo "==> Waiting for the above writes to reach the Kafka change feed"
echo "    (event publishing is async and best-effort, and right after 'compose up'"
echo "     Kafka itself may still be starting -- this can take up to ~15-20s the"
echo "     first time; this is exactly why the leader isn't killed until after we"
echo "     confirm the in-flight publishes have actually landed)"
for i in $(seq 1 40); do
  count=$(docker logs kv-events-consumer 2>&1 | grep -c '^2.*event: ' || true)
  if [ "$count" -ge 5 ]; then
    break
  fi
  sleep 1
done
docker logs --tail 20 kv-events-consumer

echo
echo "==> Killing the current leader container (kv-node1) to force a failover"
docker kill kv-node1
sleep 3
echo "cluster status from node2 after failover:"
curl -s http://localhost:8082/cluster/status | python3 -m json.tool

echo
echo "==> Confirming writes still succeed and prior data survived"
echo "GET alpha (pre-failover key) -> $(curl -s http://localhost:8083/kv/alpha)"
echo "PUT zeta (post-failover write) -> $(curl -s -X PUT http://localhost:8083/kv/zeta -d val-zeta)"

echo
echo "==> Prometheus targets: http://localhost:9090/targets"
echo "==> Grafana dashboard:  http://localhost:3000/d/kv-store-overview (anonymous access enabled)"
echo
echo "==> Done. Tear down with: docker compose -f deploy/docker-compose.yml down -v"
