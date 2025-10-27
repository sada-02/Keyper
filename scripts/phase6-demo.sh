#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

# clean previous
./scripts/clean.sh --rm-data || true

mkdir -p node1-data node2-data

# node1, shard-base ports starting at 13000
nohup go run ./cmd/server --data-dir ./node1-data --http-addr :8080 --node-id node1 \
  --enable-raft --shard-count 4 --raft-base-port 13000 > node1.log 2>&1 &
echo $! > node1.pid
sleep 2

# node2, different raft-base-port to avoid clashes if running on same host
nohup go run ./cmd/server --data-dir ./node2-data --http-addr :8081 --node-id node2 \
  --enable-raft --shard-count 4 --raft-base-port 14000 > node2.log 2>&1 &
echo $! > node2.pid
sleep 3

echo "node1 shards:"
curl -s http://127.0.0.1:8080/v1/shards; echo
echo "node2 shards:"
curl -s http://127.0.0.1:8081/v1/shards; echo

echo "Shard status on node1:"
curl -s http://127.0.0.1:8080/v1/shards/status | jq . || true
echo "Shard status on node2:"
curl -s http://127.0.0.1:8081/v1/shards/status | jq . || true

echo "Run cmd/sharded_test to exercise sharded puts/gets"
