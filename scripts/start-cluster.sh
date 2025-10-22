#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

# clean previous state but keep data by default
./scripts/clean.sh

mkdir -p node1-data node2-data node3-data

echo "Starting node1 (bootstrap) ..."
nohup go run ./cmd/server --data-dir ./node1-data --http-addr :8080 --node-id node1 \
  --enable-raft --raft-addr 127.0.0.1:12000 > node1.log 2>&1 &
echo $! > node1.pid
sleep 2

echo "Waiting for node1 leader..."
for i in {1..20}; do
  s=$(curl -s http://127.0.0.1:8080/v1/status || true)
  echo "$s"
  if echo "$s" | grep -q '"is_leader":true'; then
    echo "node1 is leader"
    break
  fi
  sleep 1
done

echo "Starting node2 (auto-join to node1) ..."
nohup go run ./cmd/server --data-dir ./node2-data --http-addr :8081 --node-id node2 \
  --enable-raft --raft-addr 127.0.0.1:12001 --join http://127.0.0.1:8080 > node2.log 2>&1 &
echo $! > node2.pid
sleep 1

echo "Starting node3 (auto-join to node1) ..."
nohup go run ./cmd/server --data-dir ./node3-data --http-addr :8082 --node-id node3 \
  --enable-raft --raft-addr 127.0.0.1:12002 --join http://127.0.0.1:8080 > node3.log 2>&1 &
echo $! > node3.pid
sleep 3

echo "Cluster started. PID files: node1.pid node2.pid node3.pid"
echo "Node statuses:"
curl -s http://127.0.0.1:8080/v1/status || true; echo
curl -s http://127.0.0.1:8081/v1/status || true; echo
curl -s http://127.0.0.1:8082/v1/status || true; echo

echo "start-cluster.sh done."
