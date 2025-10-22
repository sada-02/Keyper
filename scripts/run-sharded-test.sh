#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); cd "$ROOT"

# ensure cluster running
if [ ! -f node1.pid ]; then
  echo "Cluster is not running. Starting cluster..."
  ./scripts/start-cluster.sh
  sleep 3
fi

echo "Building sharded test program..."
go build -o ./bin/sharded_test ./cmd/sharded_test

echo "Running sharded_test (1000 keys by default)..."
./bin/sharded_test --nodes http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082 --nkeys 1000 --replicas 150
