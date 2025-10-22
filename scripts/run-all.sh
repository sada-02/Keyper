#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); cd "$ROOT"

echo "1) Running unit tests..."
./scripts/run-unit-tests.sh

echo "2) Starting cluster..."
./scripts/clean.sh --rm-data
./scripts/start-cluster.sh
sleep 3

echo "3) Running sharded-test..."
./scripts/run-sharded-test.sh

echo "4) Running linearizable read check..."
./scripts/linearizable_read.sh

echo "5) Running integration failover test..."
./scripts/integration_failover.sh

echo "Done. cluster state may be cleaned by scripts/stop-cluster.sh"
