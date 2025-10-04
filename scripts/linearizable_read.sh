#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); cd "$ROOT"

# ensure cluster running (start if missing)
if [ ! -f node1.pid ]; then
  echo "Cluster not running. Starting cluster..."
  ./scripts/start-cluster.sh
  sleep 3
fi

echo "Linearizable read check: write then immediate read from leader"
curl -s -X PUT http://127.0.0.1:8080/v1/keys/lin_key -d 'lin_val' -o /dev/null -w "\n"
# read from node1 (leader expected)
echo "GET from node1:"
curl -s -i http://127.0.0.1:8080/v1/keys/lin_key || true
echo
# read from node2 (should redirect if follower)
echo "GET from node2 (expected redirect to leader or other):"
curl -s -i http://127.0.0.1:8081/v1/keys/lin_key || true
echo
echo "linearizable_read.sh done."
