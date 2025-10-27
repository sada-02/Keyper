#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd
); cd "$ROOT"

echo "Running integration_failover.sh: fresh cluster -> write -> kill leader -> check failover"

# Start fresh cluster
./scripts/clean.sh --rm-data
./scripts/start-cluster.sh

# Wait a bit for cluster to stabilize
sleep 4

# Find current leader (query node1 first)
leader_addr=$(curl -s http://127.0.0.1:8080/v1/status | grep -o '"leader_addr":"[^"]*"' | sed 's/.*://"//' | sed 's/"//')
if [ -z "$leader_addr" ]; then
  echo "Could not determine leader from node1; falling back to 127.0.0.1:8080"
  leader_addr="http://127.0.0.1:8080"
fi
echo "Leader reported: $leader_addr"

# Write a key to the leader (we expect leader to accept writes)
echo "PUT key 'integration_key' -> 'integration_val' to leader (via node1's HTTP)..."
curl -s -X PUT http://127.0.0.1:8080/v1/keys/integration_key -d 'integration_val' -o /dev/null -w "\n"

# Give some time to replicate
sleep 1

# Determine actual leader process to kill: try node1 first
node1_status=$(curl -s http://127.0.0.1:8080/v1/status || true)
if echo "$node1_status" | grep -q '"is_leader":true'; then
  echo "Killing node1 (leader)"
  kill "$(cat node1.pid)" || true
else
  echo "node1 is not leader, inspecting node2/node3..."
  if curl -s http://127.0.0.1:8081/v1/status | grep -q '"is_leader":true'; then
    echo "Killing node2"
    kill "$(cat node2.pid)" || true
  elif curl -s http://127.0.0.1:8082/v1/status | grep -q '"is_leader":true'; then
    echo "Killing node3"
    kill "$(cat node3.pid)" || true
  else
    echo "No leader found to kill. Exiting."
    exit 2
  fi
fi

# Wait for a new leader
echo "Waiting for new leader election..."
newleader=""
for i in {1..30}; do
  s1=$(curl -s http://127.0.0.1:8081/v1/status || true)
  if echo "$s1" | grep -q '"is_leader":true'; then newleader="http://127.0.0.1:8081"; break; fi
  s2=$(curl -s http://127.0.0.1:8082/v1/status || true)
  if echo "$s2" | grep -q '"is_leader":true'; then newleader="http://127.0.0.1:8082"; break; fi
  sleep 1
done

if [ -z "$newleader" ]; then
  echo "No new leader elected after waiting — FAIL"
  tail -n 200 node1.log node2.log node3.log || true
  exit 2
fi

echo "New leader: $newleader"
echo "Trying to GET key from new leader..."
val=$(curl -s "$newleader/v1/keys/integration_key" || echo "")
if [ "$val" = "integration_val" ]; then
  echo "FAILOVER TEST PASS: value present after leader died"
  # keep cluster running or cleanup depending on environment
  # stop remaining nodes:
  kill "$(cat node2.pid)" 2>/dev/null || true
  kill "$(cat node3.pid)" 2>/dev/null || true
  rm -f node1.pid node2.pid node3.pid
  exit 0
else
  echo "FAILOVER TEST FAIL: expected 'integration_val' got: '$val'"
  tail -n 200 node1.log node2.log node3.log || true
  exit 2
fi
