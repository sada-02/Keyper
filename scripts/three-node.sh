#!/usr/bin/env bash
set -euo pipefail

ROOT=$(pwd)
NODE1_DATA=./node1-data
NODE2_DATA=./node2-data
NODE3_DATA=./node3-data

rm -rf $NODE1_DATA $NODE2_DATA $NODE3_DATA
mkdir -p $NODE1_DATA $NODE2_DATA $NODE3_DATA

# clean old pids/logs
rm -f node1.pid node2.pid node3.pid node1.log node2.log node3.log

echo "Starting node1 (bootstrap)..."
nohup go run ./cmd/server --data-dir $NODE1_DATA --http-addr :8080 --node-id node1 --enable-raft --raft-addr 127.0.0.1:12000 > node1.log 2>&1 &
echo $! > node1.pid
sleep 2

echo "Waiting for node1 to be leader..."
for i in {1..20}; do
  s=$(curl -s http://127.0.0.1:8080/v1/status || true)
  echo "$s"
  echo "$s" | grep -q '"is_leader":true' && break
  sleep 1
done

echo "Starting node2 (auto-join node1)..."
nohup go run ./cmd/server --data-dir $NODE2_DATA --http-addr :8081 --node-id node2 --enable-raft --raft-addr 127.0.0.1:12001 --join http://127.0.0.1:8080 > node2.log 2>&1 &
echo $! > node2.pid
sleep 1

echo "Starting node3 (auto-join node1)..."
nohup go run ./cmd/server --data-dir $NODE3_DATA --http-addr :8082 --node-id node3 --enable-raft --raft-addr 127.0.0.1:12002 --join http://127.0.0.1:8080 > node3.log 2>&1 &
echo $! > node3.pid
sleep 2

echo "Give cluster some time to replicate membership..."
sleep 3

echo "Cluster statuses:"
curl -s http://127.0.0.1:8080/v1/status || true
echo
curl -s http://127.0.0.1:8081/v1/status || true
echo
curl -s http://127.0.0.1:8082/v1/status || true
echo

# Write a test key to the current leader
LEADER=$(curl -s http://127.0.0.1:8080/v1/status | grep -o '"leader_addr":"[^"]*"' || true)
echo "Leader info from node1 status: $LEADER"
echo "Writing key via node1..."
curl -s -X PUT http://127.0.0.1:8080/v1/keys/phase3_key -d 'phase3_val' -o /dev/null -w "%{http_code}\n"

echo "Killing leader (node1)..."
kill $(cat node1.pid) || true
sleep 2

echo "Waiting for a new leader..."
NEWLEADER=""
for i in {1..30}; do
  s=$(curl -s http://127.0.0.1:8081/v1/status || true)
  echo "$s"
  if echo "$s" | grep -q '"is_leader":true'; then
    NEWLEADER="127.0.0.1:8081"
    break
  fi
  s2=$(curl -s http://127.0.0.1:8082/v1/status || true)
  if echo "$s2" | grep -q '"is_leader":true'; then
    NEWLEADER="127.0.0.1:8082"
    break
  fi
  sleep 1
done

if [ -z "$NEWLEADER" ]; then
  echo "NO NEW LEADER ELECTED (FAIL)"
  tail -n 200 node1.log node2.log node3.log || true
  exit 2
fi

echo "New leader: $NEWLEADER"
echo "Reading key from new leader..."
if [[ "$NEWLEADER" =~ 8081 ]]; then
  val=$(curl -s http://127.0.0.1:8081/v1/keys/phase3_key || echo "")
elif [[ "$NEWLEADER" =~ 8082 ]]; then
  val=$(curl -s http://127.0.0.1:8082/v1/keys/phase3_key || echo "")
else
  val=$(curl -s http://$NEWLEADER/v1/keys/phase3_key || echo "")
fi

if [ "$val" = "phase3_val" ]; then
  echo "PHASE 3 TEST PASS: replication + failover OK"
  # cleanup
  kill $(cat node2.pid) 2>/dev/null || true
  kill $(cat node3.pid) 2>/dev/null || true
  rm -f node1.pid node2.pid node3.pid
  exit 0
else
  echo "PHASE 3 TEST FAIL: expected 'phase3_val', got '$val'"
  tail -n 200 node1.log node2.log node3.log || true
  exit 2
fi
