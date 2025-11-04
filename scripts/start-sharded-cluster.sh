#!/bin/bash

# Keyper Sharded Cluster Startup Script
# Starts a 3-node cluster with 4 shards per node
# Each shard has its own Raft consensus group

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

SHARD_COUNT=4
NODES=3

echo "🚀 Starting Keyper Sharded Cluster"
echo "========================================"
echo "Configuration:"
echo "  - Nodes: $NODES"
echo "  - Shards per node: $SHARD_COUNT"
echo "  - Total shards: $SHARD_COUNT (replicated across all nodes)"
echo ""

# Clean old data
echo "Cleaning old data..."
rm -rf node*-data/ logs/*.log logs/*.pid 2>/dev/null || true
mkdir -p logs

# Build binaries if needed
if [ ! -f "bin/server" ]; then
    echo "Building server binary..."
    go build -o bin/server ./cmd/server
fi

if [ ! -f "bin/migrate_coordinator" ]; then
    echo "Building migration coordinator binary..."
    go build -o bin/migrate_coordinator ./cmd/migrate_coordinator
fi

# Start Node 1 (Bootstrap node)
echo ""
echo "Starting Node 1 (Bootstrap/Leader candidate)..."
nohup ./bin/server \
    --node-id=node1 \
    --http-addr=:8080 \
    --raft-addr=127.0.0.1:9080 \
    --data-dir=./node1-data \
    --enable-raft \
    --shard-count=$SHARD_COUNT \
    --raft-base-port=10080 \
    > logs/node1.log 2>&1 &
echo $! > logs/node1.pid
echo "Node 1 PID: $(cat logs/node1.pid)"

# Wait for node1 to start
sleep 3

# Start Node 2
echo ""
echo "Starting Node 2..."
nohup ./bin/server \
    --node-id=node2 \
    --http-addr=:8081 \
    --raft-addr=127.0.0.1:9081 \
    --data-dir=./node2-data \
    --join=http://localhost:8080 \
    --enable-raft \
    --shard-count=$SHARD_COUNT \
    --raft-base-port=10180 \
    > logs/node2.log 2>&1 &
echo $! > logs/node2.pid
echo "Node 2 PID: $(cat logs/node2.pid)"

sleep 2

# Start Node 3
echo ""
echo "Starting Node 3..."
nohup ./bin/server \
    --node-id=node3 \
    --http-addr=:8082 \
    --raft-addr=127.0.0.1:9082 \
    --data-dir=./node3-data \
    --join=http://localhost:8080 \
    --enable-raft \
    --shard-count=$SHARD_COUNT \
    --raft-base-port=10280 \
    > logs/node3.log 2>&1 &
echo $! > logs/node3.pid
echo "Node 3 PID: $(cat logs/node3.pid)"

echo ""
echo "✅ Keyper Sharded Cluster Started!"
echo "========================================"
echo ""
echo "🌐 HTTP API Endpoints:"
echo "   Node 1: http://localhost:8080"
echo "   Node 2: http://localhost:8081"
echo "   Node 3: http://localhost:8082"
echo ""
echo "🔗 Raft Addresses:"
echo "   Node 1: 127.0.0.1:9080"
echo "   Node 2: 127.0.0.1:9081"
echo "   Node 3: 127.0.0.1:9082"
echo ""
echo "📦 Shard Raft Ports:"
echo "   Node 1 Shards: 10080-10083"
echo "   Node 2 Shards: 10180-10183"
echo "   Node 3 Shards: 10280-10283"
echo ""
echo "📋 Log Files:"
echo "   Node 1: logs/node1.log"
echo "   Node 2: logs/node2.log"
echo "   Node 3: logs/node3.log"
echo ""
echo "🔧 Shard Management:"
echo "   List shards:      curl http://localhost:8080/v1/shards"
echo "   Check shard 0:    curl http://localhost:8080/v1/shards/shard-0/status"
echo "   Add shard to node: curl -X POST http://localhost:8080/v1/shards/shard-0/add"
echo ""
echo "🔑 Test Key Operations (keys automatically distributed):"
echo "   PUT key:  curl -X PUT http://localhost:8080/v1/keys/user:123 -d 'value'"
echo "   GET key:  curl http://localhost:8080/v1/keys/user:123"
echo "   DELETE:   curl -X DELETE http://localhost:8080/v1/keys/user:123"
echo ""
echo "🛑 To stop all services:"
echo "   ./scripts/stop-sharded-cluster.sh"
echo ""
echo "⏳ Waiting for cluster to stabilize (5 seconds)..."
sleep 5

# Check status
echo ""
echo "📊 Cluster Status:"
for port in 8080 8081 8082; do
    echo -n "   Node on :$port - "
    curl -s http://localhost:$port/v1/status 2>/dev/null | grep -o '"node_id":"[^"]*"' || echo "Not responding"
done

echo ""
echo "✨ Cluster is ready! Try inserting some keys to see automatic sharding."
