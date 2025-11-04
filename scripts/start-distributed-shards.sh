#!/bin/bash
#
# Start Keyper with TRUE distributed sharding:
# - 6 nodes total
# - 3 shards (0, 1, 2)
# - Each shard has 2 replicas (primary + backup)
# - Keys are distributed across shards via consistent hashing
#

set -e

echo "🚀 Starting Keyper Distributed Sharding Cluster"
echo ""
echo "Architecture:"
echo "  Shard 0: node1 (primary) + node4 (replica)"
echo "  Shard 1: node2 (primary) + node5 (replica)"  
echo "  Shard 2: node3 (primary) + node6 (replica)"
echo ""
echo "Each shard gets DIFFERENT keys via CRC32 hashing"
echo ""

# Create necessary directories
mkdir -p logs
mkdir -p node1-data node2-data node3-data node4-data node5-data node6-data

# Clean up old processes
pkill -f "bin/server" || true
sleep 2

# Remove old PID files
rm -f logs/node*.pid

echo "Starting nodes..."
echo ""

# Shard 0 - Node 1 (Primary)
echo "▶️  Starting node1 (Shard 0 primary)..."
nohup ./bin/server \
    --node-id=node1 \
    --http-addr=:8080 \
    --raft-addr=127.0.0.1:9080 \
    --data-dir=node1-data \
    --enable-raft \
    --shard-count=3 \
    --assigned-shards=0 \
    --raft-base-port=10080 \
    > logs/node1.log 2>&1 &
echo $! > logs/node1.pid
sleep 1

# Shard 1 - Node 2 (Primary)
echo "▶️  Starting node2 (Shard 1 primary)..."
nohup ./bin/server \
    --node-id=node2 \
    --http-addr=:8081 \
    --raft-addr=127.0.0.1:9081 \
    --data-dir=node2-data \
    --enable-raft \
    --shard-count=3 \
    --assigned-shards=1 \
    --raft-base-port=10180 \
    > logs/node2.log 2>&1 &
echo $! > logs/node2.pid
sleep 1

# Shard 2 - Node 3 (Primary)
echo "▶️  Starting node3 (Shard 2 primary)..."
nohup ./bin/server \
    --node-id=node3 \
    --http-addr=:8082 \
    --raft-addr=127.0.0.1:9082 \
    --data-dir=node3-data \
    --enable-raft \
    --shard-count=3 \
    --assigned-shards=2 \
    --raft-base-port=10280 \
    > logs/node3.log 2>&1 &
echo $! > logs/node3.pid
sleep 2

# Shard 0 - Node 4 (Replica - joins node1's shard 0 raft)
echo "▶️  Starting node4 (Shard 0 replica)..."
nohup ./bin/server \
    --node-id=node4 \
    --http-addr=:8083 \
    --raft-addr=127.0.0.1:9083 \
    --data-dir=node4-data \
    --enable-raft \
    --shard-count=3 \
    --assigned-shards=0 \
    --raft-base-port=10380 \
    --join=http://localhost:8080 \
    > logs/node4.log 2>&1 &
echo $! > logs/node4.pid
sleep 1

# Shard 1 - Node 5 (Replica - joins node2's shard 1 raft)
echo "▶️  Starting node5 (Shard 1 replica)..."
nohup ./bin/server \
    --node-id=node5 \
    --http-addr=:8084 \
    --raft-addr=127.0.0.1:9084 \
    --data-dir=node5-data \
    --enable-raft \
    --shard-count=3 \
    --assigned-shards=1 \
    --raft-base-port=10480 \
    --join=http://localhost:8081 \
    > logs/node5.log 2>&1 &
echo $! > logs/node5.pid
sleep 1

# Shard 2 - Node 6 (Replica - joins node3's shard 2 raft)
echo "▶️  Starting node6 (Shard 2 replica)..."
nohup ./bin/server \
    --node-id=node6 \
    --http-addr=:8085 \
    --raft-addr=127.0.0.1:9085 \
    --data-dir=node6-data \
    --enable-raft \
    --shard-count=3 \
    --assigned-shards=2 \
    --raft-base-port=10580 \
    --join=http://localhost:8082 \
    > logs/node6.log 2>&1 &
echo $! > logs/node6.pid

echo ""
echo "⏳ Waiting for cluster to stabilize..."
sleep 3

echo ""
echo "✅ Distributed Sharding Cluster Started!"
echo ""
echo "📊 Cluster Layout:"
echo "┌─────────┬─────────┬──────────┬────────────────┐"
echo "│ Shard   │ Primary │ Replica  │ HTTP Ports     │"
echo "├─────────┼─────────┼──────────┼────────────────┤"
echo "│ Shard 0 │ node1   │ node4    │ :8080, :8083   │"
echo "│ Shard 1 │ node2   │ node5    │ :8081, :8084   │"
echo "│ Shard 2 │ node3   │ node6    │ :8082, :8085   │"
echo "└─────────┴─────────┴──────────┴────────────────┘"
echo ""
echo "🔑 Key Distribution (CRC32 hash % 3):"
echo "  - Keys with hash % 3 = 0 → Shard 0 (node1/node4)"
echo "  - Keys with hash % 3 = 1 → Shard 1 (node2/node5)"
echo "  - Keys with hash % 3 = 2 → Shard 2 (node3/node6)"
echo ""
echo "🌐 Access any node - requests auto-route to correct shard:"
echo "  curl -X PUT http://localhost:8080/v1/keys/mykey -d 'value'"
echo "  curl http://localhost:8081/v1/keys/mykey"
echo ""
echo "📋 Check which shard owns a key:"
echo "  # CRC32('mykey') % 3 determines the shard"
echo ""
echo "🎯 Web Dashboard:"
echo "  ./bin/webui  # Then open http://localhost:9000"
echo ""
