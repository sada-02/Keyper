#!/bin/bash
#
# Hybrid Architecture: Single Raft Cluster + Distributed Sharding
# - 6 nodes participate in ONE Raft cluster (1 leader elected from all 6)
# - Each node owns specific shards for data distribution
# - Keys distributed via CRC32 hashing to shards
#

set -e

echo "🚀 Starting Keyper Hybrid Sharding Cluster"
echo ""
echo "Architecture:"
echo "  ✓ Single Raft Cluster: All 6 nodes (1 leader elected)"
echo "  ✓ Distributed Sharding: 3 shards across 6 nodes"
echo "  ✓ Data Distribution: Keys partitioned by CRC32 hash"
echo ""
echo "Shard Assignment:"
echo "  Shard 0: node1 (primary) + node4 (replica)"
echo "  Shard 1: node2 (primary) + node5 (replica)"  
echo "  Shard 2: node3 (primary) + node6 (replica)"
echo ""

# Create necessary directories
mkdir -p logs
mkdir -p node1-data node2-data node3-data node4-data node5-data node6-data

# Clean up old processes
pkill -f "bin/server" || true
sleep 2

# Remove old PID files and data
rm -f logs/node*.pid
rm -rf node*-data/*

echo "Starting nodes..."
echo ""

# Node 1 - Bootstrap node (Shard 0)
echo "▶️  Starting node1 (Raft bootstrap + Shard 0)..."
nohup ./bin/server \
    --node-id=node1 \
    --http-addr=:8080 \
    --raft-addr=127.0.0.1:9080 \
    --data-dir=node1-data \
    --enable-raft \
    --shard-count=3 \
    --assigned-shards=0 \
    > logs/node1.log 2>&1 &
echo $! > logs/node1.pid
sleep 2

# Node 2 - Join Raft cluster (Shard 1)
echo "▶️  Starting node2 (Join Raft + Shard 1)..."
nohup ./bin/server \
    --node-id=node2 \
    --http-addr=:8081 \
    --raft-addr=127.0.0.1:9081 \
    --data-dir=node2-data \
    --enable-raft \
    --join=http://localhost:8080 \
    --shard-count=3 \
    --assigned-shards=1 \
    > logs/node2.log 2>&1 &
echo $! > logs/node2.pid
sleep 2

# Node 3 - Join Raft cluster (Shard 2)
echo "▶️  Starting node3 (Join Raft + Shard 2)..."
nohup ./bin/server \
    --node-id=node3 \
    --http-addr=:8082 \
    --raft-addr=127.0.0.1:9082 \
    --data-dir=node3-data \
    --enable-raft \
    --join=http://localhost:8080 \
    --shard-count=3 \
    --assigned-shards=2 \
    > logs/node3.log 2>&1 &
echo $! > logs/node3.pid
sleep 2

# Node 4 - Join Raft cluster (Shard 0 replica)
echo "▶️  Starting node4 (Join Raft + Shard 0 replica)..."
nohup ./bin/server \
    --node-id=node4 \
    --http-addr=:8083 \
    --raft-addr=127.0.0.1:9083 \
    --data-dir=node4-data \
    --enable-raft \
    --join=http://localhost:8080 \
    --shard-count=3 \
    --assigned-shards=0 \
    > logs/node4.log 2>&1 &
echo $! > logs/node4.pid
sleep 1

# Node 5 - Join Raft cluster (Shard 1 replica)
echo "▶️  Starting node5 (Join Raft + Shard 1 replica)..."
nohup ./bin/server \
    --node-id=node5 \
    --http-addr=:8084 \
    --raft-addr=127.0.0.1:9084 \
    --data-dir=node5-data \
    --enable-raft \
    --join=http://localhost:8080 \
    --shard-count=3 \
    --assigned-shards=1 \
    > logs/node5.log 2>&1 &
echo $! > logs/node5.pid
sleep 1

# Node 6 - Join Raft cluster (Shard 2 replica)
echo "▶️  Starting node6 (Join Raft + Shard 2 replica)..."
nohup ./bin/server \
    --node-id=node6 \
    --http-addr=:8085 \
    --raft-addr=127.0.0.1:9085 \
    --data-dir=node6-data \
    --enable-raft \
    --join=http://localhost:8080 \
    --shard-count=3 \
    --assigned-shards=2 \
    > logs/node6.log 2>&1 &
echo $! > logs/node6.pid

echo ""
echo "⏳ Waiting for Raft cluster to elect leader..."
sleep 5

# Check which node is the Raft leader
echo ""
echo "🗳️  Raft Leader Election Results:"
for port in 8080 8081 8082 8083 8084 8085; do
    result=$(curl -s http://localhost:$port/v1/status | grep -o '"is_leader":[^,]*' || echo '"is_leader":false')
    node_id=$(curl -s http://localhost:$port/v1/status | grep -o '"node_id":"[^"]*' | cut -d'"' -f4)
    is_leader=$(echo $result | grep -o 'true\|false')
    
    if [ "$is_leader" = "true" ]; then
        echo "  ✅ $node_id (port $port) - LEADER"
    else
        echo "  ➡️  $node_id (port $port) - Follower"
    fi
done

echo ""
echo "✅ Hybrid Sharding Cluster Started!"
echo ""
echo "📊 Architecture Summary:"
echo "┌─────────────────────────────────────────────────────────┐"
echo "│  RAFT CONSENSUS (Global)                                │"
echo "│  ├─ 6 nodes in single Raft cluster                      │"
echo "│  ├─ 1 leader elected (see above)                        │"
echo "│  └─ 5 followers                                          │"
echo "│                                                          │"
echo "│  DATA DISTRIBUTION (Sharding)                            │"
echo "│  ├─ Shard 0: node1 + node4 (both have same keys)        │"
echo "│  ├─ Shard 1: node2 + node5 (both have same keys)        │"
echo "│  └─ Shard 2: node3 + node6 (both have same keys)        │"
echo "└─────────────────────────────────────────────────────────┘"
echo ""
echo "🔑 Key Distribution:"
echo "  - Each key hashed to 1 of 3 shards (CRC32 % 3)"
echo "  - Shard 0 keys only on node1 & node4"
echo "  - Shard 1 keys only on node2 & node5"
echo "  - Shard 2 keys only on node3 & node6"
echo ""
echo "✍️  Write Operations:"
echo "  - All writes must go through the Raft LEADER"
echo "  - Leader determines shard, writes to appropriate nodes"
echo "  - Raft ensures consistency across replicas"
echo ""
echo "📖 Read Operations:"
echo "  - Can read from any node that owns the shard"
echo "  - Linearizable reads go through leader"
echo ""
echo "🎯 Test Commands:"
echo "  # Insert key (auto-routes to leader)"
echo "  curl -X PUT http://localhost:8080/v1/keys/mykey -d 'myvalue'"
echo ""
echo "  # Check Raft status"
echo "  curl http://localhost:8080/v1/status"
echo ""
echo "  # Check shard ownership"
echo "  curl http://localhost:8080/v1/shards"
echo ""
echo "🌐 Web Dashboard:"
echo "  ./bin/webui  # Then open http://localhost:9000"
echo ""
