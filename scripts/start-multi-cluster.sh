#!/bin/bash

# Keyper Multi-Cluster Architecture
# - 3 independent Raft clusters
# - Each cluster: 1 leader + 2 replicas (3 nodes total)
# - Keys distributed ACROSS clusters (not within)
# - Each cluster has unique Cluster ID

set -e

echo "🚀 Starting Keyper Multi-Cluster System"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Build if needed
if [ ! -f bin/server ]; then
    echo "Building server..."
    go build -o bin/server ./cmd/server
fi

# Create logs directory
mkdir -p logs

# Clear old data
rm -rf node*-data cluster*-data
mkdir -p cluster0-data cluster1-data cluster2-data

echo "📋 Architecture:"
echo "  Cluster 0 (Shard 0): node1 (leader) + node2 + node3"
echo "  Cluster 1 (Shard 1): node4 (leader) + node5 + node6"
echo "  Cluster 2 (Shard 2): node7 (leader) + node8 + node9"
echo ""

# ============================================
# CLUSTER 0 - Shard 0 (nodes 1-3)
# ============================================
echo "🔷 Starting Cluster 0 (Shard 0)..."

# Node 1 - Cluster 0 Bootstrap (Leader)
nohup ./bin/server \
  --node-id=cluster0-node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:9080 \
  --data-dir=cluster0-data/node1 \
  --enable-raft \
  > logs/cluster0-node1.log 2>&1 &
echo $! > logs/cluster0-node1.pid
echo "  ✅ cluster0-node1 started (port 8080, raft 9080) - BOOTSTRAP"

sleep 2

# Node 2 - Cluster 0 Replica
nohup ./bin/server \
  --node-id=cluster0-node2 \
  --http-addr=:8081 \
  --raft-addr=127.0.0.1:9081 \
  --data-dir=cluster0-data/node2 \
  --enable-raft \
  --join=http://localhost:8080 \
  > logs/cluster0-node2.log 2>&1 &
echo $! > logs/cluster0-node2.pid
echo "  ✅ cluster0-node2 started (port 8081, raft 9081) - REPLICA"

# Node 3 - Cluster 0 Replica
nohup ./bin/server \
  --node-id=cluster0-node3 \
  --http-addr=:8082 \
  --raft-addr=127.0.0.1:9082 \
  --data-dir=cluster0-data/node3 \
  --enable-raft \
  --join=http://localhost:8080 \
  > logs/cluster0-node3.log 2>&1 &
echo $! > logs/cluster0-node3.pid
echo "  ✅ cluster0-node3 started (port 8082, raft 9082) - REPLICA"

sleep 2

# ============================================
# CLUSTER 1 - Shard 1 (nodes 4-6)
# ============================================
echo ""
echo "🔶 Starting Cluster 1 (Shard 1)..."

# Node 4 - Cluster 1 Bootstrap (Leader)
nohup ./bin/server \
  --node-id=cluster1-node4 \
  --http-addr=:8083 \
  --raft-addr=127.0.0.1:9083 \
  --data-dir=cluster1-data/node4 \
  --enable-raft \
  
  
  > logs/cluster1-node4.log 2>&1 &
echo $! > logs/cluster1-node4.pid
echo "  ✅ cluster1-node4 started (port 8083, raft 9083) - BOOTSTRAP"

sleep 2

# Node 5 - Cluster 1 Replica
nohup ./bin/server \
  --node-id=cluster1-node5 \
  --http-addr=:8084 \
  --raft-addr=127.0.0.1:9084 \
  --data-dir=cluster1-data/node5 \
  --enable-raft \
  
  
  --join=http://localhost:8083 \
  > logs/cluster1-node5.log 2>&1 &
echo $! > logs/cluster1-node5.pid
echo "  ✅ cluster1-node5 started (port 8084, raft 9084) - REPLICA"

# Node 6 - Cluster 1 Replica
nohup ./bin/server \
  --node-id=cluster1-node6 \
  --http-addr=:8085 \
  --raft-addr=127.0.0.1:9085 \
  --data-dir=cluster1-data/node6 \
  --enable-raft \
  
  
  --join=http://localhost:8083 \
  > logs/cluster1-node6.log 2>&1 &
echo $! > logs/cluster1-node6.pid
echo "  ✅ cluster1-node6 started (port 8085, raft 9085) - REPLICA"

sleep 2

# ============================================
# CLUSTER 2 - Shard 2 (nodes 7-9)
# ============================================
echo ""
echo "🔵 Starting Cluster 2 (Shard 2)..."

# Node 7 - Cluster 2 Bootstrap (Leader)
nohup ./bin/server \
  --node-id=cluster2-node7 \
  --http-addr=:8086 \
  --raft-addr=127.0.0.1:9086 \
  --data-dir=cluster2-data/node7 \
  --enable-raft \
  
  
  > logs/cluster2-node7.log 2>&1 &
echo $! > logs/cluster2-node7.pid
echo "  ✅ cluster2-node7 started (port 8086, raft 9086) - BOOTSTRAP"

sleep 2

# Node 8 - Cluster 2 Replica
nohup ./bin/server \
  --node-id=cluster2-node8 \
  --http-addr=:8087 \
  --raft-addr=127.0.0.1:9087 \
  --data-dir=cluster2-data/node8 \
  --enable-raft \
  
  
  --join=http://localhost:8086 \
  > logs/cluster2-node8.log 2>&1 &
echo $! > logs/cluster2-node8.pid
echo "  ✅ cluster2-node8 started (port 8087, raft 9087) - REPLICA"

# Node 9 - Cluster 2 Replica
nohup ./bin/server \
  --node-id=cluster2-node9 \
  --http-addr=:8088 \
  --raft-addr=127.0.0.1:9088 \
  --data-dir=cluster2-data/node9 \
  --enable-raft \
  
  
  --join=http://localhost:8086 \
  > logs/cluster2-node9.log 2>&1 &
echo $! > logs/cluster2-node9.pid
echo "  ✅ cluster2-node9 started (port 8088, raft 9088) - REPLICA"

sleep 3

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Multi-Cluster System Ready!"
echo ""
echo "📊 Cluster Overview:"
echo "  🔷 Cluster 0 (Shard 0): Nodes 1-3   | Ports 8080-8082"
echo "  🔶 Cluster 1 (Shard 1): Nodes 4-6   | Ports 8083-8085"
echo "  🔵 Cluster 2 (Shard 2): Nodes 7-9   | Ports 8086-8088"
echo ""
echo "🔑 Key Distribution:"
echo "  CRC32(key) % 3 = 0 → Cluster 0 (all 3 nodes replicate)"
echo "  CRC32(key) % 3 = 1 → Cluster 1 (all 3 nodes replicate)"
echo "  CRC32(key) % 3 = 2 → Cluster 2 (all 3 nodes replicate)"
echo ""
echo "🗳️  Elections:"
echo "  Each cluster has independent Raft elections"
echo "  3 leaders total (one per cluster)"
echo ""
echo "🌐 Web Dashboard: http://localhost:9000"
echo "   (Start with: nohup ./bin/webui > logs/webui.log 2>&1 &)"
