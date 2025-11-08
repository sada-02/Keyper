#!/bin/bash

# Keyper Web Demo Launcher
# This script starts a 3-node Keyper cluster and web UI for testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🚀 Starting Keyper Web Demo Environment"
echo "========================================"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Clean up old data
echo -e "${YELLOW}Cleaning old data...${NC}"
cd "$PROJECT_ROOT"
rm -rf test-shard-data/ ctrl1-data/ node*-data/ cluster*-node*-data/

# Build binaries
echo -e "${YELLOW}Building binaries...${NC}"
go build -o bin/server ./cmd/server
go build -o bin/webui ./cmd/webui

# Create terminal session files for reference
mkdir -p logs

# Start Cluster 0 - Nodes 1-3 (Shard 0)
echo -e "${GREEN}Starting Cluster 0 - Node 1 (localhost:8080, Shard 0)...${NC}"
./bin/server \
  --node-id=cluster0-node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:9080 \
  --data-dir=./cluster0-node1-data \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="0" \
  --raft-base-port=12000 \
  > logs/cluster0-node1.log 2>&1 &
NODE1_PID=$!
echo "Cluster 0 Node 1 PID: $NODE1_PID"

sleep 2

echo -e "${GREEN}Starting Cluster 0 - Node 2 (localhost:8081, Shard 0)...${NC}"
./bin/server \
  --node-id=cluster0-node2 \
  --http-addr=:8081 \
  --raft-addr=127.0.0.1:9081 \
  --data-dir=./cluster0-node2-data \
  --join=http://localhost:8080 \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="0" \
  --raft-base-port=12000 \
  > logs/cluster0-node2.log 2>&1 &
NODE2_PID=$!
echo "Cluster 0 Node 2 PID: $NODE2_PID"

sleep 2

echo -e "${GREEN}Starting Cluster 0 - Node 3 (localhost:8082, Shard 0)...${NC}"
./bin/server \
  --node-id=cluster0-node3 \
  --http-addr=:8082 \
  --raft-addr=127.0.0.1:9082 \
  --data-dir=./cluster0-node3-data \
  --join=http://localhost:8080 \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="0" \
  --raft-base-port=12000 \
  > logs/cluster0-node3.log 2>&1 &
NODE3_PID=$!
echo "Cluster 0 Node 3 PID: $NODE3_PID"

sleep 2

# Start Cluster 1 - Nodes 4-6 (Shard 1)
echo -e "${GREEN}Starting Cluster 1 - Node 4 (localhost:8083, Shard 1)...${NC}"
./bin/server \
  --node-id=cluster1-node4 \
  --http-addr=:8083 \
  --raft-addr=127.0.0.1:9083 \
  --data-dir=./cluster1-node4-data \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="1" \
  --raft-base-port=12000 \
  > logs/cluster1-node4.log 2>&1 &
NODE4_PID=$!
echo "Cluster 1 Node 4 PID: $NODE4_PID"

sleep 2

echo -e "${GREEN}Starting Cluster 1 - Node 5 (localhost:8084, Shard 1)...${NC}"
./bin/server \
  --node-id=cluster1-node5 \
  --http-addr=:8084 \
  --raft-addr=127.0.0.1:9084 \
  --data-dir=./cluster1-node5-data \
  --join=http://localhost:8083 \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="1" \
  --raft-base-port=12000 \
  > logs/cluster1-node5.log 2>&1 &
NODE5_PID=$!
echo "Cluster 1 Node 5 PID: $NODE5_PID"

sleep 2

echo -e "${GREEN}Starting Cluster 1 - Node 6 (localhost:8085, Shard 1)...${NC}"
./bin/server \
  --node-id=cluster1-node6 \
  --http-addr=:8085 \
  --raft-addr=127.0.0.1:9085 \
  --data-dir=./cluster1-node6-data \
  --join=http://localhost:8083 \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="1" \
  --raft-base-port=12000 \
  > logs/cluster1-node6.log 2>&1 &
NODE6_PID=$!
echo "Cluster 1 Node 6 PID: $NODE6_PID"

sleep 2

# Start Cluster 2 - Nodes 7-9 (Shard 2)
echo -e "${GREEN}Starting Cluster 2 - Node 7 (localhost:8086, Shard 2)...${NC}"
./bin/server \
  --node-id=cluster2-node7 \
  --http-addr=:8086 \
  --raft-addr=127.0.0.1:9086 \
  --data-dir=./cluster2-node7-data \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="2" \
  --raft-base-port=12000 \
  > logs/cluster2-node7.log 2>&1 &
NODE7_PID=$!
echo "Cluster 2 Node 7 PID: $NODE7_PID"

sleep 2

echo -e "${GREEN}Starting Cluster 2 - Node 8 (localhost:8087, Shard 2)...${NC}"
./bin/server \
  --node-id=cluster2-node8 \
  --http-addr=:8087 \
  --raft-addr=127.0.0.1:9087 \
  --data-dir=./cluster2-node8-data \
  --join=http://localhost:8086 \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="2" \
  --raft-base-port=12000 \
  > logs/cluster2-node8.log 2>&1 &
NODE8_PID=$!
echo "Cluster 2 Node 8 PID: $NODE8_PID"

sleep 2

echo -e "${GREEN}Starting Cluster 2 - Node 9 (localhost:8088, Shard 2)...${NC}"
./bin/server \
  --node-id=cluster2-node9 \
  --http-addr=:8088 \
  --raft-addr=127.0.0.1:9088 \
  --data-dir=./cluster2-node9-data \
  --join=http://localhost:8086 \
  --enable-raft \
  --shard-count=6 \
  --assigned-shards="2" \
  --raft-base-port=12000 \
  > logs/cluster2-node9.log 2>&1 &
NODE9_PID=$!
echo "Cluster 2 Node 9 PID: $NODE9_PID"

sleep 3

# Start Web UI
echo -e "${BLUE}Starting Web UI (localhost:9000)...${NC}"
WEBUI_PORT=9000 ./bin/webui > logs/webui.log 2>&1 &
WEBUI_PID=$!
echo "Web UI PID: $WEBUI_PID"

# Save PIDs for cleanup
echo "$NODE1_PID" > logs/cluster0-node1.pid
echo "$NODE2_PID" > logs/cluster0-node2.pid
echo "$NODE3_PID" > logs/cluster0-node3.pid
echo "$NODE4_PID" > logs/cluster1-node4.pid
echo "$NODE5_PID" > logs/cluster1-node5.pid
echo "$NODE6_PID" > logs/cluster1-node6.pid
echo "$NODE7_PID" > logs/cluster2-node7.pid
echo "$NODE8_PID" > logs/cluster2-node8.pid
echo "$NODE9_PID" > logs/cluster2-node9.pid
echo "$WEBUI_PID" > logs/webui.pid

sleep 2

echo ""
echo -e "${GREEN}✅ Keyper Multi-Cluster Demo Environment Started!${NC}"
echo "========================================"
echo ""
echo -e "${BLUE}🌐 Web Dashboard:${NC}"
echo "   Multi-Cluster UI: http://localhost:9000"
echo ""
echo -e "${BLUE}📡 API Endpoints (3 Clusters, 9 Nodes):${NC}"
echo "   Cluster 0 (Shard 0):"
echo "     Node 1: http://localhost:8080"
echo "     Node 2: http://localhost:8081"
echo "     Node 3: http://localhost:8082"
echo ""
echo "   Cluster 1 (Shard 1):"
echo "     Node 4: http://localhost:8083"
echo "     Node 5: http://localhost:8084"
echo "     Node 6: http://localhost:8085"
echo ""
echo "   Cluster 2 (Shard 2):"
echo "     Node 7: http://localhost:8086"
echo "     Node 8: http://localhost:8087"
echo "     Node 9: http://localhost:8088"
echo ""
echo -e "${YELLOW}📋 Log Files:${NC}"
echo "   Cluster 0: logs/cluster0-node{1,2,3}.log"
echo "   Cluster 1: logs/cluster1-node{4,5,6}.log"
echo "   Cluster 2: logs/cluster2-node{7,8,9}.log"
echo "   Web UI: logs/webui.log"
echo ""
echo -e "${YELLOW}🛑 To stop all services:${NC}"
echo "   ./scripts/stop-web-demo.sh"
echo ""
echo -e "${GREEN}Opening Web UI in browser...${NC}"

# Try to open browser (works on most Linux systems)
if command -v xdg-open > /dev/null; then
    xdg-open http://localhost:9000 > /dev/null 2>&1 &
elif command -v gnome-open > /dev/null; then
    gnome-open http://localhost:9000 > /dev/null 2>&1 &
fi

# Wait for user interrupt
echo ""
echo "Press Ctrl+C to stop all services..."
trap 'echo ""; echo "Stopping services..."; ./scripts/stop-web-demo.sh; exit 0' INT

# Keep script running
tail -f /dev/null
