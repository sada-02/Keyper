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
rm -rf test-shard-data/ ctrl1-data/ node*-data/

# Build binaries
echo -e "${YELLOW}Building binaries...${NC}"
go build -o bin/server ./cmd/server
go build -o bin/webui ./cmd/webui

# Create terminal session files for reference
mkdir -p logs

# Start Node 1
echo -e "${GREEN}Starting Node 1 (localhost:8080)...${NC}"
./bin/server \
  --node-id=node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:9080 \
  --data-dir=./node1-data \
  --enable-raft \
  > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "Node 1 PID: $NODE1_PID"

sleep 2

# Start Node 2
echo -e "${GREEN}Starting Node 2 (localhost:8081)...${NC}"
./bin/server \
  --node-id=node2 \
  --http-addr=:8081 \
  --raft-addr=127.0.0.1:9081 \
  --data-dir=./node2-data \
  --join=http://localhost:8080 \
  --enable-raft \
  > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "Node 2 PID: $NODE2_PID"

sleep 2

# Start Node 3
echo -e "${GREEN}Starting Node 3 (localhost:8082)...${NC}"
./bin/server \
  --node-id=node3 \
  --http-addr=:8082 \
  --raft-addr=127.0.0.1:9082 \
  --data-dir=./node3-data \
  --join=http://localhost:8080 \
  --enable-raft \
  > logs/node3.log 2>&1 &
NODE3_PID=$!
echo "Node 3 PID: $NODE3_PID"

sleep 3

# Start Web UI
echo -e "${BLUE}Starting Web UI (localhost:9000)...${NC}"
WEBUI_PORT=9000 ./bin/webui > logs/webui.log 2>&1 &
WEBUI_PID=$!
echo "Web UI PID: $WEBUI_PID"

# Save PIDs for cleanup
echo "$NODE1_PID" > logs/node1.pid
echo "$NODE2_PID" > logs/node2.pid
echo "$NODE3_PID" > logs/node3.pid
echo "$WEBUI_PID" > logs/webui.pid

sleep 2

echo ""
echo -e "${GREEN}✅ Keyper Demo Environment Started!${NC}"
echo "========================================"
echo ""
echo -e "${BLUE}🌐 Web Interfaces:${NC}"
echo "   Server Monitor:  http://localhost:9000/server"
echo "   Client Interface: http://localhost:9000/client"
echo ""
echo -e "${BLUE}📡 API Endpoints:${NC}"
echo "   Node 1: http://localhost:8080"
echo "   Node 2: http://localhost:8081"
echo "   Node 3: http://localhost:8082"
echo ""
echo -e "${YELLOW}📋 Log Files:${NC}"
echo "   Node 1: logs/node1.log"
echo "   Node 2: logs/node2.log"
echo "   Node 3: logs/node3.log"
echo "   Web UI: logs/webui.log"
echo ""
echo -e "${YELLOW}🛑 To stop all services:${NC}"
echo "   ./scripts/stop-web-demo.sh"
echo ""
echo -e "${GREEN}Opening Web UI in browser...${NC}"

# Try to open browser (works on most Linux systems)
if command -v xdg-open > /dev/null; then
    xdg-open http://localhost:9000/server > /dev/null 2>&1 &
elif command -v gnome-open > /dev/null; then
    gnome-open http://localhost:9000/server > /dev/null 2>&1 &
fi

# Wait for user interrupt
echo ""
echo "Press Ctrl+C to stop all services..."
trap 'echo ""; echo "Stopping services..."; ./scripts/stop-web-demo.sh; exit 0' INT

# Keep script running
tail -f /dev/null
