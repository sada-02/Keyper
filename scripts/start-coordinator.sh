#!/bin/bash

# Start Migration Coordinator for Keyper
# Coordinates zero-downtime shard migrations

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "🔄 Starting Keyper Migration Coordinator"
echo "========================================"

# Build coordinator if needed
if [ ! -f "bin/migrate_coordinator" ]; then
    echo "Building migration coordinator..."
    go build -o bin/migrate_coordinator ./cmd/migrate_coordinator
fi

mkdir -p logs

# Start coordinator
echo "Starting coordinator on :7000..."
nohup ./bin/migrate_coordinator \
    --addr=:7000 \
    > logs/coordinator.log 2>&1 &
echo $! > logs/coordinator.pid

sleep 2

PID=$(cat logs/coordinator.pid)
echo ""
echo "✅ Migration Coordinator Started!"
echo "========================================"
echo ""
echo "🌐 Coordinator API: http://localhost:7000"
echo "📋 Log File: logs/coordinator.log"
echo "⚙️  PID: $PID"
echo ""
echo "🔧 Migration Commands:"
echo ""
echo "  1. Register nodes with coordinator:"
echo "     for port in 8080 8081 8082; do"
echo "       curl -X POST http://localhost:7000/register \\"
echo "         -d '{\"node_id\":\"node\$((port-8080+1))\",\"http_addr\":\"localhost:\$port\"}'"
echo "     done"
echo ""
echo "  2. Start a migration (shard-0 from node1 to node2):"
echo "     curl -X POST http://localhost:7000/migrate \\"
echo "       -d '{\"shard_id\":\"shard-0\",\"from_node\":\"node1\",\"to_node\":\"node2\"}'"
echo ""
echo "  3. Check migration status:"
echo "     curl http://localhost:7000/migrations"
echo ""
echo "  4. Get cluster view:"
echo "     curl http://localhost:7000/cluster"
echo ""
echo "🛑 To stop:"
echo "   kill $PID"
echo ""
