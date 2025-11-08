#!/bin/bash

# Stop Keyper Multi-Cluster Web Demo

echo "🛑 Stopping Keyper Multi-Cluster Demo Environment..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Kill processes by PID if files exist
if [ -f logs/webui.pid ]; then
    kill $(cat logs/webui.pid) 2>/dev/null && echo "✅ Stopped Web UI"
    rm logs/webui.pid
fi

# Cluster 0
for node in cluster0-node1 cluster0-node2 cluster0-node3; do
    if [ -f logs/$node.pid ]; then
        kill $(cat logs/$node.pid) 2>/dev/null && echo "✅ Stopped $node"
        rm logs/$node.pid
    fi
done

# Cluster 1
for node in cluster1-node4 cluster1-node5 cluster1-node6; do
    if [ -f logs/$node.pid ]; then
        kill $(cat logs/$node.pid) 2>/dev/null && echo "✅ Stopped $node"
        rm logs/$node.pid
    fi
done

# Cluster 2
for node in cluster2-node7 cluster2-node8 cluster2-node9; do
    if [ -f logs/$node.pid ]; then
        kill $(cat logs/$node.pid) 2>/dev/null && echo "✅ Stopped $node"
        rm logs/$node.pid
    fi
done

# Fallback: kill by process name
pkill -f "bin/server" 2>/dev/null
pkill -f "bin/webui" 2>/dev/null

echo "✅ All services stopped"
echo ""
echo "To clean data directories, run: rm -rf cluster*-node*-data node*-data"
