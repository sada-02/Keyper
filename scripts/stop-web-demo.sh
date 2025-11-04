#!/bin/bash

# Stop Keyper Web Demo

echo "🛑 Stopping Keyper Web Demo Environment..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Kill processes by PID if files exist
if [ -f logs/webui.pid ]; then
    kill $(cat logs/webui.pid) 2>/dev/null && echo "✅ Stopped Web UI"
    rm logs/webui.pid
fi

if [ -f logs/node1.pid ]; then
    kill $(cat logs/node1.pid) 2>/dev/null && echo "✅ Stopped Node 1"
    rm logs/node1.pid
fi

if [ -f logs/node2.pid ]; then
    kill $(cat logs/node2.pid) 2>/dev/null && echo "✅ Stopped Node 2"
    rm logs/node2.pid
fi

if [ -f logs/node3.pid ]; then
    kill $(cat logs/node3.pid) 2>/dev/null && echo "✅ Stopped Node 3"
    rm logs/node3.pid
fi

# Fallback: kill by process name
pkill -f "bin/server" 2>/dev/null
pkill -f "bin/webui" 2>/dev/null

echo "✅ All services stopped"
echo ""
echo "To clean data directories, run: ./scripts/clean.sh"
