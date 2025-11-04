#!/bin/bash

# Stop Keyper Sharded Cluster

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "🛑 Stopping Keyper Sharded Cluster..."

# Stop migration coordinator if running
if [ -f "logs/coordinator.pid" ]; then
    PID=$(cat logs/coordinator.pid)
    if kill -0 $PID 2>/dev/null; then
        kill $PID
        echo "✅ Stopped Migration Coordinator (PID: $PID)"
    fi
    rm -f logs/coordinator.pid
fi

# Stop all nodes
for i in 1 2 3 4 5 6 7 8 9; do
    PIDFILE="logs/node$i.pid"
    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        if kill -0 $PID 2>/dev/null; then
            kill $PID
            echo "✅ Stopped Node $i (PID: $PID)"
        fi
        rm -f "$PIDFILE"
    fi
done

# Also stop any stray server processes
pkill -f "bin/server" 2>/dev/null && echo "✅ Stopped additional server processes"
pkill -f "bin/migrate_coordinator" 2>/dev/null && echo "✅ Stopped additional coordinator processes"

echo "✅ All services stopped"
echo ""
echo "To clean data directories, run: ./scripts/clean.sh"
