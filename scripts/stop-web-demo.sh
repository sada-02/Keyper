#!/bin/bash

# Stop Keyper Multi-Cluster Web Demo
# Usage: ./stop-web-demo.sh [--clean]
#   --clean: Also remove all logs and data directories

CLEAN_DATA=false

# Parse arguments
if [[ "$1" == "--clean" ]]; then
    CLEAN_DATA=true
fi

echo "🛑 Stopping Keyper Multi-Cluster Demo Environment..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Kill processes by PID if files exist
if [ -f logs/webui.pid ]; then
    kill $(cat logs/webui.pid) 2>/dev/null && echo "✅ Stopped Web UI"
    rm logs/webui.pid
fi

# Stop all cluster nodes (dynamically discover from PID files)
echo "🔍 Discovering and stopping all cluster nodes..."
stopped_count=0

# Stop new-format cluster nodes (cluster0-node1, etc.)
for pidfile in logs/cluster*.pid; do
    if [ -f "$pidfile" ]; then
        node_name=$(basename "$pidfile" .pid)
        pid=$(cat "$pidfile" 2>/dev/null)
        
        if [ -n "$pid" ]; then
            # Try to kill the process
            if kill "$pid" 2>/dev/null; then
                echo "✅ Stopped $node_name (PID: $pid)"
                ((stopped_count++))
            else
                # Process already dead, just note it
                echo "⚠️  $node_name process not running (PID: $pid)"
            fi
        fi
        
        # Remove PID file (whether process was running or not)
        rm "$pidfile"
    fi
done

# Also stop old-format nodes (node1, node2, etc.) if they exist
for pidfile in logs/node*.pid; do
    if [ -f "$pidfile" ]; then
        node_name=$(basename "$pidfile" .pid)
        pid=$(cat "$pidfile" 2>/dev/null)
        
        if [ -n "$pid" ]; then
            if kill "$pid" 2>/dev/null; then
                echo "✅ Stopped $node_name (PID: $pid) [legacy]"
                ((stopped_count++))
            else
                echo "⚠️  $node_name process not running (PID: $pid) [legacy]"
            fi
        fi
        
        # Remove PID file
        rm "$pidfile"
    fi
done

if [ $stopped_count -eq 0 ]; then
    echo "ℹ️  No cluster nodes found in PID files"
fi

# Fallback: kill any remaining processes by name
remaining=$(pkill -f "bin/server" 2>/dev/null && echo "killed" || echo "none")
if [ "$remaining" = "killed" ]; then
    echo "⚠️  Killed additional server processes by name"
fi

remaining=$(pkill -f "bin/webui" 2>/dev/null && echo "killed" || echo "none")
if [ "$remaining" = "killed" ]; then
    echo "⚠️  Killed additional webui processes by name"
fi

echo ""
echo "✅ All services stopped and PID files cleaned up"

# Clean up logs and data if requested
if [ "$CLEAN_DATA" = true ]; then
    echo ""
    echo "🧹 Cleaning up logs and data directories..."
    
    # Count files before cleanup
    cluster_log_count=$(ls -1 logs/cluster*.log 2>/dev/null | wc -l)
    old_log_count=$(ls -1 logs/node*.log 2>/dev/null | wc -l)
    data_count=$(ls -d cluster*-node*-data 2>/dev/null | wc -l)
    
    # Remove cluster log files (new format)
    if [ $cluster_log_count -gt 0 ]; then
        rm -f logs/cluster*.log
        echo "🗑️  Removed $cluster_log_count cluster log files"
    fi
    
    # Remove old-format node log files
    if [ $old_log_count -gt 0 ]; then
        rm -f logs/node*.log
        echo "🗑️  Removed $old_log_count legacy node log files"
    fi
    
    # Remove cluster data directories
    if [ $data_count -gt 0 ]; then
        rm -rf cluster*-node*-data
        echo "🗑️  Removed $data_count cluster data directories"
    fi
    
    # Also remove any old node data directories (legacy format)
    old_data_count=$(ls -d node*-data 2>/dev/null | wc -l)
    if [ $old_data_count -gt 0 ]; then
        rm -rf node*-data
        echo "🗑️  Removed $old_data_count old-format data directories"
    fi
    
    # Also remove any old shard data directories (very old format)
    shard_data_count=$(ls -d shard*-node* 2>/dev/null | wc -l)
    if [ $shard_data_count -gt 0 ]; then
        rm -rf shard*-node*
        echo "🗑️  Removed $shard_data_count shard data directories"
    fi
    
    # Remove webui.log if it exists (optional but thorough cleanup)
    if [ -f logs/webui.log ]; then
        rm -f logs/webui.log
        echo "🗑️  Removed webui.log"
    fi
    
    echo "✨ Complete cleanup finished!"
else
    echo ""
    echo "💡 To also remove logs and data directories, run:"
    echo "   ./scripts/stop-web-demo.sh --clean"
fi
echo ""
