#!/usr/bin/env bash
set -euo pipefail

# safe-clean: stops node processes referenced by pid files and optionally deletes data dirs
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NODE1_PID_FILE="$ROOT/node1.pid"
NODE2_PID_FILE="$ROOT/node2.pid"
NODE3_PID_FILE="$ROOT/node3.pid"

stop_pidfile() {
  local pidfile="$1"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stopping pid $pid from $pidfile"
      kill "$pid" || true
      sleep 1
      if kill -0 "$pid" 2>/dev/null; then
        echo "Force killing $pid"
        kill -9 "$pid" || true
      fi
    fi
    rm -f "$pidfile"
  fi
}

echo "Stopping any running keyper nodes..."
stop_pidfile "$NODE1_PID_FILE"
stop_pidfile "$NODE2_PID_FILE"
stop_pidfile "$NODE3_PID_FILE"

if [ "${1:-}" = "--rm-data" ]; then
  echo "Removing data dirs..."
  rm -rf "$ROOT/node1-data" "$ROOT/node2-data" "$ROOT/node3-data"
  echo "Data dirs removed."
fi

echo "clean.sh done."