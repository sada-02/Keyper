#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); cd "$ROOT"

echo "Running go vet..."
go vet ./...

echo "Running unit tests (with race detector)..."
go test ./... -race
echo "Unit tests done."
