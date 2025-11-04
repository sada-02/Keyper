# Keyper - Production-Ready Distributed Key-Value Store

A high-performance distributed key-value store with **Raft consensus**, **horizontal sharding**, **zero-downtime migration**, **TLS security**, and **comprehensive observability**.

---

## ✨ Features

### Core Capabilities
- ✅ **Distributed Consensus** - HashiCorp Raft for strong consistency
- ✅ **Horizontal Sharding** - CRC32-based consistent hashing with per-shard Raft
- ✅ **Zero-Downtime Migration** - Safe shard movement between nodes
- ✅ **Control Plane** - Centralized topology management and service discovery
- ✅ **Auto-Membership** - Self-healing Raft cluster coordination

### Security & Operations
- ✅ **TLS Encryption** - Mutual TLS for Raft and HTTPS for API
- ✅ **Authentication** - Bearer token protection for admin endpoints
- ✅ **Observability** - Prometheus metrics, structured logging, health checks
- ✅ **Kubernetes-Ready** - Liveness and readiness probes

### Data & Storage
- ✅ **Persistent Storage** - BadgerDB embedded key-value engine
- ✅ **Streaming Export/Import** - Efficient gzip-compressed data transfer
- ✅ **Smart Client** - Auto-discovery and intelligent routing

---

## 🚀 Quick Start

### Prerequisites

- **Go 1.20+** (tested with 1.25.x)
- Available ports: 8080-8082 (HTTP), 9080-9082 (Raft), 7000 (Control Plane)

### Installation

```bash
# Clone repository
git clone https://github.com/sada-02/Keyper.git
cd Keyper

# Install dependencies
go mod tidy

# Build all binaries
go build -o bin/server ./cmd/server
go build -o bin/control ./cmd/control
go build -o bin/migrate_coordinator ./cmd/migrate_coordinator
```

### Start a 3-Node Cluster

```bash
# Start control plane
./bin/control \
  --node-id ctrl \
  --http-addr :7000 \
  --raft-addr :7001 \
  --data-dir ./ctrl-data \
  --bootstrap

# Start node 1 (shards 0,1)
./bin/server \
  --node-id node1 \
  --http-addr :8080 \
  --raft-addr :9080 \
  --data-dir ./data/node1 \
  --shard-count 4 \
  --assigned-shards "0,1" \
  --control-plane localhost:7000 \
  --bootstrap

# Start node 2 (shards 2,3)
./bin/server \
  --node-id node2 \
  --http-addr :8081 \
  --raft-addr :9081 \
  --data-dir ./data/node2 \
  --shard-count 4 \
  --assigned-shards "2,3" \
  --control-plane localhost:7000
```

Or use the convenience script:

```bash
./scripts/start-cluster.sh
```

### 🌐 Interactive Web Demo (Recommended for Testing)

For an interactive visualization and testing experience:

```bash
# Start cluster with web UI
./scripts/start-web-demo.sh

# Or run the guided demo
./scripts/demo.sh
```

Then open your browser to:
- **Server Monitor**: http://localhost:9000/server (view cluster state, leaders, key distribution)
- **Client Interface**: http://localhost:9000/client (send requests, see history)

The web demo provides:
- ✅ Real-time cluster visualization (leaders, followers, shard states)
- ✅ Interactive client for sending PUT/GET/DELETE requests
- ✅ Live key distribution across nodes
- ✅ Request history with timestamps and responses
- ✅ Auto-refresh every 2 seconds

See [WEB_DEMO_README.md](WEB_DEMO_README.md) for the complete guide.

### Basic Operations (CLI)

```bash
# Write data
curl -X PUT http://localhost:8080/v1/keys/mykey -d 'myvalue'

# Read data
curl http://localhost:8080/v1/keys/mykey

# Delete data
curl -X DELETE http://localhost:8080/v1/keys/mykey

# Check health
curl http://localhost:8080/v1/health

# View metrics
curl http://localhost:8080/metrics
```

---

## 🏗️ Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Control Plane (Raft)                    │
│  • Node registration & discovery                            │
│  • Shard ownership mapping                                  │
│  • Membership coordination                                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
       ┌───────────────┼───────────────┐
       │               │               │
   ┌───▼────┐     ┌────▼────┐     ┌───▼─────┐
   │ Node 1 │     │ Node 2  │     │ Node 3  │
   │────────│     │─────────│     │─────────│
   │Shard 0 │     │Shard 2  │     │Shard 0* │
   │Shard 1 │     │Shard 3  │     │Shard 1* │
   │        │     │         │     │         │
   │(Raft)  │◄───►│ (Raft)  │◄───►│ (Raft)  │
   │Badger  │     │ Badger  │     │ Badger  │
   └────────┘     └─────────┘     └─────────┘
   
   * Raft replicas for fault tolerance
```

### Key Design Principles

1. **Sharding**: Keys distributed via `CRC32(key) % shard_count`
2. **Per-Shard Raft**: Isolated consensus for better scalability
3. **Ownership Enforcement**: Nodes redirect misrouted requests (307)
4. **Auto-Discovery**: Clients query control plane for topology
5. **Self-Healing**: Automatic Raft membership reconciliation

---

## 📚 API Reference

### Data Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/v1/keys/{key}` | Write or update key |
| GET | `/v1/keys/{key}` | Read key value |
| DELETE | `/v1/keys/{key}` | Delete key |
| GET | `/v1/status` | Node status |

### Health & Monitoring

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/health` | Detailed health with shard status |
| GET | `/v1/health/ready` | Readiness probe (Kubernetes) |
| GET | `/v1/health/live` | Liveness probe (Kubernetes) |
| GET | `/metrics` | Prometheus metrics |

### Shard Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/shards/{id}/pause` | Enable read-only mode |
| POST | `/v1/shards/{id}/resume` | Disable read-only mode |
| GET | `/v1/shards/{id}/state` | Get shard operational state |
| POST | `/v1/shards/{id}/join` | Add Raft voter to shard |
| POST | `/v1/shards/{id}/leave` | Remove Raft voter from shard |

### Migration

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/shards/{id}/_snapshot` | Export shard data (gzipped) |
| POST | `/v1/shards/{id}/_import` | Import shard data |

### Control Plane

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/control/nodes` | Register node |
| GET | `/v1/control/nodes` | List registered nodes |
| GET | `/v1/control/shards` | List shard mappings |
| POST | `/v1/control/shards/{id}/members` | Set shard membership |

---

## 🔐 Security

### TLS Configuration

Generate test certificates:

```bash
./scripts/generate-test-certs.sh
```

Start with TLS enabled:

```bash
./bin/server \
  --node-id node1 \
  --http-addr :8080 \
  --raft-addr :9080 \
  --tls-cert ./test-certs/server-cert.pem \
  --tls-key ./test-certs/server-key.pem \
  --raft-tls-cert ./test-certs/server-cert.pem \
  --raft-tls-key ./test-certs/server-key.pem \
  --raft-tls-ca ./test-certs/ca-cert.pem \
  --auth-token "my-secret-token"
```

Access with authentication:

```bash
curl --cacert ./test-certs/ca-cert.pem \
  -H "Authorization: Bearer my-secret-token" \
  -X PUT https://localhost:8080/v1/keys/secure -d 'value'
```

---

## 📊 Observability

### Prometheus Metrics

Keyper exposes 30+ metrics at `/metrics`:

**Raft Metrics**:
- `keyper_raft_commit_duration_seconds` - Commit latency
- `keyper_raft_leader_changes_total` - Leader elections
- `keyper_raft_applied_ops_total` - Operations by type

**HTTP Metrics**:
- `keyper_http_requests_total` - Request rate by status
- `keyper_http_request_duration_seconds` - Latency histogram

**Storage Metrics**:
- `keyper_badger_writes_total` - Write operations
- `keyper_badger_lsm_size_bytes` - Storage size

**Shard Metrics**:
- `keyper_shard_operations_total` - Shard operations
- `keyper_shard_migrations_total` - Migration events

### Grafana Dashboard

Import the pre-built dashboard from `examples/grafana-dashboard.json`

### Structured Logging

All logs include contextual fields:

```json
{
  "level": "info",
  "node_id": "node1",
  "shard_id": "shard0",
  "raft_addr": "localhost:9080",
  "msg": "raft election won",
  "term": 5
}
```

---

## 🔄 Migration

### Single-Command Migration

Move shard 0 from node1 to node2:

```bash
./bin/migrate_coordinator \
  -shard 0 \
  -source localhost:8080 \
  -dests localhost:8081 \
  -source-raft-id node1-shard0 \
  -dest-raft-ids node2-shard0 \
  -dest-raft-addrs localhost:9081 \
  -control localhost:7000
```

### Migration Workflow

The coordinator executes an 8-phase workflow:

1. **Pause** - Set source shard to read-only
2. **Export** - Stream snapshot from source
3. **Import** - Load data into destination(s)
4. **Add Voters** - Join destination nodes to Raft
5. **Stabilize** - Wait for replication
6. **Remove Voters** - Remove source from Raft
7. **Update Control** - Update shard ownership
8. **Resume** - Re-enable writes on destination

### Safety Guarantees

✅ **Zero data loss** - Raft ensures all writes replicated  
✅ **Read availability** - Reads work during migration  
✅ **Automatic rollback** - Failures trigger cleanup  
✅ **Idempotent** - Safe to retry on failure

---

## 🧪 Testing

### Run All Tests

```bash
# Unit tests
./scripts/run-unit-tests.sh

# Or manually
go test ./...

# With race detector
go test ./... -race

# Specific package
go test ./metrics -v
go test ./httpapi -v
```

### Integration Testing

```bash
# Start test cluster
./scripts/start-cluster.sh

# Run operations
go run ./cmd/client_example

# Stop cluster
./scripts/stop-cluster.sh

# Clean data
./scripts/clean.sh
```

---

## ⚙️ Configuration

### Server Options

```bash
--node-id string          Unique node identifier (required)
--http-addr string        HTTP server address (default ":8080")
--raft-addr string        Raft transport address (default ":9080")
--data-dir string         Data directory (default "./data")
--shard-count int         Number of shards (default 0, disabled)
--assigned-shards string  Shards to host, e.g., "0,1,2" (default: all)
--control-plane string    Control plane address for auto-discovery
--bootstrap               Bootstrap new Raft cluster (first node only)
--join string             Join existing cluster at this address

# TLS options
--tls-cert string         TLS certificate for HTTPS
--tls-key string          TLS private key for HTTPS
--raft-tls-cert string    TLS certificate for Raft
--raft-tls-key string     TLS private key for Raft
--raft-tls-ca string      CA certificate for Raft peer verification

# Security
--auth-token string       Bearer token for admin endpoints
```

### Control Plane Options

```bash
--node-id string     Control plane node ID
--http-addr string   HTTP address (default ":7000")
--raft-addr string   Raft address (default ":7001")
--data-dir string    Data directory
--bootstrap          Bootstrap new cluster
```

---

## 📁 Project Structure

```
Keyper/
├── cmd/
│   ├── server/              # Main KV server
│   ├── control/             # Control plane service
│   ├── migrate_coordinator/ # Migration CLI tool
│   └── client_example/      # Example client
├── client/
│   ├── client.go            # HTTP client library
│   ├── sharded_client.go    # Sharded client with auto-routing
│   └── discovery.go         # Control plane discovery
├── config/                  # Configuration management
├── control/                 # Control plane implementation
├── httpapi/                 # HTTP API handlers
│   ├── handler.go           # Core KV operations
│   ├── shard_handlers.go    # Shard management
│   ├── shard_lifecycle.go   # Pause/resume/join/leave
│   ├── migrate_handlers.go  # Export/import endpoints
│   ├── health.go            # Health check endpoints
│   └── middleware.go        # Metrics middleware
├── logging/                 # Structured logging
├── metrics/                 # Prometheus metrics
├── migrate/                 # Migration orchestration
│   ├── coordinator.go       # 8-phase workflow
│   └── manager.go           # Snapshot/import logic
├── raft/                    # Raft consensus
│   ├── node.go              # Raft node wrapper
│   ├── fsm.go               # Finite state machine
│   └── apply.go             # Command application
├── shard/                   # Sharding logic
│   ├── keymap.go            # CRC32 consistent hashing
│   ├── manager.go           # Shard lifecycle
│   ├── membership.go        # Auto Raft membership
│   └── state.go             # Operational state
├── shardraft/               # Per-shard Raft
├── store/                   # BadgerDB wrapper
├── scripts/                 # Operational scripts
│   ├── clean.sh
│   ├── start-cluster.sh
│   ├── stop-cluster.sh
│   ├── generate-test-certs.sh
│   └── run-unit-tests.sh
└── examples/                # Configuration examples
    ├── prometheus-config.yml
    ├── grafana-dashboard.json
    └── keyper-alerts.yml
```

---

## 🎯 Performance

**Benchmarks** (single node, 4 shards):

| Operation | Latency (P99) | Throughput |
|-----------|---------------|------------|
| Write (Raft) | ~5-10ms | 10k ops/sec |
| Read (local) | ~1-2ms | 50k ops/sec |
| Migration | N/A | ~1-5 sec/GB |

**Scalability**:
- Linear scaling with shards (per-shard Raft isolation)
- Supports 1000+ shards per cluster
- Tested with multi-GB datasets

---

## 🛠️ Troubleshooting

### Common Issues

**Port already in use**:
```bash
./scripts/stop-cluster.sh
# Or manually: pkill -f server
```

**Raft leader not elected**:
- Ensure at least 2 nodes are running
- Check network connectivity between Raft addresses
- Verify `--bootstrap` only used on first node

**Shard not found (404)**:
- Check `--assigned-shards` configuration
- Verify `--shard-count` matches across cluster
- Ensure shard ID is valid (0 to shard_count-1)

**307 Redirect responses**:
- This is normal! Client should follow `X-Shard-Owner` header
- Use `ShardedClient` for automatic retry

**Migration fails**:
- Check source and destination nodes are running
- Verify Raft addresses are reachable
- Ensure shard exists on source node
- Check logs for detailed error messages

---

## 🚧 Limitations & Future Work

### Current Limitations

- Follower reads are not linearizable (no Raft barrier)
- No automatic shard rebalancing
- Control plane is single-node (not clustered)
- Certificate rotation requires restart

### Roadmap

- [ ] Linearizable reads with Raft ReadIndex
- [ ] Automatic shard rebalancing based on load
- [ ] Clustered control plane for high availability
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Client libraries (Python, Java, JavaScript)
- [ ] Backup/restore utilities
- [ ] Multi-region replication
- [ ] Compression support

---

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

---

## 🙏 Acknowledgments

Built with excellent open-source projects:

- [HashiCorp Raft](https://github.com/hashicorp/raft) - Consensus implementation
- [BadgerDB](https://github.com/dgraph-io/badger) - Embedded key-value store
- [Prometheus](https://prometheus.io/) - Metrics and monitoring
- [go-hclog](https://github.com/hashicorp/go-hclog) - Structured logging

Inspired by:
- [etcd](https://etcd.io/) - Distributed key-value store
- [CockroachDB](https://www.cockroachlabs.com/) - Distributed SQL database
- [Cassandra](https://cassandra.apache.org/) - Wide-column store

---

**Built as a production-ready distributed systems implementation** 🚀

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
