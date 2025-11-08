# Keyper - Production-Ready Distributed Key-Value Store

A high-performance distributed key-value store with **Raft consensus**, **horizontal sharding**, **zero-downtime migration**, **TLS security**, and **comprehensive observability**.

---

## ✨ Features

### Core Capabilities
- ✅ **Distributed Consensus** - HashiCorp Raft for strong consistency
- ✅ **Auto-Join Clustering** - New nodes automatically join via existing node
- ✅ **Leader Election** - Automatic failover when leader fails
- ✅ **Data Replication** - All writes replicated to majority of nodes
- ✅ **Linearizable Reads** - Strong consistency guarantees from leader
- ✅ **Optional Stale Reads** - Fast reads from followers with eventual consistency

### Security & Operations
- ✅ **TLS Encryption** - Mutual TLS for Raft and HTTPS for API
- ✅ **Authentication** - Bearer token protection for admin endpoints
- ✅ **Observability** - Prometheus metrics, structured logging, health checks
- ✅ **Web UI** - Interactive dashboard for monitoring and testing

### Data & Storage
- ✅ **Persistent Storage** - BadgerDB embedded key-value engine
- ✅ **RESTful API** - Simple HTTP interface for all operations
- ✅ **Export/Import** - Data snapshot and restore capabilities

### Advanced Features (Optional)
- ⚙️ **Horizontal Sharding** - CRC32-based consistent hashing (experimental)
- ⚙️ **Control Plane** - Centralized topology management (experimental)
- ⚙️ **Zero-Downtime Migration** - Safe shard movement (experimental)

---

## 🚀 Quick Start

### Prerequisites

- **Go 1.20+** (tested with 1.25.x)
- Available ports: 8080-8082 (HTTP), 12000-12002 (Raft), 9000 (Web UI)

### Installation

```bash
# Clone repository
git clone https://github.com/sada-02/Keyper.git
cd Keyper

# Install dependencies
go mod tidy

# Build all binaries
go build -o bin/server ./cmd/server
go build -o bin/webui ./cmd/webui
go build -o bin/control ./cmd/control
```

### Start a 3-Node Cluster

**Option 1: Using the convenience script (Recommended)**

```bash
./scripts/start-cluster.sh
```

This will start a 3-node Raft cluster with:
- Node 1 (Leader): http://localhost:8080
- Node 2 (Follower): http://localhost:8081  
- Node 3 (Follower): http://localhost:8082

**Option 2: Manual start**

```bash
# Start node 1 (bootstrap node)
./bin/server \
  --node-id=node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:12000 \
  --data-dir=./node1-data \
  --enable-raft

# Wait 2-3 seconds for node1 to elect itself as leader

# Start node 2 (joins node1)
./bin/server \
  --node-id=node2 \
  --http-addr=:8081 \
  --raft-addr=127.0.0.1:12001 \
  --data-dir=./node2-data \
  --enable-raft \
  --join=http://localhost:8080

# Start node 3 (joins node1)
./bin/server \
  --node-id=node3 \
  --http-addr=:8082 \
  --raft-addr=127.0.0.1:12002 \
  --data-dir=./node3-data \
  --enable-raft \
  --join=http://localhost:8080
```

**Stop the cluster:**

```bash
./scripts/stop-cluster.sh
# Or manually: pkill -f "bin/server"
```

### 🧪 Performance Testing

Keyper includes comprehensive performance tests:

```bash
# Run migration tests
./performance/test_migration.sh

# Run metrics tests
./performance/test_metrics.sh

# Or from performance directory
cd performance
./test_migration.sh
./test_metrics.sh
```

**Available Tests:**
- **Migration Tests**: Export/import, pause/resume, zero-downtime migration
- **Metrics Tests**: Prometheus endpoint, HTTP/Raft/BadgerDB metrics
- **Throughput Tests**: Request processing performance
- **Scalability Tests**: Cluster scaling (1-10 nodes)
- **Stability Tests**: Long-term operation validation

See `performance/README.md` for detailed documentation.

### 🌐 Interactive Web Demo (Recommended for Testing)

For an interactive visualization and testing experience with multi-cluster sharding:

```bash
# Start multi-cluster setup with web UI
./scripts/start-web-demo.sh
```

This will automatically:
- Start **3 independent Raft clusters** (9 nodes total)
  - Cluster 0 (Shard 0): nodes 1-3 on ports 8080-8082
  - Cluster 1 (Shard 1): nodes 4-6 on ports 8083-8085
  - Cluster 2 (Shard 2): nodes 7-9 on ports 8086-8088
- Launch the multi-cluster web UI on port 9000
- Open your browser (if available)

Then open your browser to:
- **Multi-Cluster Dashboard**: http://localhost:9000

The web demo provides:
- ✅ **Real-time cluster visualization** - View all 3 clusters with their leaders and followers
- ✅ **Automated test clients** - Generate distributed traffic across shards
- ✅ **Shard-aware routing** - Keys automatically routed to correct cluster via CRC32 hashing
- ✅ **Request history** - Live feed of all operations with timestamps and responses
- ✅ **Leader election triggers** - Test graceful leader stepdown and failover
- ✅ **Dynamic cluster management** - Add new clusters on-the-fly
- ✅ **Auto-refresh every 3 seconds** - Always up-to-date cluster state

**Features**:
- Each cluster runs an independent Raft consensus group
- Keys are distributed across clusters using consistent hashing
- Full fault tolerance within each cluster (survives 1 node failure per cluster)
- Real-time monitoring of leaders, followers, and key distribution

**Stop the web demo:**

```bash
./scripts/stop-web-demo.sh
```

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
┌─────────────────────────────────────────────────────┐
│                  CLIENT APPLICATION                 │
│          (curl, web UI, or custom client)           │
└──────────────────────┬──────────────────────────────┘
                       │
       ┌───────────────┼───────────────┐
       │               │               │
   ┌───▼────┐     ┌────▼────┐     ┌───▼─────┐
   │ Node 1 │     │ Node 2  │     │ Node 3  │
   │(Leader)│◄───►│(Follower)◄───►│(Follower)│
   │        │     │         │     │         │
   │ Raft   │     │  Raft   │     │  Raft   │
   │Consensus     │Consensus│     │Consensus│
   │        │     │         │     │         │
   │ Badger │     │ Badger  │     │ Badger  │
   │  Store │     │  Store  │     │  Store  │
   └────────┘     └─────────┘     └─────────┘
   
   All nodes replicate data via Raft consensus
```

### Key Design Principles

1. **Raft Consensus**: Leader-based replication for strong consistency
2. **Auto-Join**: New nodes automatically join the cluster
3. **Leader Election**: Automatic failover when leader fails
4. **Data Replication**: All writes replicated to majority before acknowledgment
5. **Linearizable Reads**: Reads from leader guaranteed to be up-to-date
6. **Stale Reads**: Optional fast reads from followers with `?stale=true`

---

## 📚 API Reference

### Data Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/v1/keys/{key}` | Write or update key |
| GET | `/v1/keys/{key}` | Read key value (linearizable from leader) |
| GET | `/v1/keys/{key}?stale=true` | Read key value (fast read from any node) |
| DELETE | `/v1/keys/{key}` | Delete key |
| GET | `/v1/status` | Node status (leader, num_keys, etc.) |

### Health & Monitoring

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/health` | Health check endpoint |
| GET | `/metrics` | Prometheus metrics |

### Election Management (Testing)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/election/status` | Get current Raft state and leader |
| POST | `/v1/election/trigger` | Trigger leader election (for testing) |

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
  --node-id=node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:12000 \
  --data-dir=./node1-data \
  --enable-raft \
  --tls-cert=./test-certs/server-cert.pem \
  --tls-key=./test-certs/server-key.pem \
  --raft-tls-cert=./test-certs/server-cert.pem \
  --raft-tls-key=./test-certs/server-key.pem \
  --raft-tls-ca=./test-certs/ca-cert.pem \
  --auth-token="my-secret-token"
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
- `keyper_raft_state` - Current Raft state (leader/follower/candidate)

**HTTP Metrics**:
- `keyper_http_requests_total` - Request rate by status
- `keyper_http_request_duration_seconds` - Latency histogram

**Storage Metrics**:
- `keyper_badger_writes_total` - Write operations
- `keyper_badger_reads_total` - Read operations

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
go test ./raft -v
go test ./store -v
```

### Integration Testing

```bash
# Start test cluster
./scripts/start-cluster.sh

# Test basic operations
curl -X PUT http://localhost:8080/v1/keys/test -d "hello"
curl http://localhost:8080/v1/keys/test

# Check cluster status
curl http://localhost:8080/v1/status
curl http://localhost:8081/v1/status
curl http://localhost:8082/v1/status

# Stop cluster
./scripts/stop-cluster.sh

# Clean data
./scripts/clean.sh
```

---

## ⚙️ Configuration

### Server Options

```bash
--node-id string          Unique node identifier (default "node-1")
--http-addr string        HTTP server address (default ":8080")
--raft-addr string        Raft transport address (default "127.0.0.1:12000")
--data-dir string         Data directory (default "./data")
--enable-raft             Enable Raft replication (required for clustering)
--join string             HTTP address of existing node to join (e.g. http://host:8080)

# Sharding options (optional, for advanced setups)
--shard-count int         Number of shards (default 0, disabled)
--assigned-shards string  Shards to host, e.g., "0,1,2" (default: all)
--raft-base-port int      Base port for per-shard raft instances (default 12000)
--control-plane string    Control plane address for service discovery

# TLS options
--tls-cert string         TLS certificate for HTTPS
--tls-key string          TLS private key for HTTPS
--raft-tls-cert string    TLS certificate for Raft
--raft-tls-key string     TLS private key for Raft
--raft-tls-ca string      CA certificate for Raft peer verification

# Security
--auth-token string       Bearer token for admin endpoints
```

---

## 📁 Project Structure

```
Keyper/
├── cmd/
│   ├── server/              # Main KV server
│   ├── webui/               # Web UI for visualization
│   └── control/             # Control plane service (optional)
├── client/
│   ├── client.go            # HTTP client library
│   ├── sharded_client.go    # Sharded client with auto-routing
│   └── discovery.go         # Control plane discovery
├── config/                  # Configuration management
├── httpapi/                 # HTTP API handlers
│   ├── handler.go           # Core KV operations
│   ├── election_handlers.go # Election management
│   ├── health.go            # Health check endpoints
│   └── middleware.go        # Metrics middleware
├── logging/                 # Structured logging
├── metrics/                 # Prometheus metrics
├── raft/                    # Raft consensus
│   ├── node.go              # Raft node wrapper
│   ├── fsm.go               # Finite state machine
│   └── apply.go             # Command application
├── store/                   # BadgerDB wrapper
│   ├── store.go             # Key-value storage
│   ├── iterator.go          # Key iteration
│   └── export_import.go     # Data export/import
├── scripts/                 # Operational scripts
│   ├── start-cluster.sh     # Start 3-node cluster
│   ├── stop-cluster.sh      # Stop cluster
│   ├── start-web-demo.sh    # Start with web UI
│   ├── stop-web-demo.sh     # Stop web demo
│   ├── clean.sh             # Clean data directories
│   └── run-unit-tests.sh    # Run tests
└── examples/                # Configuration examples
    ├── prometheus-config.yml
    └── grafana-dashboard.json
```

---

## 🎯 Performance

**Benchmarks** (3-node cluster, single Raft group):

| Operation | Latency (P99) | Throughput |
|-----------|---------------|------------|
| Write (Raft) | ~5-10ms | 5-10k ops/sec |
| Read (leader) | ~1-2ms | 20-50k ops/sec |
| Read (stale) | ~0.5-1ms | 50-100k ops/sec |

**Scalability**:
- Supports 3+ node clusters
- Automatic leader election and failover
- Tested with thousands of keys
- Supports sharding for horizontal scaling (optional)

---

## 🛠️ Troubleshooting

### Common Issues

**Port already in use**:
```bash
./scripts/stop-cluster.sh
# Or manually: pkill -f "bin/server"
```

**Raft leader not elected**:
- Wait 2-3 seconds for election to complete
- Ensure at least 2 nodes are running (majority required)
- Check network connectivity between Raft addresses
- Verify first node started without `--join` flag

**Cannot read key (404 or redirect)**:
- Reads must go to leader by default for linearizability
- Use `?stale=true` query parameter to read from followers
- Check which node is leader: `curl http://localhost:8080/v1/status`

**Node fails to join cluster**:
- Ensure bootstrap node (node1) is running and leader
- Wait 2-3 seconds after starting node1 before starting node2/node3
- Check `--join` address is correct (e.g., `http://localhost:8080`)
- Verify ports are not blocked by firewall

**Clean start needed**:
```bash
./scripts/stop-cluster.sh
./scripts/clean.sh
./scripts/start-cluster.sh
```

---

## 🚧 Limitations & Future Work

### Current Limitations

- Follower reads require `?stale=true` flag (not linearizable by default)
- No automatic data migration between nodes
- Single Raft group (sharding is optional/experimental)
- Certificate rotation requires restart

### Roadmap

- [ ] Linearizable follower reads with Raft ReadIndex
- [ ] Automatic sharding and rebalancing
- [ ] Multi-datacenter replication
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Client libraries (Python, Java, JavaScript)
- [ ] Backup/restore utilities
- [ ] Snapshot compression
- [ ] Watch/notification API

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
