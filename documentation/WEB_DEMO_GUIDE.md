# Keyper Web Demo Guide

## 🌐 Multi-Cluster Interactive Dashboard

The Keyper web demo provides a real-time visualization of a distributed key-value store with **3 independent Raft clusters** handling different shards of data.

---

## 🚀 Quick Start

### Start the Demo

```bash
./scripts/start-web-demo.sh
```

This command will:
1. Clean any existing data directories
2. Build the necessary binaries (`bin/server` and `bin/webui`)
3. Start **9 server nodes** organized into **3 Raft clusters**:
   - **Cluster 0 (Shard 0)**: cluster0-node1, cluster0-node2, cluster0-node3
   - **Cluster 1 (Shard 1)**: cluster1-node4, cluster1-node5, cluster1-node6
   - **Cluster 2 (Shard 2)**: cluster2-node7, cluster2-node8, cluster2-node9
4. Launch the web UI on port 9000
5. Attempt to open your browser automatically

### Access the Dashboard

Open your browser to: **http://localhost:9000**

### Stop the Demo

```bash
./scripts/stop-web-demo.sh
```

---

## 📊 Dashboard Features

### 1. **Stats Bar**
At the top of the dashboard, you'll see real-time statistics:
- **Clusters**: Number of active Raft clusters (3)
- **Total Nodes**: All server nodes across clusters (9)
- **Leaders**: One leader per cluster (should be 3)
- **Active Clients**: Automated clients generating traffic
- **Total Requests**: Cumulative requests from all clients

### 2. **Cluster View**
Each of the 3 clusters is displayed with:
- **Cluster ID and Shard ID**: e.g., "cluster0 (Shard 0)"
- **Node Count**: Number of nodes in the cluster (3 per cluster)
- **Leader Information**: Which node is currently the leader
- **Quorum Status**: Shows 2/3 (majority of 2 needed for consensus)

**Per-Node Information**:
- Node ID (e.g., cluster0-node1)
- Status badge: 👑 LEADER or Follower
- HTTP endpoint (e.g., :8080)
- Raft address (e.g., 127.0.0.1:9080)
- Process ID (PID)
- Number of keys stored

### 3. **Automated Clients**
The right panel shows automated test clients that:
- Generate PUT/GET/DELETE requests every 2 seconds
- Route keys to the correct cluster using CRC32 hashing
- Display request counts and key prefixes
- Can be started/stopped individually

**Actions**:
- **➕ Add New Client**: Creates a new automated client
- **Start/Stop**: Control individual clients

### 4. **Request History**
Live feed showing the last 15 requests with:
- Timestamp
- Client ID
- Operation (PUT/GET/DELETE)
- Key name
- Target node
- Status (✅ success or ❌ error)

### 5. **Interactive Features**

#### 🗳️ Graceful Leader Election
Each cluster has a "Graceful Election" button that:
- Triggers the current leader to step down voluntarily
- Forces a new leader election among the remaining nodes
- Demonstrates Raft's automatic failover capability
- Shows the old and new leader after completion

**Use this to test**:
- Leader election mechanism
- Cluster resilience
- Request handling during leadership transitions

#### ➕ Add New Cluster
At the bottom, you can dynamically add new clusters:
- Creates 3 new nodes forming a new Raft cluster
- Assigns a new shard ID
- Starts on new ports automatically
- Integrates with the existing sharding system

---

## 🔍 How Sharding Works

### Key Distribution

When a client writes a key (e.g., `c1:key5`):

1. **Hash Calculation**: CRC32 hash of the key is computed
2. **Shard Selection**: `hash % 3` determines the target cluster
   - Shard 0 → Cluster 0 (nodes 1-3)
   - Shard 1 → Cluster 1 (nodes 4-6)
   - Shard 2 → Cluster 2 (nodes 2-9)
3. **Leader Routing**: Request is sent to the leader of that cluster
4. **Raft Replication**: Leader replicates to followers before acknowledging

### Why 3 Clusters?

- **Horizontal Scalability**: Each cluster handles 1/3 of the keys
- **Independent Consensus**: Failures in one cluster don't affect others
- **Parallel Operations**: All 3 clusters can process requests simultaneously
- **Fault Tolerance**: Each cluster can survive 1 node failure (2/3 quorum)

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     Web Dashboard (Port 9000)            │
│              Real-time Monitoring & Control              │
└───────────────────────┬──────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
  ┌─────▼─────┐   ┌─────▼─────┐   ┌────▼──────┐
  │ Cluster 0 │   │ Cluster 1 │   │ Cluster 2 │
  │ (Shard 0) │   │ (Shard 1) │   │ (Shard 2) │
  └───────────┘   └───────────┘   └───────────┘
        │               │               │
  ┌─────┴─────┐   ┌─────┴─────┐   ┌────┴──────┐
  │  Node 1   │   │  Node 4   │   │  Node 7   │
  │ Leader 👑 │   │ Leader 👑 │   │ Leader 👑 │
  │ :8080     │   │ :8083     │   │ :8086     │
  ├───────────┤   ├───────────┤   ├───────────┤
  │  Node 2   │   │  Node 5   │   │  Node 8   │
  │ Follower  │   │ Follower  │   │ Follower  │
  │ :8081     │   │ :8084     │   │ :8087     │
  ├───────────┤   ├───────────┤   ├───────────┤
  │  Node 3   │   │  Node 6   │   │  Node 9   │
  │ Follower  │   │ Follower  │   │ Follower  │
  │ :8082     │   │ :8085     │   │ :8088     │
  └───────────┘   └───────────┘   └───────────┘
```

---

## 📡 Direct API Access

While the web UI is running, you can also interact with nodes directly:

### Check Cluster Status

```bash
# Cluster 0 leader
curl http://localhost:8080/v1/status | jq .

# Cluster 1 leader
curl http://localhost:8083/v1/status | jq .

# Cluster 2 leader
curl http://localhost:8086/v1/status | jq .
```

### Manual Key Operations

```bash
# Write a key (goes to appropriate shard automatically)
curl -X PUT http://localhost:8080/v1/keys/mykey -d 'myvalue'

# Read a key
curl http://localhost:8080/v1/keys/mykey

# Delete a key
curl -X DELETE http://localhost:8080/v1/keys/mykey
```

### View Metrics

```bash
# Prometheus metrics from any node
curl http://localhost:8080/metrics
curl http://localhost:8083/metrics
curl http://localhost:8086/metrics
```

---

## 🧪 Testing Scenarios

### 1. Test Normal Operations
1. Add a client using the "Add New Client" button
2. Watch requests flow to different clusters
3. Observe key distribution across shards
4. See real-time request history

### 2. Test Leader Election
1. Click "🗳️ Graceful Election" on Cluster 0
2. Watch the leader step down gracefully
3. Observe automatic election of a new leader
4. Verify requests continue to be processed

### 3. Test Shard Distribution
1. Add multiple clients (3-5)
2. Let them generate traffic for 30 seconds
3. Compare key counts across clusters:
   ```bash
   curl -s http://localhost:8080/v1/status | jq '.num_keys'  # Cluster 0
   curl -s http://localhost:8083/v1/status | jq '.num_keys'  # Cluster 1
   curl -s http://localhost:8086/v1/status | jq '.num_keys'  # Cluster 2
   ```
4. Keys should be relatively evenly distributed

### 4. Test Cluster Expansion
1. Click "➕ Add New Cluster (3 nodes)"
2. Wait for new cluster to start (~10 seconds)
3. Refresh the page to see the new cluster
4. Add a client to generate traffic to the new cluster

---

## 📋 Log Files

All logs are stored in the `logs/` directory:

```
logs/
├── cluster0-node1.log    # Cluster 0 logs
├── cluster0-node2.log
├── cluster0-node3.log
├── cluster1-node4.log    # Cluster 1 logs
├── cluster1-node5.log
├── cluster1-node6.log
├── cluster2-node7.log    # Cluster 2 logs
├── cluster2-node8.log
├── cluster2-node9.log
└── webui.log            # Web UI logs
```

### View Logs

```bash
# Watch cluster 0 leader log
tail -f logs/cluster0-node1.log

# Check for errors across all nodes
grep -i error logs/*.log

# Monitor Raft elections
grep -i "election" logs/cluster*.log
```

---

## 🐛 Troubleshooting

### Dashboard shows 0 clusters
- Wait 3-5 seconds for initial refresh
- Check if nodes are running: `ps aux | grep bin/server`
- Verify nodes are responding: `curl http://localhost:8080/v1/status`

### Clients not generating requests
- Check that at least one client is active (green checkmark)
- Verify leaders are elected in each cluster
- Check webui.log for errors: `tail -f logs/webui.log`

### Leader election fails
- Ensure at least 2 nodes per cluster are running
- Check Raft logs for network issues
- Verify no port conflicts: `netstat -tulpn | grep -E "(808|908)"`

### Clean restart needed
```bash
./scripts/stop-web-demo.sh
rm -rf cluster*-node*-data node*-data
./scripts/start-web-demo.sh
```

---

## 🎯 Performance Notes

- **Auto-refresh**: Dashboard updates every 3 seconds
- **Client Rate**: Each client sends 1 request every 2 seconds
- **Expected Latency**: 
  - PUT requests: ~5-10ms (with Raft consensus)
  - GET requests: ~1-2ms (from leader)
- **Throughput**: ~500 requests/sec total across all clusters

---

## 🔗 Related Documentation

- [Main README](README.md) - Full project documentation
- [Performance Testing](performance/README.md) - Benchmarking guide
- [Sharded Setup](documentation/SHARDED_SETUP.md) - Advanced sharding configuration

---

**Enjoy exploring the distributed system!** 🚀
