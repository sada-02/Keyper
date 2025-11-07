# Multi-Terminal Setup Guide

## Architecture
- **2 Independent Raft Clusters** with **2 nodes each** (4 servers total)
- **1 Client terminal** for testing

```
Cluster 0:
  - node1 (Leader)   - HTTP: 8080, Raft: 12000
  - node2 (Follower) - HTTP: 8081, Raft: 12001

Cluster 1:
  - node3 (Leader)   - HTTP: 8083, Raft: 12003
  - node4 (Follower) - HTTP: 8084, Raft: 12004

Note: These are TWO SEPARATE clusters, not a sharded single cluster.
Each cluster replicates its own data independently.
```

**Important:** This setup creates two **independent** Raft clusters. There is NO automatic sharding or key distribution between them. Each cluster operates independently with its own data.

---

## Setup Instructions

### Terminal 1: Cluster 0 - Node 1 (Bootstrap)

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf cluster0-data/node1
mkdir -p cluster0-data/node1 logs

# Start node1 (cluster0 leader)
./bin/server \
  --node-id=cluster0-node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:12000 \
  --data-dir=cluster0-data/node1 \
  --enable-raft

# You should see:
# Started raft node: id=cluster0-node1 raft_addr=127.0.0.1:12000
```

---

### Terminal 2: Cluster 0 - Node 2 (Join node1)

**Wait for Terminal 1 to show "Started raft node" before running this!**

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf cluster0-data/node2
mkdir -p cluster0-data/node2

# Start node2 (joins cluster0)
./bin/server \
  --node-id=cluster0-node2 \
  --http-addr=:8081 \
  --raft-addr=127.0.0.1:12001 \
  --data-dir=cluster0-data/node2 \
  --enable-raft \
  --join=http://localhost:8080

# You should see:
# Successfully joined cluster via http://localhost:8080
```

---

### Terminal 3: Cluster 1 - Node 3 (Bootstrap)

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf cluster1-data/node3
mkdir -p cluster1-data/node3

# Start node3 (cluster1 leader)
./bin/server \
  --node-id=cluster1-node3 \
  --http-addr=:8083 \
  --raft-addr=127.0.0.1:12003 \
  --data-dir=cluster1-data/node3 \
  --enable-raft

# You should see:
# Started raft node: id=cluster1-node3 raft_addr=127.0.0.1:12003
```

---

### Terminal 4: Cluster 1 - Node 4 (Join node3)

**Wait for Terminal 3 to show "Started raft node" before running this!**

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf cluster1-data/node4
mkdir -p cluster1-data/node4

# Start node4 (joins cluster1)
./bin/server \
  --node-id=cluster1-node4 \
  --http-addr=:8084 \
  --raft-addr=127.0.0.1:12004 \
  --data-dir=cluster1-data/node4 \
  --enable-raft \
  --join=http://localhost:8083

# You should see:
# Successfully joined cluster via http://localhost:8083
```

---

## Terminal 5: Client (Testing)

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Wait 3-5 seconds for all servers to be ready
sleep 5

echo "=== Testing Cluster 0 (node1) ==="
# PUT key to cluster0
curl -X PUT http://localhost:8080/v1/keys/user:alice -d "Alice Smith"
echo ""

# GET key from cluster0
curl -s http://localhost:8080/v1/keys/user:alice
echo ""

# Check node status
curl -s http://localhost:8080/v1/status | jq '.'
echo ""

echo "=== Testing Cluster 1 (node3) ==="
# PUT key to cluster1 - NOTE: This is a SEPARATE cluster!
curl -X PUT http://localhost:8083/v1/keys/user:alice -d "Different Alice"
echo ""

# GET key from cluster1 - Will return "Different Alice"
curl -s http://localhost:8083/v1/keys/user:alice
echo ""

# Check node status
curl -s http://localhost:8083/v1/status | jq '.'
echo ""

echo "=== Testing Replication (read from follower) ==="
# IMPORTANT: By default, reads must go to the leader (linearizable reads)
# Followers will return 307 redirect

# This will FAIL with "not leader" error:
echo "Linearizable read from follower (will redirect):"
curl -v http://localhost:8081/v1/keys/user:alice 2>&1 | grep -E "HTTP|not leader"

# To read from follower, use ?stale=true (allows slightly old data)
echo ""
echo "Stale read from follower (allowed):"
curl -s "http://localhost:8081/v1/keys/user:alice?stale=true"
echo " (from node2 - follower, stale read allowed)"

# Leader can always serve reads
echo ""
echo "Linearizable read from leader:"
curl -s http://localhost:8080/v1/keys/user:alice
echo " (from node1 - leader, always fresh)"

echo ""
echo "=== Verify Clusters are Independent ==="
# Write to Cluster 0
curl -X PUT http://localhost:8080/v1/keys/test:cluster0 -d "Only in Cluster 0" 2>/dev/null
echo "Wrote 'test:cluster0' to Cluster 0"

# Write to Cluster 1
curl -X PUT http://localhost:8083/v1/keys/test:cluster1 -d "Only in Cluster 1" 2>/dev/null
echo "Wrote 'test:cluster1' to Cluster 1"

echo ""
echo "Key counts (these are independent):"
echo -n "Cluster 0 keys: "
curl -s http://localhost:8080/v1/status | jq '.num_keys'

echo -n "Cluster 1 keys: "
curl -s http://localhost:8083/v1/status | jq '.num_keys'

echo ""
echo "=== Test Election (optional) ==="
echo "Trigger election on cluster0:"
curl -X POST http://localhost:8080/v1/election/trigger \
  -H "Content-Type: application/json" \
  -d '{"method":"stepdown"}' | jq '.'

sleep 2
echo "New cluster0 leader:"
curl -s http://localhost:8080/v1/status | jq '.leader_addr'
curl -s http://localhost:8081/v1/status | jq '.leader_addr'
```

---

## Quick Test Commands (Copy-Paste Ready)

### Test 1: Basic PUT/GET
```bash
curl -X PUT http://localhost:8080/v1/keys/mykey -d "myvalue"
curl -s http://localhost:8080/v1/keys/mykey
```

### Test 2: Check Cluster Status
```bash
# Cluster 0
curl -s http://localhost:8080/v1/status | jq '.'
curl -s http://localhost:8081/v1/status | jq '.'

# Cluster 1
curl -s http://localhost:8083/v1/status | jq '.'
curl -s http://localhost:8084/v1/status | jq '.'
```

### Test 3: Verify Replication Within Each Cluster
```bash
# Write to Cluster 0 leader
curl -X PUT http://localhost:8080/v1/keys/replicated -d "test123"

# Linearizable read from leader - always works
curl -s http://localhost:8080/v1/keys/replicated

# Read from follower - TWO OPTIONS:

# Option 1: Stale read (fast, might be slightly old)
curl -s "http://localhost:8081/v1/keys/replicated?stale=true"

# Option 2: Without stale flag (will redirect to leader)
# Returns 307 with X-Raft-Leader header pointing to leader
curl -v http://localhost:8081/v1/keys/replicated 2>&1 | grep -E "HTTP|X-Raft"
```

### Test 4: Verify Clusters are Independent
```bash
# Add data to Cluster 0
curl -X PUT http://localhost:8080/v1/keys/data0 -d "cluster0" 2>/dev/null
echo "Added to Cluster 0"

# Add data to Cluster 1
curl -X PUT http://localhost:8083/v1/keys/data1 -d "cluster1" 2>/dev/null
echo "Added to Cluster 1"

# Check counts - they are independent
echo "Cluster 0: $(curl -s http://localhost:8080/v1/status | jq '.num_keys') keys"
echo "Cluster 1: $(curl -s http://localhost:8083/v1/status | jq '.num_keys') keys"

# Try to read data0 from Cluster 1 - will get 404 (not found)
curl -s http://localhost:8083/v1/keys/data0 || echo "Not found in Cluster 1 (expected)"
```

### Test 5: Leader Election
```bash
# Check who's leader
curl -s http://localhost:8080/v1/election/status | jq '.state, .leader'

# Trigger graceful stepdown
curl -X POST http://localhost:8080/v1/election/trigger \
  -H "Content-Type: application/json" \
  -d '{"method":"stepdown"}'

# Check new leader
sleep 2
curl -s http://localhost:8080/v1/election/status | jq '.state, .leader'
```

---

## Shutdown

### Stop All Servers (in any terminal)
```bash
pkill -f "bin/server"
```

### Clean All Data
```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper
rm -rf cluster0-data cluster1-data logs/*.log logs/*.pid
```

---

## Troubleshooting

### Issue: "connection refused"
**Solution:** Make sure you started the bootstrap nodes (Terminal 1 & 3) first and waited 2-3 seconds before starting followers.

### Issue: "not leader" errors
**Solution:** Elections take ~1 second. Wait a moment and retry, or check which node is leader:
```bash
curl -s http://localhost:8080/v1/status | jq '.is_leader, .leader_addr'
```

### Issue: Port already in use
**Solution:** Kill old processes:
```bash
pkill -f "bin/server"
sleep 1
# Then restart
```

### Issue: No jq installed
**Solution:** Install jq or remove `| jq '.'` from commands:
```bash
sudo apt install jq  # Ubuntu/Debian
```

---

## Architecture Summary

```
┌─────────────────────────────────────────┐
│           CLIENT (Terminal 5)            │
│         curl commands for testing        │
└──────────────┬──────────────────────────┘
               │
       ┌───────┴────────┐
       │                │
┌──────▼──────┐  ┌──────▼──────┐
│  CLUSTER 0  │  │  CLUSTER 1  │
│ (Independent)│ │ (Independent)│
├─────────────┤  ├─────────────┤
│ node1: 8080 │  │ node3: 8083 │  ← Leaders
│ node2: 8081 │  │ node4: 8084 │  ← Followers
└─────────────┘  └─────────────┘

Data Distribution:
- Cluster 0 and Cluster 1 are COMPLETELY SEPARATE
- No automatic sharding or key routing between them
- You must explicitly send requests to the cluster you want

Replication:
- Within Cluster 0: node1 and node2 have identical data
- Within Cluster 1: node3 and node4 have identical data
- Across clusters: NO data sharing (independent databases)
```

---

## Expected Behavior

✅ **After startup, you should see:**
- All 4 nodes running in 2 independent clusters
- 2 leaders (one per cluster)
- 2 followers (one per cluster)

✅ **When you add keys:**
- Keys sent to Cluster 0 (port 8080/8081) stay in Cluster 0
- Keys sent to Cluster 1 (port 8083/8084) stay in Cluster 1
- NO automatic distribution - you control which cluster receives data

✅ **When leader fails:**
- Follower in that cluster automatically becomes leader
- No data loss within that cluster
- The other cluster is unaffected

---

## 🔍 Want Automatic Sharding?

This setup does NOT include automatic sharding. If you want to enable sharding (automatic key distribution using CRC32 hashing), you need to:

1. Start servers with `--shard-count=N` flag
2. Use `--assigned-shards` to specify which shards each node hosts
3. Optionally use `--control-plane` for centralized coordination

Example for 2-shard setup on a single node:
```bash
./bin/server \
  --node-id=node1 \
  --http-addr=:8080 \
  --data-dir=./node1-data \
  --shard-count=2 \
  --assigned-shards="0,1" \
  --raft-base-port=12000
```

This is an **advanced feature** and requires more complex setup. The basic setup in this guide focuses on simple Raft replication without sharding.
