# Multi-Terminal Setup Guide

## Architecture
- **2 Clusters** with **2 nodes each** (4 servers total)
- **1 Client terminal** for testing

```
Cluster 0 (Shard 0):
  - node1 (Leader)   - HTTP: 8080, Raft: 9080
  - node2 (Follower) - HTTP: 8081, Raft: 9081

Cluster 1 (Shard 1):
  - node3 (Leader)   - HTTP: 8083, Raft: 9083
  - node4 (Follower) - HTTP: 8084, Raft: 9084

Sharding: CRC32(key) % 2 → Routes to Cluster 0 or 1
```

---

## Setup Instructions

### Terminal 1: Cluster 0 - Node 1 (Bootstrap)

```bash
cd ~/Desktop/Keyper

# Clean old data
rm -rf cluster0-data/node1
mkdir -p cluster0-data/node1 logs

# Start node1 (cluster0 leader)
./bin/server \
  --node-id=cluster0-node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:9080 \
  --data-dir=cluster0-data/node1 \
  --enable-raft

# You should see:
# Started raft node: id=cluster0-node1 raft_addr=127.0.0.1:9080
```

---

### Terminal 2: Cluster 0 - Node 2 (Join node1)

**Wait for Terminal 1 to show "Started raft node" before running this!**

```bash
cd ~/Desktop/Keyper

# Clean old data
rm -rf cluster0-data/node2
mkdir -p cluster0-data/node2

# Start node2 (joins cluster0)
./bin/server \
  --node-id=cluster0-node2 \
  --http-addr=:8081 \
  --raft-addr=127.0.0.1:9081 \
  --data-dir=cluster0-data/node2 \
  --enable-raft \
  --join=http://localhost:8080

# You should see:
# Successfully joined cluster via http://localhost:8080
```

---

### Terminal 3: Cluster 1 - Node 3 (Bootstrap)

```bash
cd ~/Desktop/Keyper

# Clean old data
rm -rf cluster1-data/node3
mkdir -p cluster1-data/node3

# Start node3 (cluster1 leader)
./bin/server \
  --node-id=cluster1-node3 \
  --http-addr=:8083 \
  --raft-addr=127.0.0.1:9083 \
  --data-dir=cluster1-data/node3 \
  --enable-raft

# You should see:
# Started raft node: id=cluster1-node3 raft_addr=127.0.0.1:9083
```

---

### Terminal 4: Cluster 1 - Node 4 (Join node3)

**Wait for Terminal 3 to show "Started raft node" before running this!**

```bash
cd ~/Desktop/Keyper

# Clean old data
rm -rf cluster1-data/node4
mkdir -p cluster1-data/node4

# Start node4 (joins cluster1)
./bin/server \
  --node-id=cluster1-node4 \
  --http-addr=:8084 \
  --raft-addr=127.0.0.1:9084 \
  --data-dir=cluster1-data/node4 \
  --enable-raft \
  --join=http://localhost:8083

# You should see:
# Successfully joined cluster via http://localhost:8083
```

---

## Terminal 5: Client (Testing)

```bash
cd ~/Desktop/Keyper

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
# PUT key to cluster1
curl -X PUT http://localhost:8083/v1/keys/order:12345 -d "Order details"
echo ""

# GET key from cluster1
curl -s http://localhost:8083/v1/keys/order:12345
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
echo "=== Testing Sharding (automatic routing) ==="
# These keys will automatically route to correct cluster
for key in test:1 test:2 test:3 test:4 test:5; do
  curl -X PUT http://localhost:8080/v1/keys/$key -d "value-$key" 2>/dev/null
  echo "Stored: $key"
done

echo ""
echo "=== Check key distribution ==="
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

### Test 3: Verify Replication
```bash
# Write to leader
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

### Test 4: Test Sharding
```bash
# Add 10 keys - they'll distribute across clusters
for i in {1..10}; do
  curl -X PUT http://localhost:8080/v1/keys/key$i -d "value$i" 2>/dev/null
  echo "Added key$i"
done

# Check distribution
echo "Cluster 0: $(curl -s http://localhost:8080/v1/status | jq '.num_keys') keys"
echo "Cluster 1: $(curl -s http://localhost:8083/v1/status | jq '.num_keys') keys"
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
cd ~/Desktop/Keyper
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
│  (Shard 0)  │  │  (Shard 1)  │
├─────────────┤  ├─────────────┤
│ node1: 8080 │  │ node3: 8083 │  ← Leaders
│ node2: 8081 │  │ node4: 8084 │  ← Followers
└─────────────┘  └─────────────┘

Key Distribution:
- CRC32(key) % 2 = 0 → Cluster 0
- CRC32(key) % 2 = 1 → Cluster 1

Replication:
- Within each cluster: both nodes have same data
- Across clusters: different data (sharding)
```

---

## Expected Behavior

✅ **After startup, you should see:**
- All 4 nodes running
- 2 leaders (one per cluster)
- 2 followers (one per cluster)

✅ **When you add keys:**
- Keys distributed ~50/50 across clusters
- Each key replicated to both nodes in its cluster

✅ **When leader fails:**
- Follower automatically becomes leader
- No data loss
- Clients can still read/write

Enjoy your distributed key-value store! 🚀
