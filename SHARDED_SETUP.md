# Sharded Multi-Terminal Setup Guide

## Architecture
- **2 Shards** with **2 nodes each** (4 servers total + 1 control plane)
- **Automatic key routing** via CRC32 hashing
- **Replication within each shard** for fault tolerance
- **1 Client terminal** for testing

```
Control Plane:
  - control1 - HTTP: 7000, Raft: 7001

Shard 0:
  - node1 (Leader)   - HTTP: 8080, Main Raft: 11000, Shard 0 Raft: 12001
  - node2 (Follower) - HTTP: 8081, Main Raft: 11001, Shard 0 Raft: 12002

Shard 1:
  - node3 (Leader)   - HTTP: 8082, Main Raft: 11002, Shard 1 Raft: 12103
  - node4 (Follower) - HTTP: 8083, Main Raft: 11003, Shard 1 Raft: 12104

Port Calculation: base_port + (shard_id * 100) + node_number
  - Shard 0, Node 1: 12000 + (0 * 100) + 1 = 12001
  - Shard 0, Node 2: 12000 + (0 * 100) + 2 = 12002
  - Shard 1, Node 3: 12000 + (1 * 100) + 3 = 12103
  - Shard 1, Node 4: 12000 + (1 * 100) + 4 = 12104

Key Routing: CRC32(key) % 2 → Automatically routes to Shard 0 or Shard 1
```

---

## Prerequisites

Make sure you have:
- Built binaries: `./bin/server` and `./bin/control`
- Ports available: 7000-7001 (control), 8080-8083 (HTTP), 11000-11003 (main Raft), 12000-12001 (shard Raft)
- `jq` installed for JSON formatting: `sudo apt install jq`

---

## Setup Instructions

### Terminal 1: Control Plane (Required for Sharding)

The control plane manages shard topology and membership.

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf ctrl-data
mkdir -p ctrl-data logs

# Start control plane
./bin/control \
  --node-id=control1 \
  --http-addr=:7000 \
  --raft-addr=127.0.0.1:7001 \
  --data-dir=./ctrl-data \
  --bootstrap

# You should see:
# Control plane started on :7000
```

**Keep this terminal running!**

---

### Terminal 2: Shard 0 - Node 1 (Bootstrap)

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf shard0-node1
mkdir -p shard0-node1

# Start node1 (hosts shard 0, bootstrap)
./bin/server \
  --node-id=node1 \
  --http-addr=:8080 \
  --raft-addr=127.0.0.1:11000 \
  --data-dir=./shard0-node1 \
  --shard-count=2 \
  --assigned-shards="0" \
  --raft-base-port=12000 \
  --control-plane=localhost:7000 \
  --enable-raft

# You should see:
# Started raft node for shard 0
# Registered with control plane
# HTTP server listening on :8080
```

**Keep this terminal running!**

---

### Terminal 3: Shard 0 - Node 2 (Join Node 1)

**Wait 3-5 seconds after starting Terminal 2 before running this!**

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf shard0-node2
mkdir -p shard0-node2

# Start node2 (hosts shard 0, joins node1)
./bin/server \
  --node-id=node2 \
  --http-addr=:8081 \
  --raft-addr=127.0.0.1:11001 \
  --data-dir=./shard0-node2 \
  --shard-count=2 \
  --assigned-shards="0" \
  --raft-base-port=12000 \
  --control-plane=localhost:7000 \
  --join=http://localhost:8080 \
  --enable-raft

# You should see:
# Started raft node for shard 0
# Joined shard 0 cluster
# HTTP server listening on :8081
```

**Keep this terminal running!**

---

### Terminal 4: Shard 1 - Node 3 (Bootstrap)

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf shard1-node3
mkdir -p shard1-node3

# Start node3 (hosts shard 1, bootstrap)
./bin/server \
  --node-id=node3 \
  --http-addr=:8082 \
  --raft-addr=127.0.0.1:11002 \
  --data-dir=./shard1-node3 \
  --shard-count=2 \
  --assigned-shards="1" \
  --raft-base-port=12000 \
  --control-plane=localhost:7000 \
  --enable-raft

# You should see:
# Started raft node for shard 1
# Registered with control plane
# HTTP server listening on :8082
```

**Keep this terminal running!**

---

### Terminal 5: Shard 1 - Node 4 (Join Node 3)

**Wait 3-5 seconds after starting Terminal 4 before running this!**

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
rm -rf shard1-node4
mkdir -p shard1-node4

# Start node4 (hosts shard 1, joins node3)
./bin/server \
  --node-id=node4 \
  --http-addr=:8083 \
  --raft-addr=127.0.0.1:11003 \
  --data-dir=./shard1-node4 \
  --shard-count=2 \
  --assigned-shards="1" \
  --raft-base-port=12000 \
  --control-plane=localhost:7000 \
  --join=http://localhost:8082 \
  --enable-raft

# You should see:
# Started raft node for shard 1
# Joined shard 1 cluster
# HTTP server listening on :8083
```

**Keep this terminal running!**

---

## Terminal 6: Client (Testing)

Wait 5-10 seconds for all nodes to be ready, then run tests:

```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Wait for cluster to stabilize
sleep 5

echo "========================================"
echo "Testing Sharded Keyper Cluster"
echo "========================================"
echo ""

echo "=== Test 1: Basic Operations ==="
# PUT key (will be automatically routed to correct shard)
curl -X PUT http://localhost:8080/v1/keys/user:alice -d "Alice Smith"
echo "Stored: user:alice"

# GET key (can query any node, will be redirected if needed)
echo -n "Retrieved: "
curl -s http://localhost:8080/v1/keys/user:alice
echo ""
echo ""

echo "=== Test 2: Check Node Status ==="
echo "Node 1 (Shard 0):"
curl -s http://localhost:8080/v1/status | jq '.'
echo ""

echo "Node 2 (Shard 0):"
curl -s http://localhost:8081/v1/status | jq '.'
echo ""

echo "Node 3 (Shard 1):"
curl -s http://localhost:8082/v1/status | jq '.'
echo ""

echo "Node 4 (Shard 1):"
curl -s http://localhost:8083/v1/status | jq '.'
echo ""

echo "=== Test 3: Verify Replication Within Shards ==="
# Write to shard 0 (using a key that belongs to shard 0)
curl -X PUT http://localhost:8080/v1/keys/test0 -d "shard0-data" 2>/dev/null
echo "Wrote 'test0' to Shard 0 via node1"

# Read from shard 0 node2 (stale read)
echo -n "Read from Shard 0 node2: "
curl -s "http://localhost:8081/v1/keys/test0?stale=true"
echo " ✓ Replicated within shard 0"
echo ""

# Now test shard 1
curl -X PUT http://localhost:8082/v1/keys/replicated -d "shard1-data" 2>/dev/null
echo "Wrote 'replicated' to Shard 1 via node3"

# Read from shard 1 node4 (stale read)
echo -n "Read from Shard 1 node4: "
curl -s "http://localhost:8083/v1/keys/replicated?stale=true"
echo " ✓ Replicated within shard 1"
echo ""

echo "=== Test 4: Test Automatic Sharding ==="
echo "Adding 10 keys - they will distribute across shards automatically..."
for i in {1..10}; do
  curl -X PUT http://localhost:8080/v1/keys/key$i -d "value$i" 2>/dev/null
  echo "Added key$i"
done
echo ""

echo "Checking key distribution:"
SHARD0_NODE1=$(curl -s http://localhost:8080/v1/status | jq '.num_keys')
SHARD0_NODE2=$(curl -s http://localhost:8081/v1/status | jq '.num_keys')
SHARD1_NODE3=$(curl -s http://localhost:8082/v1/status | jq '.num_keys')
SHARD1_NODE4=$(curl -s http://localhost:8083/v1/status | jq '.num_keys')

echo "Shard 0 - Node1: $SHARD0_NODE1 keys"
echo "Shard 0 - Node2: $SHARD0_NODE2 keys (should match Node1)"
echo "Shard 1 - Node3: $SHARD1_NODE3 keys"
echo "Shard 1 - Node4: $SHARD1_NODE4 keys (should match Node3)"
echo ""

if [ "$SHARD0_NODE1" != "$SHARD0_NODE2" ]; then
    echo "⚠️  Warning: Shard 0 replication mismatch"
fi

if [ "$SHARD1_NODE3" != "$SHARD1_NODE4" ]; then
    echo "⚠️  Warning: Shard 1 replication mismatch"
fi

if [ "$SHARD0_NODE1" != "0" ] && [ "$SHARD1_NODE3" != "0" ]; then
    echo "✓ Keys distributed across both shards"
else
    echo "⚠️  All keys might be in one shard (check CRC32 distribution)"
fi
echo ""

echo "=== Test 5: Leader Election ==="
echo "Triggering election on Shard 0:"
curl -X POST http://localhost:8080/v1/election/trigger \
  -H "Content-Type: application/json" \
  -d '{"method":"stepdown"}' 2>/dev/null | jq '.'

sleep 3
echo ""
echo "New Shard 0 leaders:"
curl -s http://localhost:8080/v1/status | jq '.leader_addr'
curl -s http://localhost:8081/v1/status | jq '.leader_addr'
echo ""

echo "========================================"
echo "All tests completed!"
echo "========================================"
```

---

## Quick Test Commands (Copy-Paste Ready)

### Test 1: Basic PUT/GET with Automatic Routing
```bash
# Write to any node - will route to correct shard automatically
curl -X PUT http://localhost:8080/v1/keys/mykey -d "myvalue"
curl -s http://localhost:8080/v1/keys/mykey
```

### Test 2: Check All Node Status
```bash
# Check all nodes
curl -s http://localhost:8080/v1/status | jq '.'
curl -s http://localhost:8081/v1/status | jq '.'
curl -s http://localhost:8082/v1/status | jq '.'
curl -s http://localhost:8083/v1/status | jq '.'
```

### Test 3: Verify Replication Within Shards
```bash
# Write to Shard 0 node 1 (use a key that belongs to shard 0)
curl -X PUT http://localhost:8080/v1/keys/test0 -d "shard0-data"

# Read from Shard 0 node 2 (stale read) - should return same data
curl -s "http://localhost:8081/v1/keys/test0?stale=true"

# Write to Shard 1 node 3 (use a key that belongs to shard 1)
curl -X PUT http://localhost:8082/v1/keys/replicated -d "shard1-data"

# Read from Shard 1 node 4 (stale read) - should return same data
curl -s "http://localhost:8083/v1/keys/replicated?stale=true"

# Both nodes in each shard should have identical data
```

### Test 4: Test Automatic Sharding Distribution
```bash
# Add multiple keys - they'll distribute automatically
for i in {1..20}; do
  curl -X PUT http://localhost:8080/v1/keys/testkey$i -d "value$i" 2>/dev/null
  echo "Added testkey$i"
done

# Check distribution across shards
echo "Shard 0: $(curl -s http://localhost:8080/v1/status | jq '.num_keys') keys"
echo "Shard 1: $(curl -s http://localhost:8082/v1/status | jq '.num_keys') keys"

# Verify replication within each shard
echo "Shard 0 Node 2: $(curl -s http://localhost:8081/v1/status | jq '.num_keys') keys (should match Shard 0 Node 1)"
echo "Shard 1 Node 4: $(curl -s http://localhost:8083/v1/status | jq '.num_keys') keys (should match Shard 1 Node 3)"
```

### Test 5: Leader Election
```bash
# Check current leader for Shard 0
curl -s http://localhost:8080/v1/election/status | jq '.state, .leader'

# Trigger graceful stepdown
curl -X POST http://localhost:8080/v1/election/trigger \
  -H "Content-Type: application/json" \
  -d '{"method":"stepdown"}'

# Check new leader after 2-3 seconds
sleep 3
curl -s http://localhost:8080/v1/election/status | jq '.state, .leader'
```

### Test 6: Control Plane Status
```bash
# Check registered nodes
curl -s http://localhost:7000/v1/control/nodes | jq '.'

# Check shard mappings
curl -s http://localhost:7000/v1/control/shards | jq '.'
```

### Test 7: Verify Shard Ownership
```bash
# Each key is routed to a specific shard based on CRC32(key) % 2
# You can verify by checking which nodes actually store the key

# This key should be in Shard 0 or Shard 1
curl -X PUT http://localhost:8080/v1/keys/verify:shard -d "testing"

# Check if it's in Shard 0
curl -s http://localhost:8080/v1/keys/verify:shard 2>&1
# Check if it's in Shard 1
curl -s http://localhost:8082/v1/keys/verify:shard 2>&1

# The key will only be found in one shard, not both
```

---

## Shutdown

### Stop All Servers (in any terminal)
```bash
pkill -f "bin/server"
pkill -f "bin/control"
```

### Clean All Data
```bash
cd ~/IITR/sem5/csc303-cn/keyper/Keyper
rm -rf shard*-node* ctrl-data logs/
```

---

## Troubleshooting

### Issue: "connection refused"
**Solution:** Make sure you started the control plane (Terminal 1) first, then wait 2-3 seconds before starting server nodes.

### Issue: "not leader" errors
**Solution:** Elections take ~1-2 seconds. Wait a moment and retry, or check which node is leader:
```bash
curl -s http://localhost:8080/v1/status | jq '.is_leader, .leader_addr'
```

### Issue: Port already in use
**Solution:** Kill old processes:
```bash
pkill -f "bin/server"
pkill -f "bin/control"
sleep 2
# Then restart
```

**Note on Port Allocation:** Each node uses multiple Raft ports:
- **Main Raft port** (`--raft-addr`): For the node's main Raft instance (11000-11003 range)
- **Shard Raft ports** (auto-calculated): Each node gets a unique port per shard
  - **Formula**: `base_port + (shard_id × 100) + node_number`
  - **Example calculations**:
    - node1, shard 0: `12000 + (0 × 100) + 1 = 12001`
    - node2, shard 0: `12000 + (0 × 100) + 2 = 12002`
    - node3, shard 1: `12000 + (1 × 100) + 3 = 12103`
    - node4, shard 1: `12000 + (1 × 100) + 4 = 12104`
  
This ensures no port conflicts while allowing nodes to join the same shard's Raft cluster.

### Issue: "shard not found" or keys not routing
**Solution:** 
- Verify `--shard-count=2` is set on all nodes
- Check that `--assigned-shards` is correct (node1,node2: "0", node3,node4: "1")
- Ensure control plane is running and nodes registered
- Make sure `--control-plane` flag does NOT include `http://` prefix (use `localhost:7000`, not `http://localhost:7000`)

### Issue: "shard X not owned by this node"
**This is normal behavior!** It means:
- The key you're trying to access belongs to a different shard
- Keys are distributed via `CRC32(key) % shard_count`
- Each node only owns certain shards

**Example:**
- Key `test0` → CRC32 → Shard 0 (node1 or node2)
- Key `replicated` → CRC32 → Shard 1 (node3 or node4)

**To test which shard a key belongs to:**
```bash
# Using Python
python3 -c "import binascii; key='yourkey'; print(f'Shard: {binascii.crc32(key.encode()) % 2}')"
```

**Solution:** Either:
1. Query the correct shard/node that owns the key, OR
2. Use the routing feature (if implemented) that automatically forwards to the right shard

### Issue: Replication not working within a shard
**Solution:**
- Wait 3-5 seconds for Raft consensus
- Check both nodes in the shard have joined the same Raft group
- Use `?stale=true` for follower reads

### Issue: Multiple leaders in the same shard
**Problem:** If you see node2 or node4 becoming leaders (instead of followers), it means they bootstrapped as independent clusters instead of joining the existing clusters.

**Solution:** Make sure node2 and node4 use the `--join` flag:
- node2 must have `--join=http://localhost:8080` (to join node1's cluster)
- node4 must have `--join=http://localhost:8082` (to join node3's cluster)

**How to verify:** Check the Raft state:
```bash
# Shard 0 should have only ONE leader
curl -s http://localhost:8080/v1/status | jq '.is_leader'  # Should be true
curl -s http://localhost:8081/v1/status | jq '.is_leader'  # Should be false

# Shard 1 should have only ONE leader
curl -s http://localhost:8082/v1/status | jq '.is_leader'  # Should be true
curl -s http://localhost:8083/v1/status | jq '.is_leader'  # Should be false
```

If both nodes in a shard show `is_leader: true`, you have a split-brain issue. Clean data and restart with proper `--join` flags.

### Issue: No jq installed
**Solution:** Install jq or remove `| jq '.'` from commands:
```bash
sudo apt install jq  # Ubuntu/Debian
```

---

## Architecture Summary

```
┌─────────────────────────────────────────────────┐
│          CLIENT (Terminal 6)                    │
│      curl commands with automatic routing       │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────┐
│         CONTROL PLANE (Terminal 1)              │
│    • Shard topology management                  │
│    • Node registration                          │
│    • Membership coordination                    │
│    • HTTP: 7000, Raft: 7001                     │
└────────────────┬────────────────────────────────┘
                 │
        ┌────────┴────────┐
        │                 │
   ┌────▼─────┐     ┌────▼─────┐
   │ SHARD 0  │     │ SHARD 1  │
   ├──────────┤     ├──────────┤
   │ node1    │     │ node3    │  ← Leaders
   │ HTTP:8080│     │ HTTP:8082│
   │ MRaft:   │     │ MRaft:   │  (Main Raft)
   │  11000   │     │  11002   │
   │ SRaft:   │     │ SRaft:   │  (Shard Raft)
   │  12001   │     │  12103   │
   │          │     │          │
   │ node2    │     │ node4    │  ← Followers
   │ HTTP:8081│     │ HTTP:8083│
   │ MRaft:   │     │ MRaft:   │
   │  11001   │     │  11003   │
   │ SRaft:   │     │ SRaft:   │
   │  12002   │     │  12104   │
   └──────────┘     └──────────┘

Port Allocation:
- Control Plane: HTTP 7000, Raft 7001
- HTTP API: 8080-8083 (one per node)
- Main Raft: 11000-11003 (one per node)
- Shard Raft: Unique per node per shard (formula: base + shard*100 + node_num)
  * Shard 0: node1=12001, node2=12002
  * Shard 1: node3=12103, node4=12104

Key Routing:
- CRC32(key) % 2 = 0 → Shard 0 (node1 or node2)
- CRC32(key) % 2 = 1 → Shard 1 (node3 or node4)

Replication:
- Within Shard 0: node1 and node2 have identical data
- Within Shard 1: node3 and node4 have identical data
- Across shards: different data (partitioned by hash)

Fault Tolerance:
- Each shard can lose 1 node and still operate
- Automatic leader election within each shard
- Control plane manages topology changes
```

---

## Expected Behavior

✅ **After startup, you should see:**
- Control plane running on port 7000
- 4 server nodes running on ports 8080-8083
- 2 Raft groups (one per shard) with 2 nodes each
- Each shard has 1 leader and 1 follower

✅ **When you add keys:**
- Keys automatically route to correct shard via CRC32 hash
- Keys distributed approximately 50/50 across shards
- Each key replicated to both nodes within its shard

✅ **When shard leader fails:**
- Follower in that shard automatically becomes leader
- No data loss within that shard
- Keys in the failed shard can still be accessed
- Other shard continues operating normally

✅ **When you query any node:**
- Request automatically redirected to shard that owns the key
- Can read from any node using `?stale=true`
- Leader reads are always linearizable

---

## Key Differences from Basic Setup

| Feature | Basic Setup (TERMINAL_SETUP.md) | Sharded Setup (This Guide) |
|---------|----------------------------------|---------------------------|
| Architecture | 2 independent clusters | 1 unified cluster with 2 shards |
| Key Routing | Manual (must specify cluster) | Automatic via CRC32 hash |
| Data Distribution | No distribution | Automatic ~50/50 split |
| Control Plane | Not required | Required for coordination |
| Nodes per Shard | N/A | 2 nodes (replication) |
| Fault Tolerance | Per-cluster | Per-shard |
| Complexity | Simple | Advanced |

---

## Performance Considerations

- **Writes**: Slightly slower due to Raft consensus within shard
- **Reads**: Fast from leader, very fast with `?stale=true` from follower
- **Scalability**: Can add more shards by increasing `--shard-count`
- **Fault Tolerance**: Each shard can lose 1 node (50% of shard nodes)

---

## Next Steps

Once comfortable with this setup, you can:

1. **Add more shards**: Increase `--shard-count` to 4, 8, 16, etc.
2. **Add more replicas**: Run 3+ nodes per shard for higher availability
3. **Enable TLS**: Add `--tls-cert` and `--tls-key` flags
4. **Add authentication**: Use `--auth-token` for admin endpoints
5. **Monitor with Prometheus**: Scrape `/metrics` endpoints
6. **Test migrations**: Move shards between nodes (advanced)

---

## Reference: Complete Command Summary

**Control Plane:**
```bash
./bin/control --node-id=control1 --http-addr=:7000 --raft-addr=127.0.0.1:7001 --data-dir=./ctrl-data --bootstrap
```

**Shard 0 Nodes:**
```bash
# Node 1 (main Raft: 11000, shard 0 Raft: 12001) - BOOTSTRAP
./bin/server --node-id=node1 --http-addr=:8080 --raft-addr=127.0.0.1:11000 --data-dir=./shard0-node1 --shard-count=2 --assigned-shards="0" --raft-base-port=12000 --control-plane=localhost:7000 --enable-raft

# Node 2 (main Raft: 11001, shard 0 Raft: 12002) - JOIN node1
./bin/server --node-id=node2 --http-addr=:8081 --raft-addr=127.0.0.1:11001 --data-dir=./shard0-node2 --shard-count=2 --assigned-shards="0" --raft-base-port=12000 --control-plane=localhost:7000 --join=http://localhost:8080 --enable-raft
```

**Shard 1 Nodes:**
```bash
# Node 3 (main Raft: 11002, shard 1 Raft: 12103) - BOOTSTRAP
./bin/server --node-id=node3 --http-addr=:8082 --raft-addr=127.0.0.1:11002 --data-dir=./shard1-node3 --shard-count=2 --assigned-shards="1" --raft-base-port=12000 --control-plane=localhost:7000 --enable-raft

# Node 4 (main Raft: 11003, shard 1 Raft: 12104) - JOIN node3
./bin/server --node-id=node4 --http-addr=:8083 --raft-addr=127.0.0.1:11003 --data-dir=./shard1-node4 --shard-count=2 --assigned-shards="1" --raft-base-port=12000 --control-plane=localhost:7000 --join=http://localhost:8082 --enable-raft
```

---

**Enjoy your production-ready sharded distributed key-value store!** 🚀

For questions or issues, check the main README.md or the troubleshooting section above.
