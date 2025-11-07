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
sleep 10

echo "========================================"
echo "Testing Sharded Keyper Cluster"
echo "========================================"
echo ""

# First, determine which nodes are leaders for each shard
# NOTE: We need to detect SHARD leadership, not main Raft leadership!
# The /v1/status endpoint shows main Raft leadership, which may differ from shard leadership
echo "=== Detecting Shard Leaders ==="
SHARD0_LEADER=""
SHARD1_LEADER=""

# Test which node accepts writes for Shard 0
# Using key "_probe4" which belongs to shard 0 (CRC32("_probe4") % 2 = 0)
SHARD0_TEST_RESULT=$(curl -s -X PUT http://localhost:8080/v1/keys/_probe4 -d "probe" 2>&1)
if echo "$SHARD0_TEST_RESULT" | grep -q "not leader for shard"; then
    SHARD0_LEADER="8081"
    echo "Shard 0 Leader: Node 2 (port 8081)"
    # Clean up test on actual leader
    curl -s -X DELETE http://localhost:8081/v1/keys/_probe4 > /dev/null 2>&1
else
    SHARD0_LEADER="8080"
    echo "Shard 0 Leader: Node 1 (port 8080)"
    # Clean up test
    curl -s -X DELETE http://localhost:8080/v1/keys/_probe4 > /dev/null 2>&1
fi

# Test which node accepts writes for Shard 1
# Using key "_probe0" which belongs to shard 1 (CRC32("_probe0") % 2 = 1)
SHARD1_TEST_RESULT=$(curl -s -X PUT http://localhost:8082/v1/keys/_probe0 -d "probe" 2>&1)
if echo "$SHARD1_TEST_RESULT" | grep -q "not leader for shard"; then
    SHARD1_LEADER="8083"
    echo "Shard 1 Leader: Node 4 (port 8083)"
    # Clean up test on actual leader
    curl -s -X DELETE http://localhost:8083/v1/keys/_probe0 > /dev/null 2>&1
else
    SHARD1_LEADER="8082"
    echo "Shard 1 Leader: Node 3 (port 8082)"
    # Clean up test
    curl -s -X DELETE http://localhost:8082/v1/keys/_probe0 > /dev/null 2>&1
fi
echo ""

echo "=== Test 1: Determine Key to Shard Mapping ==="
echo "Calculating which shard each test key belongs to..."
python3 << 'EOF'
import binascii
keys = ['user:alice', 'test0', 'replicated', 'key1', 'key2', 'key3', 'key4', 'key5']
for key in keys:
    shard = binascii.crc32(key.encode()) % 2
    print(f"  {key:15s} -> Shard {shard}")
EOF
echo ""

echo "=== Test 2: Writing Keys to Correct Shard Leaders ==="
echo "Writing keys that belong to Shard 0..."
curl -X PUT http://localhost:$SHARD0_LEADER/v1/keys/user:alice -d "Alice Smith" 2>/dev/null && echo "✓ Wrote user:alice to Shard 0"
curl -X PUT http://localhost:$SHARD0_LEADER/v1/keys/test0 -d "shard0-data" 2>/dev/null && echo "✓ Wrote test0 to Shard 0"
curl -X PUT http://localhost:$SHARD0_LEADER/v1/keys/key1 -d "value1" 2>/dev/null && echo "✓ Wrote key1 to Shard 0"
curl -X PUT http://localhost:$SHARD0_LEADER/v1/keys/key2 -d "value2" 2>/dev/null && echo "✓ Wrote key2 to Shard 0"
curl -X PUT http://localhost:$SHARD0_LEADER/v1/keys/key3 -d "value3" 2>/dev/null && echo "✓ Wrote key3 to Shard 0"
echo ""

echo "Writing keys that belong to Shard 1..."
curl -X PUT http://localhost:$SHARD1_LEADER/v1/keys/replicated -d "shard1-data" 2>/dev/null && echo "✓ Wrote replicated to Shard 1"
curl -X PUT http://localhost:$SHARD1_LEADER/v1/keys/key4 -d "value4" 2>/dev/null && echo "✓ Wrote key4 to Shard 1"
curl -X PUT http://localhost:$SHARD1_LEADER/v1/keys/key5 -d "value5" 2>/dev/null && echo "✓ Wrote key5 to Shard 1"
echo ""

echo "=== Test 3: Reading Keys from Shard Leaders ==="
echo -n "user:alice from Shard 0: "
curl -s http://localhost:$SHARD0_LEADER/v1/keys/user:alice
echo ""
echo -n "replicated from Shard 1: "
curl -s http://localhost:$SHARD1_LEADER/v1/keys/replicated
echo ""
echo ""

echo "=== Test 4: Check Node Status ==="
echo "Node 1 (Shard 0):"
curl -s http://localhost:8080/v1/status | jq '{node_id, is_leader, num_keys, status}'
echo ""

echo "Node 2 (Shard 0):"
curl -s http://localhost:8081/v1/status | jq '{node_id, is_leader, num_keys, status}'
echo ""

echo "Node 3 (Shard 1):"
curl -s http://localhost:8082/v1/status | jq '{node_id, is_leader, num_keys, status}'
echo ""

echo "Node 4 (Shard 1):"
curl -s http://localhost:8083/v1/status | jq '{node_id, is_leader, num_keys, status}'
echo ""

echo "=== Test 5: Verify Replication Within Shards ==="
SHARD0_NODE1=$(curl -s http://localhost:8080/v1/status | jq '.num_keys')
SHARD0_NODE2=$(curl -s http://localhost:8081/v1/status | jq '.num_keys')
SHARD1_NODE3=$(curl -s http://localhost:8082/v1/status | jq '.num_keys')
SHARD1_NODE4=$(curl -s http://localhost:8083/v1/status | jq '.num_keys')

echo "Shard 0 - Node1: $SHARD0_NODE1 keys"
echo "Shard 0 - Node2: $SHARD0_NODE2 keys"
echo ""
echo "Shard 1 - Node3: $SHARD1_NODE3 keys"
echo "Shard 1 - Node4: $SHARD1_NODE4 keys"
echo ""

if [ "$SHARD0_NODE1" = "$SHARD0_NODE2" ]; then
    echo "✓ Shard 0 replication working correctly"
else
    echo "⚠️  Warning: Shard 0 replication mismatch"
fi

if [ "$SHARD1_NODE3" = "$SHARD1_NODE4" ]; then
    echo "✓ Shard 1 replication working correctly"
else
    echo "⚠️  Warning: Shard 1 replication mismatch"
fi
echo ""

echo "=== Test 6: Control Plane Status ==="
echo "Registered nodes:"
curl -s http://localhost:7000/v1/control/nodes | jq '.nodes | length'
echo " nodes registered"
echo ""

echo "========================================"
echo "All tests completed!"
echo "========================================"
```

---

## Quick Test Commands (Copy-Paste Ready)

### First: Determine Current Shard Leaders

Since leadership can change during startup, always check first:

**IMPORTANT:** The `/v1/status` endpoint shows **main Raft** leadership, which may be different from **shard Raft** leadership. To find the actual shard leader, try writing a test key:

```bash
# Method 1: Try writing and check for "not leader for shard" error
echo "Testing Shard 0 leadership:"
curl -X PUT http://localhost:8080/v1/keys/_probe -d "test" 2>&1 | grep -q "not leader" && echo "Node 1 (8080): Follower" || echo "Node 1 (8080): LEADER"
curl -X PUT http://localhost:8081/v1/keys/_probe -d "test" 2>&1 | grep -q "not leader" && echo "Node 2 (8081): Follower" || echo "Node 2 (8081): LEADER"

echo "Testing Shard 1 leadership:"
curl -X PUT http://localhost:8082/v1/keys/_probe -d "test" 2>&1 | grep -q "not leader" && echo "Node 3 (8082): Follower" || echo "Node 3 (8082): LEADER"
curl -X PUT http://localhost:8083/v1/keys/_probe -d "test" 2>&1 | grep -q "not leader" && echo "Node 4 (8083): Follower" || echo "Node 4 (8083): LEADER"

# Method 2: Check main Raft status (may differ from shard leadership!)
echo "Main Raft Leadership (for reference only):"
echo "Node 1:" && curl -s http://localhost:8080/v1/status | jq '.node_id, .is_leader'
echo "Node 2:" && curl -s http://localhost:8081/v1/status | jq '.node_id, .is_leader'
echo "Node 3:" && curl -s http://localhost:8082/v1/status | jq '.node_id, .is_leader'
echo "Node 4:" && curl -s http://localhost:8083/v1/status | jq '.node_id, .is_leader'
```

### Test 1: Calculate Which Shard a Key Belongs To

```bash
# Use Python to calculate shard assignment
python3 -c "import binascii; key='mykey'; print(f'{key} belongs to Shard {binascii.crc32(key.encode()) % 2}')"
```

### Test 2: Write and Read from Correct Shard

```bash
# Example: If "user:alice" belongs to Shard 0 and Node 2 is the leader (port 8081)
curl -X PUT http://localhost:8081/v1/keys/user:alice -d "Alice Smith"
curl -s http://localhost:8081/v1/keys/user:alice

# Example: If "testkey" belongs to Shard 1 and Node 4 is the leader (port 8083)
curl -X PUT http://localhost:8083/v1/keys/testkey -d "testvalue"
curl -s http://localhost:8083/v1/keys/testkey
```

### Test 3: Check All Node Status

```bash
# Check all nodes
curl -s http://localhost:8080/v1/status | jq '.'
curl -s http://localhost:8081/v1/status | jq '.'
curl -s http://localhost:8082/v1/status | jq '.'
curl -s http://localhost:8083/v1/status | jq '.'
```

### Test 4: Verify Replication Within Shards

```bash
# Write to the shard leader, wait, then check key counts on both nodes
# Both nodes in a shard should have identical key counts

echo "Shard 0 key counts:"
echo "Node 1: $(curl -s http://localhost:8080/v1/status | jq '.num_keys')"
echo "Node 2: $(curl -s http://localhost:8081/v1/status | jq '.num_keys')"

echo "Shard 1 key counts:"
echo "Node 3: $(curl -s http://localhost:8082/v1/status | jq '.num_keys')"
echo "Node 4: $(curl -s http://localhost:8083/v1/status | jq '.num_keys')"
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

### Test 7: Add Multiple Keys and Verify Distribution

```bash
# First determine which keys go to which shard
python3 << 'EOF'
import binascii
for i in range(1, 11):
    key = f"key{i}"
    shard = binascii.crc32(key.encode()) % 2
    print(f"{key} -> Shard {shard}")
EOF

# Then write each key to its correct shard leader
# (Replace 8081/8083 with actual leader ports discovered earlier)

# Shard 0 keys (example)
for key in key1 key2 key3; do
  curl -X PUT http://localhost:8081/v1/keys/$key -d "value_$key" 2>/dev/null
  echo "Added $key to Shard 0"
done

# Shard 1 keys (example)
for key in key4 key5; do
  curl -X PUT http://localhost:8083/v1/keys/$key -d "value_$key" 2>/dev/null
  echo "Added $key to Shard 1"
done

# Verify distribution
echo "Keys in Shard 0: $(curl -s http://localhost:8080/v1/status | jq '.num_keys')"
echo "Keys in Shard 1: $(curl -s http://localhost:8082/v1/status | jq '.num_keys')"
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

### Issue: "not leader for shard X" errors
**This is NORMAL!** It means you're trying to write to a follower node. 

**Solution:** 
1. Check which node is the leader for that shard:
   ```bash
   curl -s http://localhost:8080/v1/status | jq '.is_leader'  # Check node1
   curl -s http://localhost:8081/v1/status | jq '.is_leader'  # Check node2
   ```
2. Write to the node where `is_leader: true`

**Important:** Leadership can shift during cluster startup. The first node to bootstrap a shard may not remain the leader. Always check current leadership before writing.

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

**Solution:** Write/read from a node that owns that shard (check which node is the leader for that shard).

### Issue: Replication not working within a shard
**Solution:**
- Wait 3-5 seconds for Raft consensus
- Check both nodes in the shard have joined the same Raft group
- Verify both nodes show the same `num_keys` in their status

### Issue: Multiple leaders in the same shard
**Problem:** If you see node2 or node4 becoming leaders (instead of followers), it means they bootstrapped as independent clusters instead of joining the existing clusters.

**Solution:** Make sure node2 and node4 use the `--join` flag:
- node2 must have `--join=http://localhost:8080` (to join node1's cluster)
- node4 must have `--join=http://localhost:8082` (to join node3's cluster)

**How to verify:** Check the Raft state:
```bash
# Shard 0 should have only ONE leader total (either node1 or node2)
curl -s http://localhost:8080/v1/status | jq '.is_leader'
curl -s http://localhost:8081/v1/status | jq '.is_leader'

# Shard 1 should have only ONE leader total (either node3 or node4)
curl -s http://localhost:8082/v1/status | jq '.is_leader'
curl -s http://localhost:8083/v1/status | jq '.is_leader'
```

If both nodes in a shard show `is_leader: true`, you have a split-brain issue. Clean data and restart with proper `--join` flags.

### Issue: No jq installed
**Solution:** Install jq or remove `| jq '.'` from commands:
```bash
sudo apt install jq  # Ubuntu/Debian
```

### Issue: Writes failing with "not leader for shard" even to leader node
**Solution:** Wait 5-10 seconds after cluster startup for leadership elections to complete. Leadership may take a moment to stabilize across all shards.

---

## Important Limitations

⚠️ **Stale Reads Not Supported**: Unlike the basic setup, the sharded version does NOT support `?stale=true` for follower reads. All reads must go through the shard leader. This is a current limitation of the implementation.

⚠️ **Dynamic Leadership**: Leadership can shift between nodes during startup. The node that bootstraps a shard may not remain the leader. Always check shard leadership before writing.

⚠️ **Main Raft vs Shard Raft Leadership**: The `/v1/status` endpoint shows **main Raft** leadership (`.is_leader`), which can be DIFFERENT from **shard Raft** leadership. A node can be a main Raft leader but a shard Raft follower (or vice versa). To find the actual shard leader, you must test writes to each node.

⚠️ **Manual Leader Discovery**: You must manually determine which node is the leader for each shard before performing operations. The recommended method is to attempt a write and check for "not leader for shard" errors.

---

## Architecture Summary

```
┌─────────────────────────────────────────────────┐
│          CLIENT (Terminal 6)                    │
│      Must route requests to correct shard       │
│           leader (check is_leader)              │
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
   │ node1    │     │ node3    │  ← One will be leader
   │ HTTP:8080│     │ HTTP:8082│
   │ MRaft:   │     │ MRaft:   │  (Main Raft)
   │  11000   │     │  11002   │
   │ SRaft:   │     │ SRaft:   │  (Shard Raft)
   │  12001   │     │  12103   │
   │          │     │          │
   │ node2    │     │ node4    │  ← One will be leader
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
- CRC32(key) % 2 = 0 → Shard 0 (node1 or node2, whichever is leader)
- CRC32(key) % 2 = 1 → Shard 1 (node3 or node4, whichever is leader)

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
- Each shard has 1 leader and 1 follower (leadership may shift during startup)

✅ **When you add keys:**
- Keys must be written to the correct shard leader (check `is_leader` first)
- Keys distributed approximately 50/50 across shards based on CRC32 hash
- Each key replicated to both nodes within its shard

✅ **When shard leader fails:**
- Follower in that shard automatically becomes leader
- No data loss within that shard
- Keys in the failed shard can still be accessed via new leader
- Other shard continues operating normally

✅ **When you query any node:**
- Must query the shard leader for that key
- Follower nodes will return "not leader for shard X"
- Check `is_leader` status to find current leader

---

## Key Differences from Basic Setup

| Feature | Basic Setup (BASIC_SETUP.md) | Sharded Setup (This Guide) |
|---------|----------------------------------|---------------------------|
| Architecture | 2 independent clusters | 1 unified cluster with 2 shards |
| Key Routing | Manual (must specify cluster) | Based on CRC32 hash (automatic distribution) |
| Data Distribution | No distribution | Automatic ~50/50 split across shards |
| Control Plane | Not required | Required for coordination |
| Nodes per Shard | N/A | 2 nodes (replication) |
| Leadership | Static after bootstrap | Dynamic, can shift during startup |
| Follower Reads | Supported with `?stale=true` | NOT supported (must use leader) |
| Fault Tolerance | Per-cluster | Per-shard |
| Complexity | Simple | Advanced |

---

## Performance Considerations

- **Writes**: Must go to shard leader, slower due to Raft consensus
- **Reads**: Must go to shard leader (no stale reads supported)
- **Scalability**: Can add more shards by increasing `--shard-count`
- **Fault Tolerance**: Each shard can lose 1 node (50% of shard nodes)
- **Leadership Changes**: May require discovering new leader after failures

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

## Quick Start Script (Run Everything in Background)

```bash
#!/bin/bash
# Save as: start_sharded_cluster.sh

cd ~/IITR/sem5/csc303-cn/keyper/Keyper

# Clean old data
echo "Cleaning old data..."
pkill -f "bin/server" 2>/dev/null
pkill -f "bin/control" 2>/dev/null
sleep 2
rm -rf shard*-node* ctrl-data logs/
mkdir -p ctrl-data logs

# Start control plane
echo "Starting control plane..."
./bin/control --node-id=control1 --http-addr=:7000 --raft-addr=127.0.0.1:7001 --data-dir=./ctrl-data --bootstrap > logs/control.log 2>&1 &
sleep 3

# Start shard 0 nodes
echo "Starting Shard 0 nodes..."
./bin/server --node-id=node1 --http-addr=:8080 --raft-addr=127.0.0.1:11000 --data-dir=./shard0-node1 --shard-count=2 --assigned-shards="0" --raft-base-port=12000 --control-plane=localhost:7000 --enable-raft > logs/node1.log 2>&1 &
sleep 3

./bin/server --node-id=node2 --http-addr=:8081 --raft-addr=127.0.0.1:11001 --data-dir=./shard0-node2 --shard-count=2 --assigned-shards="0" --raft-base-port=12000 --control-plane=localhost:7000 --join=http://localhost:8080 --enable-raft > logs/node2.log 2>&1 &
sleep 3

# Start shard 1 nodes
echo "Starting Shard 1 nodes..."
./bin/server --node-id=node3 --http-addr=:8082 --raft-addr=127.0.0.1:11002 --data-dir=./shard1-node3 --shard-count=2 --assigned-shards="1" --raft-base-port=12000 --control-plane=localhost:7000 --enable-raft > logs/node3.log 2>&1 &
sleep 3

./bin/server --node-id=node4 --http-addr=:8083 --raft-addr=127.0.0.1:11003 --data-dir=./shard1-node4 --shard-count=2 --assigned-shards="1" --raft-base-port=12000 --control-plane=localhost:7000 --join=http://localhost:8082 --enable-raft > logs/node4.log 2>&1 &

echo "Waiting for cluster to stabilize..."
sleep 10

echo ""
echo "Cluster started! Check status with:"
echo "  curl -s http://localhost:8080/v1/status | jq '.'"
echo ""
echo "Find shard leaders with:"
echo "  curl -s http://localhost:8080/v1/status | jq '.node_id, .is_leader'"
echo "  curl -s http://localhost:8081/v1/status | jq '.node_id, .is_leader'"
echo "  curl -s http://localhost:8082/v1/status | jq '.node_id, .is_leader'"
echo "  curl -s http://localhost:8083/v1/status | jq '.node_id, .is_leader'"
```

---

**Enjoy your production-ready sharded distributed key-value store!** 🚀

For questions or issues, check the main README.md or the troubleshooting section above.
