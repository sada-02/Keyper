Keyper — README (current progress: Phase 1 + Phase 2)

This repository contains a small distributed key-value prototype built in Go using BadgerDB for local persistence and HashiCorp Raft for replication.
You’ve completed:

Phase 1 — single-node persisted Badger HTTP KV (PUT/GET/DELETE, GET /v1/status).

Phase 2 — Raft integration (single-node bootstrap, basic FSM, snapshot/restore, manual join endpoint and leader-enforced writes).

This README tells you exactly how to run everything up to now, run tests, do a basic 2-node join+failover test, where files are, and troubleshooting tips.

Prerequisites

Go (tested with go 1.25.x)
Confirm with:

go version


Git (recommended)

Ports 8080, 8081, and raft ports 12000, 12001 free on localhost for the examples below.

If you have network/proxy issues while fetching modules, set:

export GOPROXY=https://proxy.golang.org,direct

Quick repo layout (relevant files)
.
├─ cmd/
│  └─ server/         # main http server
├─ config/
│  └─ config.go       # CLI flags (data-dir, http-addr, raft flags)
├─ httpapi/
│  └─ handler.go      # HTTP endpoints: /v1/keys/, /v1/status, /v1/join
├─ raftnode/          # raft node + fsm (if you named folder `raft` adjust imports)
│  ├─ node.go
│  └─ fsm.go
├─ store/
│  ├─ store.go        # Badger wrapper with Get/Set/Delete + Export/Import
│  └─ store_test.go
├─ scripts/
│  └─ integration_2node.sh   # 2-node integration test script (start, join, failover)
├─ go.mod
└─ README.md


Important: your module path is github.com/sada-02/keyper. If you renamed folders or used a different module path, ensure imports match.

Install dependencies

From project root:

# ensure go.mod is correct, then:
go mod tidy


If go mod tidy errors about unknown revisions, try the steps in Troubleshooting below.

How to run (single-node, no Raft)

This runs the basic Badger-backed server (good for Phase 1).

go run ./cmd/server --data-dir ./node1-data --http-addr :8080 --node-id node1


Test API:

# Put
curl -X PUT http://localhost:8080/v1/keys/foo -d 'hello'

# Get
curl http://localhost:8080/v1/keys/foo

# Delete
curl -X DELETE http://localhost:8080/v1/keys/foo

# Status
curl http://localhost:8080/v1/status
# -> {"node_id":"node1","status":"ok",...}


Data files are in ./node1-data/ (Badger .sst, .vlog, MANIFEST, etc). Do not edit these files.

How to run with Raft enabled (single-node bootstrap)

This bootstraps a single-node Raft cluster (useful to test Raft code path).

go run ./cmd/server \
  --data-dir ./node1-data \
  --http-addr :8080 \
  --node-id node1 \
  --enable-raft \
  --raft-addr 127.0.0.1:12000


Expected behavior:

Writes (PUT/DELETE) go through Raft Apply.

GET reads directly from store (note: reads from followers are not linearizable in this simple implementation — see Next Steps).

Check status:

curl http://localhost:8080/v1/status
# shows "is_leader":true and "leader_addr":"127.0.0.1:12000"

How to add a second node (manual join) and test failover

Start node1 (leader) as above.

Start node2 (it will start but not be in the cluster until you call join):

go run ./cmd/server \
  --data-dir ./node2-data \
  --http-addr :8081 \
  --node-id node2 \
  --enable-raft \
  --raft-addr 127.0.0.1:12001 &


Ask the leader to add node2 (POST to /v1/join on leader):

curl -X POST http://127.0.0.1:8080/v1/join \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node2","raft_addr":"127.0.0.1:12001"}'
# should return 204 No Content


Confirm membership / leader:

curl http://127.0.0.1:8080/v1/status
curl http://127.0.0.1:8081/v1/status


Write a key to leader:

curl -X PUT http://127.0.0.1:8080/v1/keys/testkey -d 'replicated'


Kill the leader (simulate failure):

# if started in background, kill its PID or Ctrl+C in that terminal
# example if you started it and wrote PID into node1.pid:
kill $(cat node1.pid) || pkill -f "node1-data"


Wait for node2 to become leader and then read:

# poll until node2 shows is_leader true, then:
curl http://127.0.0.1:8081/v1/keys/testkey
# should return 'replicated'

Automated 2-node integration script

We included scripts/integration_2node.sh (see repo). Usage:

chmod +x scripts/integration_2node.sh
./scripts/integration_2node.sh


It starts node1, starts node2, has node1 add node2 via /v1/join, writes a key, kills node1, waits for node2 to be leader and verifies the key exists. Script prints PASS/FAIL.  

Run tests

Unit & store tests:

# run all tests with race detector
go test ./... -race


Run a specific test:

go test ./raftnode -run TestFSMApplySet -v
go test ./store -v


If you added the FSM/in-memory tests suggested earlier, run them too.
