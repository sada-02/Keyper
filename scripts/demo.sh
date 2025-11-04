#!/bin/bash

# Quick Demo Script - Shows example usage of the Keyper Web Demo

echo "========================================"
echo "  Keyper Web Demo - Quick Start Guide"
echo "========================================"
echo ""

echo "📚 This demo will:"
echo "  1. Start a 3-node Keyper cluster"
echo "  2. Launch the Web UI on http://localhost:9000"
echo "  3. Show you how to interact with the system"
echo ""

read -p "Press Enter to start the demo..."

# Start the demo
./scripts/start-web-demo.sh &
DEMO_PID=$!

# Wait for services to start
sleep 8

echo ""
echo "========================================"
echo "  ✅ Demo Environment is Ready!"
echo "========================================"
echo ""

echo "🌐 Open these URLs in your browser:"
echo ""
echo "  📊 Server Monitor (see cluster state):"
echo "     http://localhost:9000/server"
echo ""
echo "  🎮 Client Interface (send requests):"
echo "     http://localhost:9000/client"
echo ""

echo "========================================"
echo "  💡 Try These Examples:"
echo "========================================"
echo ""

echo "1️⃣  PUT Operations (store data):"
echo "   • Open: http://localhost:9000/client"
echo "   • Select: Node 1 (localhost:8080)"
echo "   • Method: PUT"
echo "   • Key: user:alice"
echo "   • Value: {\"name\":\"Alice\",\"age\":30}"
echo "   • Click: Send Request"
echo ""

echo "2️⃣  GET Operations (retrieve data):"
echo "   • Method: GET"
echo "   • Key: user:alice"
echo "   • Try different nodes!"
echo ""

echo "3️⃣  View Distribution:"
echo "   • Open: http://localhost:9000/server"
echo "   • See which nodes store which keys"
echo "   • Watch the LEADER/FOLLOWER badges"
echo ""

echo "4️⃣  Test Consistency:"
echo "   • PUT a key on Node 1"
echo "   • GET it from Node 2"
echo "   • Should see the same value!"
echo ""

echo "========================================"
echo "  📋 Using CURL (command line):"
echo "========================================"
echo ""

sleep 2

echo "Storing some sample data..."
curl -s -X PUT http://localhost:8080/v1/keys/user:1 -d "Alice" > /dev/null
echo "✅ Stored: user:1 = Alice"

curl -s -X PUT http://localhost:8081/v1/keys/user:2 -d "Bob" > /dev/null
echo "✅ Stored: user:2 = Bob"

curl -s -X PUT http://localhost:8082/v1/keys/user:3 -d "Charlie" > /dev/null
echo "✅ Stored: user:3 = Charlie"

sleep 1
echo ""

echo "Reading data from different nodes..."
echo ""
echo "GET user:1 from Node 1:"
curl -s http://localhost:8080/v1/keys/user:1
echo ""

echo "GET user:2 from Node 2:"
curl -s http://localhost:8081/v1/keys/user:2
echo ""

echo "GET user:3 from Node 3:"
curl -s http://localhost:8082/v1/keys/user:3
echo ""

sleep 1

echo ""
echo "========================================"
echo "  🔍 Check the Web UI Now!"
echo "========================================"
echo ""
echo "  Go to http://localhost:9000/server"
echo "  You should see the 3 keys distributed across nodes!"
echo ""

echo "========================================"
echo "  📊 Cluster Status:"
echo "========================================"
echo ""

# Get status from each node
for port in 8080 8081 8082; do
    echo "Node at localhost:$port:"
    curl -s http://localhost:$port/v1/status | python3 -m json.tool 2>/dev/null || curl -s http://localhost:$port/v1/status
    echo ""
done

echo "========================================"
echo "  🎓 Learning Exercises:"
echo "========================================"
echo ""

echo "Exercise 1: Data Distribution"
echo "  • Add 10 keys via the Web UI"
echo "  • Observe how they distribute across shards"
echo "  • Pattern: Keys hash to different shards"
echo ""

echo "Exercise 2: Leader Election"
echo "  • Note the current LEADER in Server Monitor"
echo "  • Open another terminal: ./scripts/stop-web-demo.sh"
echo "  • Then kill just the leader process"
echo "  • Watch new leader election happen!"
echo ""

echo "Exercise 3: Consistency Test"
echo "  • PUT key 'test' with value 'v1' on Node 1"
echo "  • Immediately GET 'test' from Node 2 and Node 3"
echo "  • All should return 'v1' (strong consistency)"
echo ""

echo "Exercise 4: Request Patterns"
echo "  • Send 5 PUT requests to different nodes"
echo "  • Watch Request History in Client Interface"
echo "  • See timestamps and response codes"
echo ""

echo "========================================"
echo "  📚 More Information:"
echo "========================================"
echo ""
echo "  • Full Guide: WEB_DEMO_README.md"
echo "  • API Docs: README.md"
echo "  • Logs: logs/node*.log, logs/webui.log"
echo ""

echo "========================================"
echo "  🛑 To Stop Everything:"
echo "========================================"
echo ""
echo "  Press Ctrl+C here, or run:"
echo "  ./scripts/stop-web-demo.sh"
echo ""

echo "Demo will continue running..."
echo "Have fun exploring Keyper! 🚀"
echo ""

# Wait for the demo process
wait $DEMO_PID
