#!/bin/bash
# Demo script to test master-slave connectivity

echo "=========================================="
echo "  DKVS Master-Slave Connection Demo"
echo "=========================================="
echo ""

# Clean up any existing instances
echo "Cleaning up any existing instances..."
pkill -9 -f dkvsd 2>/dev/null
sleep 1

# Build the application
echo "Building the application..."
go build -o dkvsd
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi
echo "✓ Build successful"
echo ""

# Start master server
echo "Starting Master Server..."
./dkvsd -role master -replicas 3 -local localhost:5000 -remote "" &
MASTER_PID=$!
sleep 2
echo ""

# Start first slave
echo "Starting Slave Server 1..."
./dkvsd -role slave -local localhost:5001 -remote localhost:5000 &
SLAVE1_PID=$!
sleep 2
echo ""

# Start second slave
echo "Starting Slave Server 2..."
./dkvsd -role slave -local localhost:5002 -remote localhost:5000 &
SLAVE2_PID=$!
sleep 2
echo ""

echo "=========================================="
echo "  All servers are running!"
echo "=========================================="
echo "Master PID: $MASTER_PID (localhost:5000)"
echo "Slave 1 PID: $SLAVE1_PID (localhost:5001)"
echo "Slave 2 PID: $SLAVE2_PID (localhost:5002)"
echo ""
echo "Press Ctrl+C to stop all servers..."
echo ""

# Wait for user interrupt
trap "echo ''; echo 'Stopping all servers...'; kill $MASTER_PID $SLAVE1_PID $SLAVE2_PID 2>/dev/null; exit 0" INT

# Keep script running
while true; do
    sleep 1
done
