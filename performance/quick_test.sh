#!/bin/bash

# Keyper Quick Performance Test
# Runs a subset of critical performance tests for rapid feedback

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080

print_header() {
    clear
    echo ""
    echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}║              KEYPER QUICK PERFORMANCE TEST                   ║${NC}"
    echo -e "${CYAN}${BOLD}║                Essential Tests (5-10 minutes)                ║${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

cleanup() {
    pkill -9 -f "../bin/server" 2>/dev/null || true
    sleep 1
    rm -rf test-quick-*/ quick-*.log 2>/dev/null || true
}

run_quick_test() {
    local test_name="$1"
    local description="$2"
    
    echo -e "${CYAN}${BOLD}Testing: $test_name${NC}"
    echo -e "${CYAN}$description${NC}"
    echo ""
}

test_basic_functionality() {
    run_quick_test "Basic Functionality" "Server startup and basic operations"
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="quick-test" \
        --http-addr=":$BASE_HTTP_PORT" \
        --raft-addr="127.0.0.1:$BASE_RAFT_PORT" \
        --data-dir="./test-quick-basic" \
         > quick-basic.log 2>&1 &
    
    local pid=$!
    local start_time=$(date +%s)
    
    # Wait for server to be ready
    local ready=false
    while [ $(($(date +%s) - start_time)) -lt 10 ]; do
        if curl -s "http://localhost:$BASE_HTTP_PORT/v1/status" >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 0.5
    done
    
    local startup_time=$(($(date +%s) - start_time))
    
    if [ "$ready" = "true" ]; then
        echo -e "${GREEN}  ✓ Server startup: ${startup_time}s${NC}"
        
        # Test basic operations
        local ops_success=0
        
        # PUT test
        if curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/test_key" -d "test_value" >/dev/null 2>&1; then
            ((ops_success++))
            echo -e "${GREEN}  ✓ PUT operation works${NC}"
        else
            echo -e "${RED}  ✗ PUT operation failed${NC}"
        fi
        
        # GET test
        if curl -s "http://localhost:$BASE_HTTP_PORT/v1/keys/test_key" | grep -q "test_value"; then
            ((ops_success++))
            echo -e "${GREEN}  ✓ GET operation works${NC}"
        else
            echo -e "${RED}  ✗ GET operation failed${NC}"
        fi
        
        # DELETE test
        if curl -s -X DELETE "http://localhost:$BASE_HTTP_PORT/v1/keys/test_key" >/dev/null 2>&1; then
            ((ops_success++))
            echo -e "${GREEN}  ✓ DELETE operation works${NC}"
        else
            echo -e "${RED}  ✗ DELETE operation failed${NC}"
        fi
        
        if [ $ops_success -eq 3 ]; then
            echo -e "${GREEN}${BOLD}  ✓ All basic operations successful${NC}"
        else
            echo -e "${RED}${BOLD}  ✗ Some operations failed ($ops_success/3)${NC}"
        fi
    else
        echo -e "${RED}  ✗ Server failed to start within 10s${NC}"
    fi
    
    kill $pid 2>/dev/null || true
    rm -rf test-quick-basic/ quick-basic.log
}

test_memory_usage() {
    run_quick_test "Memory Usage" "Basic memory efficiency check"
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="quick-memory" \
        --http-addr=":$((BASE_HTTP_PORT + 1))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 1))" \
        --data-dir="./test-quick-memory" \
         > quick-memory.log 2>&1 &
    
    local pid=$!
    sleep 3
    
    # Get memory usage
    local memory_kb=$(ps -o rss= -p $pid 2>/dev/null | tr -d ' ')
    if [ -n "$memory_kb" ] && [ "$memory_kb" != "0" ]; then
        local memory_mb=$((memory_kb / 1024))
        
        if [ $memory_mb -le 100 ]; then
            echo -e "${GREEN}  ✓ Memory usage: ${memory_mb}MB (excellent)${NC}"
        elif [ $memory_mb -le 200 ]; then
            echo -e "${YELLOW}  ⚠ Memory usage: ${memory_mb}MB (acceptable)${NC}"
        else
            echo -e "${RED}  ✗ Memory usage: ${memory_mb}MB (high)${NC}"
        fi
    else
        echo -e "${RED}  ✗ Could not measure memory usage${NC}"
    fi
    
    kill $pid 2>/dev/null || true
    rm -rf test-quick-memory/ quick-memory.log
}

test_basic_throughput() {
    run_quick_test "Basic Throughput" "Simple request processing test"
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="quick-throughput" \
        --http-addr=":$((BASE_HTTP_PORT + 2))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 2))" \
        --data-dir="./test-quick-throughput" \
         > quick-throughput.log 2>&1 &
    
    local pid=$!
    sleep 3
    
    # Run quick throughput test
    local requests=50
    local start_time=$(date +%s.%N)
    local success_count=0
    
    for i in $(seq 1 $requests); do
        if curl -s -X PUT "http://localhost:$((BASE_HTTP_PORT + 2))/v1/keys/quick_key_$i" -d "quick_value_$i" >/dev/null 2>&1; then
            ((success_count++))
        fi
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local throughput=$(echo "scale=1; $success_count / $duration" | bc -l)
    
    if [ $success_count -eq $requests ]; then
        echo -e "${GREEN}  ✓ Throughput: ${throughput} req/sec (${success_count}/${requests} successful)${NC}"
    else
        echo -e "${YELLOW}  ⚠ Throughput: ${throughput} req/sec (${success_count}/${requests} successful)${NC}"
    fi
    
    kill $pid 2>/dev/null || true
    rm -rf test-quick-throughput/ quick-throughput.log
}

test_cluster_formation() {
    run_quick_test "Cluster Formation" "2-node cluster startup"
    
    cleanup
    
    # Start bootstrap node
    mkdir -p test-quick-cluster-1 test-quick-cluster-2
    ../bin/server \
        --data-dir="./test-quick-cluster-1" \
        --http-addr=":$((BASE_HTTP_PORT + 3))" \
        --node-id="quick-cluster-1" \
        --enable-raft \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 3))" > quick-cluster-1.log 2>&1 &
    
    local pid1=$!
    sleep 3
    
    # Start second node
    ../bin/server \
        --data-dir="./test-quick-cluster-2" \
        --http-addr=":$((BASE_HTTP_PORT + 4))" \
        --node-id="quick-cluster-2" \
        --enable-raft \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 4))" \
        --join="http://127.0.0.1:$((BASE_HTTP_PORT + 3))" > quick-cluster-2.log 2>&1 &
    
    local pid2=$!
    sleep 5
    
    # Check both nodes
    local node1_healthy=false
    local node2_healthy=false
    
    if curl -s "http://localhost:$((BASE_HTTP_PORT + 3))/v1/status" >/dev/null 2>&1; then
        node1_healthy=true
    fi
    
    if curl -s "http://localhost:$((BASE_HTTP_PORT + 4))/v1/status" >/dev/null 2>&1; then
        node2_healthy=true
    fi
    
    if [ "$node1_healthy" = "true" ] && [ "$node2_healthy" = "true" ]; then
        echo -e "${GREEN}  ✓ 2-node cluster formed successfully${NC}"
        
        # Test cross-node operation
        if curl -s -X PUT "http://localhost:$((BASE_HTTP_PORT + 3))/v1/keys/cluster_test" -d "cluster_value" >/dev/null 2>&1; then
            if curl -s "http://localhost:$((BASE_HTTP_PORT + 4))/v1/keys/cluster_test" | grep -q "cluster_value"; then
                echo -e "${GREEN}  ✓ Cross-node replication works${NC}"
            else
                echo -e "${YELLOW}  ⚠ Cross-node replication issue${NC}"
            fi
        fi
    else
        echo -e "${RED}  ✗ Cluster formation failed (node1: $node1_healthy, node2: $node2_healthy)${NC}"
    fi
    
    kill $pid1 $pid2 2>/dev/null || true
    rm -rf test-quick-cluster-*/ quick-cluster-*.log
}

check_prerequisites() {
    echo -e "${BOLD}Quick prerequisite check...${NC}"
    
    if [ ! -f ".../bin/server" ]; then
        echo -e "${RED}Error: .../bin/server not found${NC}"
        echo "Building server..."
        cd ..
        if go build -o bin/server ./cmd/server; then
            echo -e "${GREEN}✓ Server built successfully${NC}"
            cd performance
        else
            echo -e "${RED}✗ Failed to build server${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}✓ Server binary found${NC}"
    fi
    
    # Quick port check
    local ports_busy=0
    for port in $BASE_HTTP_PORT $((BASE_HTTP_PORT+1)) $((BASE_HTTP_PORT+2)) $((BASE_HTTP_PORT+3)) $((BASE_HTTP_PORT+4)); do
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            ((ports_busy++))
        fi
    done
    
    if [ $ports_busy -gt 0 ]; then
        echo -e "${YELLOW}⚠ Warning: $ports_busy test ports are in use${NC}"
    else
        echo -e "${GREEN}✓ Test ports available${NC}"
    fi
    
    echo ""
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Quick Performance Test Complete${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "${GREEN}✓ Basic performance check completed${NC}"
    echo ""
    echo -e "${BOLD}For comprehensive testing, run:${NC}"
    echo -e "  ${CYAN}./run_all_performance_tests.sh${NC}"
    echo ""
    echo -e "${BOLD}Individual test categories:${NC}"
    echo -e "  ${CYAN}./test_startup_time.sh${NC}          - Startup performance"
    echo -e "  ${CYAN}./test_memory_efficiency.sh${NC}     - Memory analysis"
    echo -e "  ${CYAN}./test_cluster_scalability.sh${NC}   - Scalability testing"
    echo -e "  ${CYAN}./test_request_throughput.sh${NC}    - Throughput measurement"
    echo -e "  ${CYAN}./test_long_term_stability.sh${NC}   - Stability testing"
    echo ""
}

# Main execution
trap cleanup EXIT

print_header

echo -e "${BOLD}Keyper Quick Performance Test${NC}"
echo -e "${CYAN}This test provides rapid feedback on core performance aspects${NC}"
echo -e "${CYAN}Estimated runtime: 2-3 minutes${NC}"
echo ""

check_prerequisites

echo -e "${BOLD}Running quick performance tests...${NC}"
echo ""

test_basic_functionality
echo ""

test_memory_usage
echo ""

test_basic_throughput
echo ""

test_cluster_formation

print_summary
