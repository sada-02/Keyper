#!/bin/bash

# Keyper Cluster Scalability Performance Test
# Tests system performance with increasing number of nodes (1-10)

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
MAX_NODES=10

# Test data storage
NODE_COUNTS=()
STARTUP_TIMES=()
MEMORY_USAGE=()
CONNECTION_TIMES=()
    
    mkdir -p test-scale-node1
    ../bin/server \
        --data-dir="./test-scale-node1" \
        --http-addr=":$http_port" \
        --node-id="scale-node1" \
        --enable-raft \
        --raft-addr="127.0.0.1:$raft_port" > "scale-node1.log" 2>&1 &
# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
MIN_NODES=1
MAX_NODES=10
TEST_TIMEOUT=60

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Test results
PASSED=0
FAILED=0
TOTAL=0

# Arrays to store results
declare -a NODE_COUNTS
declare -a STARTUP_TIMES
declare -a MEMORY_USAGE
declare -a CONNECTION_TIMES

print_header() {
    echo ""
    echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}║             KEYPER CLUSTER SCALABILITY TEST                  ║${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_test() {
    echo -ne "${YELLOW}  ⟳${NC} $1..."
}

test_success() {
    echo -e "\r${GREEN}  ✓${NC} $1 ${GREEN}[PASSED]${NC}"
    ((PASSED++))
}

test_failure() {
    echo -e "\r${RED}  ✗${NC} $1 ${RED}[FAILED]${NC}"
    ((FAILED++))
}

test_info() {
    echo -e "    ${CYAN}→${NC} $1"
}

cleanup() {
    pkill -9 -f "../bin/server" 2>/dev/null || true
    pkill -9 -f "keyper" 2>/dev/null || true
    sleep 2
    rm -rf test-scale-*/ scale-*.log 2>/dev/null || true
}

check_prerequisites() {
    if [ ! -f "../bin/server" ]; then
        echo -e "${RED}Error: bin/server not found. Please build the server first:${NC}"
        echo "  go build -o bin/server ./cmd/server"
        exit 1
    fi
    
    # Check if we have enough port range available
    local required_ports=$((MAX_NODES * 2))  # HTTP + Raft ports
    echo -e "${BOLD}Testing scalability from $MIN_NODES to $MAX_NODES nodes${NC}"
    echo -e "${BOLD}Requires $required_ports ports (HTTP: $BASE_HTTP_PORT+, Raft: $BASE_RAFT_PORT+)${NC}"
}

wait_for_node_ready() {
    local http_port=$1
    local timeout=$2
    local attempts=0
    
    while [ $attempts -lt $timeout ]; do
        if curl -s "http://localhost:$http_port/v1/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
        ((attempts++))
    done
    return 1
}

get_cluster_memory() {
    local pids=("$@")
    local total_memory=0
    
    for pid in "${pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            local mem_kb=$(ps -o rss= -p $pid 2>/dev/null | tr -d ' ')
            if [ -n "$mem_kb" ] && [ "$mem_kb" != "0" ]; then
                total_memory=$((total_memory + mem_kb / 1024))
            fi
        fi
    done
    
    echo $total_memory
}

test_cluster_scale() {
    local node_count=$1
    print_test "Scaling to $node_count nodes"
    ((TOTAL++))
    
    cleanup
    local start_time=$(date +%s.%N)
    local pids=()
    local all_ready=true
    
    # Start bootstrap node (node 1)
    local http_port=$((BASE_HTTP_PORT))
    local raft_port=$((BASE_RAFT_PORT))
    
    mkdir -p test-scale-node1
    ../bin/server \
        --data-dir="./test-scale-node1" \
        --http-addr=":$http_port" \
        --node-id="scale-node1" \
        --enable-raft \
        --raft-addr="127.0.0.1:$raft_port" > "scale-node1.log" 2>&1 &
    
    local bootstrap_pid=$!
    pids+=($bootstrap_pid)
    
    # Wait for bootstrap node to be ready
    if ! wait_for_node_ready $http_port 20; then
        kill ${pids[@]} 2>/dev/null || true
        test_failure "Scaling to $node_count nodes"
        test_info "Bootstrap node failed to start"
        return
    fi
    
    # Start additional nodes
    for i in $(seq 2 $node_count); do
        http_port=$((BASE_HTTP_PORT + i - 1))
        raft_port=$((BASE_RAFT_PORT + i - 1))
        
        mkdir -p "test-scale-node$i"
        ../bin/server \
            --data-dir="./test-scale-node$i" \
            --http-addr=":$http_port" \
            --node-id="scale-node$i" \
            --enable-raft \
            --raft-addr="127.0.0.1:$raft_port" \
            --join="http://127.0.0.1:$BASE_HTTP_PORT" > "scale-node$i.log" 2>&1 &
        
        local node_pid=$!
        pids+=($node_pid)
        
        # Give nodes time to join properly
        sleep 2
    done
    
    # Wait for all nodes to be ready
    local ready_count=0
    for i in $(seq 1 $node_count); do
        local port=$((BASE_HTTP_PORT + i - 1))
        if wait_for_node_ready $port 30; then
            ((ready_count++))
        fi
    done
    
    local end_time=$(date +%s.%N)
    local total_time=$(echo "$end_time - $start_time" | bc -l)
    local total_time_int=$(printf "%.0f" "$total_time")
    
    # Check cluster health by testing leader election
    local leader_count=0
    for i in $(seq 1 $node_count); do
        local port=$((BASE_HTTP_PORT + i - 1))
        local status=$(curl -s "http://localhost:$port/v1/status" 2>/dev/null | grep -o '"is_leader":[^,}]*' | cut -d':' -f2 | tr -d ' "' || echo "false")
        if [ "$status" = "true" ]; then
            ((leader_count++))
        fi
    done
    
    # Get memory usage
    local total_memory=$(get_cluster_memory "${pids[@]}")
    
    # Cleanup
    kill ${pids[@]} 2>/dev/null || true
    sleep 2
    
    # Evaluation criteria
    local max_allowed_time=$((node_count * 10))  # 10 seconds per node
    local success=true
    
    if [ $ready_count -ne $node_count ]; then
        success=false
    elif [ $node_count -eq 1 ]; then
        # Single node doesn't need leader election, just needs to be responsive
        success=true
    elif [ $node_count -gt 1 ] && [ $leader_count -ne 1 ]; then
        success=false
    elif [ $total_time_int -gt $max_allowed_time ]; then
        success=false
    fi
    
    if [ "$success" = "true" ]; then
        test_success "Scaling to $node_count nodes"
        test_info "Ready: $ready_count/$node_count, Leader: $leader_count, Time: ${total_time_int}s, Memory: ${total_memory}MB"
        
        # Store results for analysis
        NODE_COUNTS+=($node_count)
        STARTUP_TIMES+=($total_time_int)
        MEMORY_USAGE+=($total_memory)
        CONNECTION_TIMES+=($total_time_int)
    else
        test_failure "Scaling to $node_count nodes"
        test_info "Ready: $ready_count/$node_count, Leader: $leader_count, Time: ${total_time_int}s (max: ${max_allowed_time}s)"
    fi
    
    rm -rf test-scale-node*/ scale-node*.log
}

test_concurrent_requests_scaling() {
    local node_count=$1
    print_test "Request handling with $node_count nodes"
    ((TOTAL++))
    
    cleanup
    local pids=()
    
    # Start cluster
    for i in $(seq 1 $node_count); do
        local http_port=$((BASE_HTTP_PORT + i - 1))
        local raft_port=$((BASE_RAFT_PORT + i - 1))
        local bootstrap_flag=""
        local join_flag=""
        
        mkdir -p "test-req-node$i"
        if [ $i -eq 1 ]; then
            bootstrap_flag="--enable-raft"
        else
            join_flag="--enable-raft --join=http://127.0.0.1:$BASE_HTTP_PORT"
        fi
        
        ../bin/server \
            --data-dir="./test-req-node$i" \
            --http-addr=":$http_port" \
            --node-id="req-node$i" \
            --raft-addr="127.0.0.1:$raft_port" \
            $bootstrap_flag $join_flag > "req-node$i.log" 2>&1 &
        
        pids+=($!)
        sleep 2
    done
    
    # Wait for cluster to be ready
    sleep 5
    
    # Test concurrent requests
    local start_time=$(date +%s.%N)
    local success_count=0
    local total_requests=100
    
    for i in $(seq 1 $total_requests); do
        local target_node=$((i % node_count + 1))
        local port=$((BASE_HTTP_PORT + target_node - 1))
        
        if curl -s -X PUT "http://localhost:$port/v1/keys/test_key_$i" -d "test_value_$i" >/dev/null 2>&1; then
            ((success_count++))
        fi &
        
        # Limit concurrent requests
        if [ $((i % 20)) -eq 0 ]; then
            wait
        fi
    done
    wait
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local requests_per_sec=$(echo "scale=1; $total_requests / $duration" | bc -l)
    
    # Cleanup
    kill ${pids[@]} 2>/dev/null || true
    
    local success_rate=$((success_count * 100 / total_requests))
    
    if [ $success_rate -ge 80 ]; then
        test_success "Request handling with $node_count nodes"
        test_info "$success_count/$total_requests requests succeeded (${requests_per_sec} req/sec)"
    else
        test_failure "Request handling with $node_count nodes"
        test_info "Only $success_count/$total_requests requests succeeded"
    fi
    
    rm -rf test-req-node*/ req-node*.log
}

test_node_failure_recovery() {
    print_test "Node failure recovery (5 nodes)"
    ((TOTAL++))
    
    cleanup
    local node_count=5
    local pids=()
    
    # Start 5-node cluster
    for i in $(seq 1 $node_count); do
        local http_port=$((BASE_HTTP_PORT + i - 1))
        local raft_port=$((BASE_RAFT_PORT + i - 1))
        local bootstrap_flag=""
        local join_flag=""
        
        mkdir -p "test-fail-node$i"
        if [ $i -eq 1 ]; then
            bootstrap_flag="--enable-raft"
        else
            join_flag="--enable-raft --join=http://127.0.0.1:$BASE_HTTP_PORT"
        fi
        
        ../bin/server \
            --data-dir="./test-fail-node$i" \
            --http-addr=":$http_port" \
            --node-id="fail-node$i" \
            --raft-addr="127.0.0.1:$raft_port" \
            $bootstrap_flag $join_flag > "fail-node$i.log" 2>&1 &
        
        pids+=($!)
        sleep 2
    done
    
    sleep 5  # Let cluster stabilize
    
    # Kill 2 nodes (minority)
    kill ${pids[1]} ${pids[2]} 2>/dev/null || true
    
    sleep 3  # Let cluster detect failures
    
    # Test if remaining nodes still work
    local working_nodes=0
    for i in 1 3 4 5; do  # Skip nodes 2 and 3 (indices 1,2)
        local port=$((BASE_HTTP_PORT + i - 1))
        if curl -s -X PUT "http://localhost:$port/v1/keys/recovery_test" -d "test_value" >/dev/null 2>&1; then
            ((working_nodes++))
        fi
    done
    
    # Cleanup remaining nodes
    kill ${pids[0]} ${pids[3]} ${pids[4]} 2>/dev/null || true
    
    if [ $working_nodes -ge 2 ]; then
        test_success "Node failure recovery (5 nodes)"
        test_info "$working_nodes/3 remaining nodes operational after 2 failures"
    else
        test_failure "Node failure recovery (5 nodes)"
        test_info "Only $working_nodes/3 remaining nodes operational"
    fi
    
    rm -rf test-fail-node*/ fail-node*.log
}

analyze_scalability_trends() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Scalability Analysis${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if [ ${#NODE_COUNTS[@]} -gt 0 ]; then
        echo -e "${BOLD}Performance by Node Count:${NC}"
        for i in "${!NODE_COUNTS[@]}"; do
            local nodes=${NODE_COUNTS[$i]}
            local startup_time=${STARTUP_TIMES[$i]}
            local memory=${MEMORY_USAGE[$i]}
            local avg_memory_per_node=$((memory / nodes))
            
            echo -e "  ${CYAN}${nodes} nodes:${NC} ${startup_time}s startup, ${memory}MB total (${avg_memory_per_node}MB/node)"
        done
        
        echo ""
        echo -e "${BOLD}Scalability Assessment:${NC}"
        
        # Check if startup time scales reasonably
        local first_time=${STARTUP_TIMES[0]}
        local last_time=${STARTUP_TIMES[-1]}
        local time_ratio=$(echo "scale=1; $last_time / $first_time" | bc -l 2>/dev/null || echo "1")
        
        echo -e "  Startup time scaling: ${time_ratio}x (${first_time}s → ${last_time}s)"
        
        # Check memory scaling
        if [ ${#MEMORY_USAGE[@]} -gt 1 ]; then
            local first_nodes=${NODE_COUNTS[0]}
            local last_nodes=${NODE_COUNTS[-1]}
            local first_memory=${MEMORY_USAGE[0]}
            local last_memory=${MEMORY_USAGE[-1]}
            
            local expected_memory=$((first_memory * last_nodes / first_nodes))
            local memory_efficiency=$(echo "scale=1; $expected_memory * 100 / $last_memory" | bc -l 2>/dev/null || echo "100")
            
            echo -e "  Memory efficiency: ${memory_efficiency}% (linear scaling baseline)"
        fi
    fi
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Scalability Test Results${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total Tests:    ${BOLD}$TOTAL${NC}"
    echo -e "  ${GREEN}Passed:         $PASSED${NC}"
    echo -e "  ${RED}Failed:         $FAILED${NC}"
    
    local pass_rate=$((PASSED * 100 / TOTAL))
    echo -e "  Pass Rate:      ${BOLD}${pass_rate}%${NC}"
    echo ""
    echo -e "  Node Range:     ${BOLD}${MIN_NODES}-${MAX_NODES} nodes${NC}"
    
    analyze_scalability_trends
    
    echo ""
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL SCALABILITY TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper scales well with increasing node count${NC}"
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review scalability bottlenecks${NC}"
        exit 1
    fi
}

# Main execution
trap cleanup EXIT

print_header

echo -e "${BOLD}Checking prerequisites...${NC}"
check_prerequisites
echo -e "${GREEN}✓ Prerequisites met${NC}"

echo ""
echo -e "${BOLD}Running scalability tests...${NC}"
echo ""

# Test different cluster sizes
for node_count in $(seq $MIN_NODES 2 $MAX_NODES); do
    test_cluster_scale $node_count
done

# Additional scalability tests
test_concurrent_requests_scaling 3
test_concurrent_requests_scaling 5
test_node_failure_recovery

print_summary
