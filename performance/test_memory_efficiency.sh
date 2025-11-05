#!/bin/bash

# Keyper Memory Efficiency Performance Test
# Tests memory usage of Keyper server under various loads

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
MEMORY_LIMIT_MB=100
TEST_DURATION=30

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

print_header() {
    echo ""
    echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}║              KEYPER MEMORY EFFICIENCY TEST                   ║${NC}"
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
    sleep 1
    rm -rf test-memory-*/ memory-*.log 2>/dev/null || true
}

check_prerequisites() {
    if [ ! -f "../bin/server" ]; then
        echo -e "${RED}Error: bin/server not found. Please build the server first:${NC}"
        echo "  go build -o bin/server ./cmd/server"
        exit 1
    fi
}

get_memory_usage() {
    local pid=$1
    if [ -z "$pid" ] || ! kill -0 $pid 2>/dev/null; then
        echo "0"
        return
    fi
    
    # Get RSS memory in KB, convert to MB
    local mem_kb=$(ps -o rss= -p $pid 2>/dev/null | tr -d ' ')
    if [ -z "$mem_kb" ] || [ "$mem_kb" = "0" ]; then
        echo "0"
    else
        echo $((mem_kb / 1024))
    fi
}

monitor_memory() {
    local pid=$1
    local duration=$2
    local interval=1
    local max_memory=0
    local samples=0
    local total_memory=0
    
    for i in $(seq 1 $duration); do
        local current_mem=$(get_memory_usage $pid)
        if [ "$current_mem" -gt "$max_memory" ]; then
            max_memory=$current_mem
        fi
        total_memory=$((total_memory + current_mem))
        ((samples++))
        sleep $interval
    done
    
    local avg_memory=$((total_memory / samples))
    echo "$max_memory $avg_memory"
}

test_idle_memory() {
    print_test "Idle server memory usage"
    ((TOTAL++))
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="memory-idle" \
        --http-addr=":8080" \
        --raft-addr="127.0.0.1:9080" \
        --data-dir="./test-memory-idle" \
         > memory-idle.log 2>&1 &
    
    local pid=$!
    sleep 3  # Let server stabilize
    
    # Monitor memory for 10 seconds
    local memory_stats=$(monitor_memory $pid 10)
    local max_memory=$(echo $memory_stats | cut -d' ' -f1)
    local avg_memory=$(echo $memory_stats | cut -d' ' -f2)
    
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    
    if [ "$max_memory" -le "$MEMORY_LIMIT_MB" ] && [ "$max_memory" -gt 0 ]; then
        test_success "Idle server memory usage"
        test_info "Peak: ${max_memory}MB, Average: ${avg_memory}MB (limit: ${MEMORY_LIMIT_MB}MB)"
    else
        test_failure "Idle server memory usage"
        test_info "Peak: ${max_memory}MB exceeds limit: ${MEMORY_LIMIT_MB}MB"
    fi
    
    rm -rf test-memory-idle/ memory-idle.log
}

test_cluster_memory() {
    print_test "3-node cluster memory usage"
    ((TOTAL++))
    
    cleanup
    
    # Start 3-node cluster
    ../bin/server \
        --node-id="mem-node1" \
        --http-addr=":8081" \
        --raft-addr="127.0.0.1:9081" \
        --data-dir="./test-memory-node1" \
         > memory-node1.log 2>&1 &
    local pid1=$!
    
    sleep 3
    
    ../bin/server \
        --node-id="mem-node2" \
        --http-addr=":8082" \
        --raft-addr="127.0.0.1:9082" \
        --data-dir="./test-memory-node2" \
        --enable-raft --join="http://127.0.0.1:9081" > memory-node2.log 2>&1 &
    local pid2=$!
    
    ../bin/server \
        --node-id="mem-node3" \
        --http-addr=":8083" \
        --raft-addr="127.0.0.1:9083" \
        --data-dir="./test-memory-node3" \
        --enable-raft --join="http://127.0.0.1:9081" > memory-node3.log 2>&1 &
    local pid3=$!
    
    sleep 5  # Let cluster stabilize
    
    # Monitor each node's memory
    local mem1=$(get_memory_usage $pid1)
    local mem2=$(get_memory_usage $pid2)
    local mem3=$(get_memory_usage $pid3)
    local total_mem=$((mem1 + mem2 + mem3))
    local avg_mem=$((total_mem / 3))
    
    # Cleanup
    kill $pid1 $pid2 $pid3 2>/dev/null || true
    wait $pid1 $pid2 $pid3 2>/dev/null || true
    
    if [ "$avg_mem" -le "$MEMORY_LIMIT_MB" ] && [ "$total_mem" -gt 0 ]; then
        test_success "3-node cluster memory usage"
        test_info "Node1: ${mem1}MB, Node2: ${mem2}MB, Node3: ${mem3}MB"
        test_info "Average per node: ${avg_mem}MB (limit: ${MEMORY_LIMIT_MB}MB)"
    else
        test_failure "3-node cluster memory usage"
        test_info "Average ${avg_mem}MB exceeds limit or cluster failed"
    fi
    
    rm -rf test-memory-node*/ memory-node*.log
}

test_memory_under_load() {
    print_test "Memory usage under load"
    ((TOTAL++))
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="memory-load" \
        --http-addr=":8084" \
        --raft-addr="127.0.0.1:9084" \
        --data-dir="./test-memory-load" \
         > memory-load.log 2>&1 &
    
    local pid=$!
    sleep 3
    
    # Generate load: insert 100 keys sequentially (avoid overwhelming server)
    for i in {1..100}; do
        curl -s -X PUT "http://localhost:8084/v1/keys/testkey${i}" -H "Content-Type: application/json" -d "\"testvalue${i}\"" >/dev/null 2>&1
        if [ $((i % 10)) -eq 0 ]; then
            sleep 0.1  # Brief pause every 10 requests
        fi
    done
    
    sleep 2  # Let server process all requests
    
    # Check memory after load
    local mem_after_load=$(get_memory_usage $pid)
    
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    
    if [ "$mem_after_load" -le $((MEMORY_LIMIT_MB * 2)) ] && [ "$mem_after_load" -gt 0 ]; then
        test_success "Memory usage under load"
        test_info "Memory after 100 keys: ${mem_after_load}MB (limit: $((MEMORY_LIMIT_MB * 2))MB)"
    else
        test_failure "Memory usage under load"
        test_info "Memory ${mem_after_load}MB exceeds acceptable limit"
    fi
    
    rm -rf test-memory-load/ memory-load.log
}

test_memory_leak() {
    print_test "Memory leak detection"
    ((TOTAL++))
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="memory-leak" \
        --http-addr=":8085" \
        --raft-addr="127.0.0.1:9085" \
        --data-dir="./test-memory-leak" \
         > memory-leak.log 2>&1 &
    
    local pid=$!
    sleep 3
    
    # Get baseline memory
    local baseline_mem=$(get_memory_usage $pid)
    
    # Perform operations in cycles
    for cycle in {1..5}; do
        # Add 100 keys
        for i in {1..100}; do
            curl -s -X PUT "http://localhost:8085/v1/keys/cycle${cycle}_key${i}" -d "value${i}" >/dev/null 2>&1
        done
        
        # Delete 50 keys
        for i in {1..50}; do
            curl -s -X DELETE "http://localhost:8085/v1/keys/cycle${cycle}_key${i}" >/dev/null 2>&1
        done
        
        sleep 2
    done
    
    # Check final memory
    local final_mem=$(get_memory_usage $pid)
    local mem_growth=$((final_mem - baseline_mem))
    
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    
    # Allow up to 20MB growth for normal operation
    if [ "$mem_growth" -le 20 ] && [ "$final_mem" -gt 0 ]; then
        test_success "Memory leak detection"
        test_info "Memory growth: ${mem_growth}MB (baseline: ${baseline_mem}MB → final: ${final_mem}MB)"
    else
        test_failure "Memory leak detection"
        test_info "Excessive memory growth: ${mem_growth}MB (may indicate leak)"
    fi
    
    rm -rf test-memory-leak/ memory-leak.log
}

test_memory_recovery() {
    print_test "Memory recovery after load"
    ((TOTAL++))
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="memory-recovery" \
        --http-addr=":8086" \
        --raft-addr="127.0.0.1:9086" \
        --data-dir="./test-memory-recovery" \
         > memory-recovery.log 2>&1 &
    
    local pid=$!
    sleep 3
    
    local baseline_mem=$(get_memory_usage $pid)
    
    # Create moderate load (reduced to avoid overwhelming server)
    for i in {1..50}; do
        curl -s -X PUT "http://localhost:8086/v1/keys/heavy_key${i}" -H "Content-Type: application/json" -d "\"$(printf 'x%.0s' {1..100})\"" >/dev/null 2>&1
        if [ $((i % 5)) -eq 0 ]; then
            sleep 0.1
        fi
    done
    
    local peak_mem=$(get_memory_usage $pid)
    
    # Delete all keys
    for i in {1..50}; do
        curl -s -X DELETE "http://localhost:8086/v1/keys/heavy_key${i}" >/dev/null 2>&1
        if [ $((i % 5)) -eq 0 ]; then
            sleep 0.1
        fi
    done
    
    sleep 5  # Allow cleanup
    
    local recovered_mem=$(get_memory_usage $pid)
    
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    
    # Check memory recovery (simplified logic for stable behavior)
    local memory_increase=$((peak_mem - baseline_mem))
    
    if [ "$memory_increase" -le 5 ]; then
        # If memory didn't increase much, consider it good behavior
        test_success "Memory recovery after load"
        test_info "Stable memory: baseline ${baseline_mem}MB, peak ${peak_mem}MB, recovered ${recovered_mem}MB"
    elif [ "$recovered_mem" -le $((baseline_mem + 10)) ]; then
        # If recovered memory is close to baseline, that's good
        test_success "Memory recovery after load"
        test_info "Good recovery: ${baseline_mem}MB → ${peak_mem}MB → ${recovered_mem}MB"
    else
        test_failure "Memory recovery after load"
        test_info "Poor recovery: ${baseline_mem}MB → ${peak_mem}MB → ${recovered_mem}MB"
    fi
    
    rm -rf test-memory-recovery/ memory-recovery.log
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Memory Efficiency Test Results${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total Tests:    ${BOLD}$TOTAL${NC}"
    echo -e "  ${GREEN}Passed:         $PASSED${NC}"
    echo -e "  ${RED}Failed:         $FAILED${NC}"
    
    local pass_rate=$((PASSED * 100 / TOTAL))
    echo -e "  Pass Rate:      ${BOLD}${pass_rate}%${NC}"
    echo ""
    echo -e "  Memory Limit:   ${BOLD}${MEMORY_LIMIT_MB}MB per node${NC}"
    echo ""
    
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL MEMORY TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper memory efficiency is optimal${NC}"
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review memory usage patterns${NC}"
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
echo -e "${BOLD}Running memory efficiency tests...${NC}"
echo ""

test_idle_memory
test_cluster_memory
test_memory_under_load
test_memory_leak
test_memory_recovery

print_summary
