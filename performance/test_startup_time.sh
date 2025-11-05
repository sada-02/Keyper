#!/bin/bash

# Keyper Startup Time Performance Test
# Tests how quickly a Keyper server can start up and become ready

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
TEST_TIMEOUT=30
TARGET_STARTUP_TIME=3

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
    echo -e "${CYAN}${BOLD}║               KEYPER STARTUP TIME PERFORMANCE                ║${NC}"
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
    # Kill any running Keyper processes
    pkill -9 -f "../bin/server" 2>/dev/null || true
    pkill -9 -f "keyper" 2>/dev/null || true
    sleep 1
    rm -rf test-startup-data/ startup-*.log 2>/dev/null || true
}

# Ensure we have the server binary
check_prerequisites() {
    if [ ! -f "../bin/server" ]; then
        echo -e "${RED}Error: bin/server not found. Please build the server first:${NC}"
        echo "  go build -o bin/server ./cmd/server"
        exit 1
    fi
}

run_startup_test() {
    local test_name="$1"
    local node_id="$2"
    local http_port="$3"
    local raft_port="$4"
    local bootstrap="$5"
    
    ((TOTAL++))
    print_test "$test_name"
    
    # Clean data directory
    rm -rf "test-startup-data-${node_id}/" 2>/dev/null || true
    
    # Record start time
    local start_time=$(date +%s.%N)
    
    # Start server
    local bootstrap_flag=""
    if [ "$bootstrap" = "true" ]; then
        bootstrap_flag=""
    fi
    
    ../bin/server \
        --node-id="$node_id" \
        --http-addr=":$http_port" \
        --raft-addr="127.0.0.1:$raft_port" \
        --data-dir="./test-startup-data-${node_id}" \
        $bootstrap_flag > "startup-${node_id}.log" 2>&1 &
    
    local pid=$!
    
    # Wait for server to be ready (check health endpoint)
    local ready=false
    local timeout_count=0
    
    while [ $timeout_count -lt $TEST_TIMEOUT ]; do
        if curl -s "http://localhost:$http_port/v1/status" >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 0.1
        ((timeout_count++))
    done
    
    # Record end time
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local duration_int=$(printf "%.0f" "$duration")
    
    # Clean up
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    
    if [ "$ready" = "true" ] && [ "$duration_int" -le "$TARGET_STARTUP_TIME" ]; then
        test_success "$test_name"
        test_info "Started in ${duration_int}s (target: ≤${TARGET_STARTUP_TIME}s)"
    else
        test_failure "$test_name"
        if [ "$ready" = "false" ]; then
            test_info "Server failed to become ready within ${TEST_TIMEOUT}s"
        else
            test_info "Started in ${duration_int}s (exceeds target: ${TARGET_STARTUP_TIME}s)"
        fi
    fi
    
    # Clean up test data
    rm -rf "test-startup-data-${node_id}/" "startup-${node_id}.log" 2>/dev/null || true
}

run_cold_start_test() {
    print_test "Cold start performance"
    ((TOTAL++))
    
    # Ensure completely clean state
    cleanup
    sleep 1
    
    # Measure cold start time
    local start_time=$(date +%s.%N)
    
    ../bin/server \
        --node-id="cold-start-node" \
        --http-addr=":8090" \
        --raft-addr="127.0.0.1:9090" \
        --data-dir="./test-cold-start" \
         > cold-start.log 2>&1 &
    
    local pid=$!
    
    # Wait for health endpoint
    local ready=false
    local attempts=0
    while [ $attempts -lt 50 ]; do
        if curl -s http://localhost:8090/v1/status >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 0.1
        ((attempts++))
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local duration_int=$(printf "%.0f" "$duration")
    
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    
    if [ "$ready" = "true" ] && [ "$duration_int" -le 5 ]; then
        test_success "Cold start performance"
        test_info "Cold start: ${duration_int}s"
    else
        test_failure "Cold start performance"
        test_info "Cold start took ${duration_int}s or failed"
    fi
    
    rm -rf test-cold-start/ cold-start.log 2>/dev/null || true
}

run_warm_restart_test() {
    print_test "Warm restart performance"
    ((TOTAL++))
    
    # First, create some data
    ../bin/server \
        --node-id="warm-node" \
        --http-addr=":8091" \
        --raft-addr="127.0.0.1:9091" \
        --data-dir="./test-warm-restart" \
         > warm-setup.log 2>&1 &
    
    local setup_pid=$!
    sleep 3
    
    # Add some data
    curl -s -X PUT http://localhost:8091/v1/keys/test-key -d "test-value" >/dev/null 2>&1 || true
    
    # Stop the server
    kill $setup_pid 2>/dev/null || true
    wait $setup_pid 2>/dev/null || true
    sleep 1
    
    # Now measure restart time
    local start_time=$(date +%s.%N)
    
    ../bin/server \
        --node-id="warm-node" \
        --http-addr=":8091" \
        --raft-addr="127.0.0.1:9091" \
        --data-dir="./test-warm-restart" > warm-restart.log 2>&1 &
    
    local pid=$!
    
    # Wait for health endpoint
    local ready=false
    local attempts=0
    while [ $attempts -lt 50 ]; do
        if curl -s http://localhost:8091/v1/status >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 0.1
        ((attempts++))
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local duration_int=$(printf "%.0f" "$duration")
    
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    
    if [ "$ready" = "true" ] && [ "$duration_int" -le 4 ]; then
        test_success "Warm restart performance"
        test_info "Restart with existing data: ${duration_int}s"
    else
        test_failure "Warm restart performance"
        test_info "Warm restart took ${duration_int}s or failed"
    fi
    
    rm -rf test-warm-restart/ warm-*.log 2>/dev/null || true
}

run_concurrent_startup_test() {
    print_test "Concurrent startup performance"
    ((TOTAL++))
    
    cleanup
    
    # Start 3 nodes concurrently
    local start_time=$(date +%s.%N)
    
    # Node 1 (bootstrap)
    mkdir -p test-concurrent1 test-concurrent2 test-concurrent3
    ../bin/server \
        --data-dir="./test-concurrent1" \
        --http-addr=":8092" \
        --node-id="concurrent1" \
        --enable-raft \
        --raft-addr="127.0.0.1:9092" > concurrent1.log 2>&1 &
    local pid1=$!
    
    sleep 3  # Let bootstrap node start and become leader
    
    # Nodes 2 and 3
    ../bin/server \
        --data-dir="./test-concurrent2" \
        --http-addr=":8093" \
        --node-id="concurrent2" \
        --enable-raft \
        --raft-addr="127.0.0.1:9093" \
        --join="http://127.0.0.1:8092" > concurrent2.log 2>&1 &
    local pid2=$!
    
    ../bin/server \
        --data-dir="./test-concurrent3" \
        --http-addr=":8094" \
        --node-id="concurrent3" \
        --enable-raft \
        --raft-addr="127.0.0.1:9094" \
        --join="http://127.0.0.1:8092" > concurrent3.log 2>&1 &
    local pid3=$!
    
    # Wait for all to be ready
    local all_ready=false
    local attempts=0
    while [ $attempts -lt 50 ]; do
        local ready_count=0
        curl -s http://localhost:8092/v1/status >/dev/null 2>&1 && ((ready_count++))
        curl -s http://localhost:8093/v1/status >/dev/null 2>&1 && ((ready_count++))
        curl -s http://localhost:8094/v1/status >/dev/null 2>&1 && ((ready_count++))
        
        if [ $ready_count -eq 3 ]; then
            all_ready=true
            break
        fi
        sleep 0.5
        ((attempts++))
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local duration_int=$(printf "%.0f" "$duration")
    
    # Cleanup
    kill $pid1 $pid2 $pid3 2>/dev/null || true
    wait $pid1 $pid2 $pid3 2>/dev/null || true
    
    if [ "$all_ready" = "true" ] && [ "$duration_int" -le 10 ]; then
        test_success "Concurrent startup performance"
        test_info "3-node cluster ready in ${duration_int}s"
    else
        test_failure "Concurrent startup performance"
        test_info "Concurrent startup took ${duration_int}s or failed"
    fi
    
    rm -rf test-concurrent*/ concurrent*.log 2>/dev/null || true
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Startup Performance Test Results${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total Tests:    ${BOLD}$TOTAL${NC}"
    echo -e "  ${GREEN}Passed:         $PASSED${NC}"
    echo -e "  ${RED}Failed:         $FAILED${NC}"
    
    local pass_rate=$((PASSED * 100 / TOTAL))
    echo -e "  Pass Rate:      ${BOLD}${pass_rate}%${NC}"
    echo ""
    
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL STARTUP TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper startup performance is optimal${NC}"
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review startup performance issues${NC}"
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
echo -e "${BOLD}Running startup performance tests...${NC}"
echo ""

# Individual startup tests
run_startup_test "Single node bootstrap" "test1" 8080 9080 "true"
run_startup_test "Node join existing cluster" "test2" 8081 9081 "false"

# Advanced startup tests
run_cold_start_test
run_warm_restart_test
run_concurrent_startup_test

print_summary
