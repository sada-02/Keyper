#!/bin/bash

# Keyper Port Reuse Capability Test
# Tests the ability to cleanly restart servers and reuse ports

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
RESTART_CYCLES=5

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
    echo -e "${CYAN}${BOLD}║              KEYPER PORT REUSE CAPABILITY TEST               ║${NC}"
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
    rm -rf test-port-reuse-*/ port-reuse-*.log 2>/dev/null || true
}

check_prerequisites() {
    if [ ! -f "../bin/server" ]; then
        echo -e "${RED}Error: bin/server not found. Please build the server first:${NC}"
        echo "  go build -o bin/server ./cmd/server"
        exit 1
    fi
}

test_basic_port_reuse() {
    print_test "Basic port reuse capability"
    ((TOTAL++))
    
    cleanup
    local success_cycles=0
    
    for cycle in $(seq 1 $RESTART_CYCLES); do
        # Start server
        ../bin/server \
            --node-id="port-test-$cycle" \
            --http-addr=":$BASE_HTTP_PORT" \
            --raft-addr="127.0.0.1:$BASE_RAFT_PORT" \
            --data-dir="./test-port-reuse-$cycle" \
             > "port-reuse-$cycle.log" 2>&1 &
        
        local pid=$!
        sleep 2
        
        # Check if server is responding
        local server_ready=false
        if curl -s "http://localhost:$BASE_HTTP_PORT/v1/status" >/dev/null 2>&1; then
            server_ready=true
        fi
        
        # Stop server
        kill $pid 2>/dev/null || true
        wait $pid 2>/dev/null || true
        
        # Wait for port to be freed
        sleep 2
        
        # Verify port is free
        local port_free=true
        if ss -tuln 2>/dev/null | grep -q ":$BASE_HTTP_PORT "; then
            port_free=false
        fi
        
        if [ "$server_ready" = "true" ] && [ "$port_free" = "true" ]; then
            ((success_cycles++))
        fi
        
        rm -rf "test-port-reuse-$cycle/" "port-reuse-$cycle.log"
    done
    
    if [ $success_cycles -eq $RESTART_CYCLES ]; then
        test_success "Basic port reuse capability"
        test_info "$success_cycles/$RESTART_CYCLES restart cycles successful"
    else
        test_failure "Basic port reuse capability"
        test_info "Only $success_cycles/$RESTART_CYCLES cycles successful"
    fi
}

test_rapid_port_reuse() {
    print_test "Rapid port reuse (minimal delay)"
    ((TOTAL++))
    
    cleanup
    local rapid_success=0
    local rapid_cycles=3
    
    for cycle in $(seq 1 $rapid_cycles); do
        # Start server
        ../bin/server \
            --node-id="rapid-test-$cycle" \
            --http-addr=":$((BASE_HTTP_PORT + 1))" \
            --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 1))" \
            --data-dir="./test-rapid-reuse-$cycle" \
             > "rapid-reuse-$cycle.log" 2>&1 &
        
        local pid=$!
        sleep 1  # Minimal startup time
        
        # Quick health check
        local healthy=false
        if curl -s "http://localhost:$((BASE_HTTP_PORT + 1))/v1/status" >/dev/null 2>&1; then
            healthy=true
        fi
        
        # Stop immediately
        kill $pid 2>/dev/null || true
        wait $pid 2>/dev/null || true
        
        # Very short delay before next cycle
        sleep 0.5
        
        if [ "$healthy" = "true" ]; then
            ((rapid_success++))
        fi
        
        rm -rf "test-rapid-reuse-$cycle/" "rapid-reuse-$cycle.log"
    done
    
    if [ $rapid_success -ge 2 ]; then
        test_success "Rapid port reuse (minimal delay)"
        test_info "$rapid_success/$rapid_cycles rapid cycles successful"
    else
        test_failure "Rapid port reuse (minimal delay)"
        test_info "Only $rapid_success/$rapid_cycles rapid cycles successful"
    fi
}

test_concurrent_port_binding() {
    print_test "Concurrent port binding handling"
    ((TOTAL++))
    
    cleanup
    
    # Start first server
    ../bin/server \
        --node-id="concurrent-1" \
        --http-addr=":$((BASE_HTTP_PORT + 2))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 2))" \
        --data-dir="./test-concurrent-1" \
         > "concurrent-1.log" 2>&1 &
    
    local pid1=$!
    sleep 2
    
    # Try to start second server on same ports (should fail gracefully)
    ../bin/server \
        --node-id="concurrent-2" \
        --http-addr=":$((BASE_HTTP_PORT + 2))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 2))" \
        --data-dir="./test-concurrent-2" \
         > "concurrent-2.log" 2>&1 &
    
    local pid2=$!
    sleep 3
    
    # Check which servers are actually running
    local server1_running=false
    local server2_running=false
    
    if kill -0 $pid1 2>/dev/null; then
        server1_running=true
    fi
    
    if kill -0 $pid2 2>/dev/null; then
        server2_running=true
    fi
    
    # Clean up
    kill $pid1 $pid2 2>/dev/null || true
    wait $pid1 $pid2 2>/dev/null || true
    
    # Only one server should be running
    if [ "$server1_running" = "true" ] && [ "$server2_running" = "false" ]; then
        test_success "Concurrent port binding handling"
        test_info "First server remained running, second failed gracefully"
    elif [ "$server1_running" = "false" ] && [ "$server2_running" = "true" ]; then
        test_success "Concurrent port binding handling" 
        test_info "Second server took over, first stopped gracefully"
    else
        test_failure "Concurrent port binding handling"
        test_info "Unexpected state: server1=$server1_running, server2=$server2_running"
    fi
    
    rm -rf test-concurrent-*/ concurrent-*.log
}

test_port_cleanup_after_crash() {
    print_test "Port cleanup after process termination"
    ((TOTAL++))
    
    cleanup
    
    # Start server
    ../bin/server \
        --node-id="crash-test" \
        --http-addr=":$((BASE_HTTP_PORT + 3))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 3))" \
        --data-dir="./test-crash-cleanup" \
         > "crash-test.log" 2>&1 &
    
    local pid=$!
    sleep 2
    
    # Verify server is running and port is bound
    local initially_bound=false
    if ss -tuln 2>/dev/null | grep -q ":$((BASE_HTTP_PORT + 3)) "; then
        initially_bound=true
    fi
    
    # Force kill the server (simulate crash)
    kill -9 $pid 2>/dev/null || true
    
    # Wait for OS to clean up
    sleep 3
    
    # Check if port is released
    local port_released=true
    if ss -tuln 2>/dev/null | grep -q ":$((BASE_HTTP_PORT + 3)) "; then
        port_released=false
    fi
    
    # Try to start new server on same port
    ../bin/server \
        --node-id="cleanup-test" \
        --http-addr=":$((BASE_HTTP_PORT + 3))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 3))" \
        --data-dir="./test-cleanup-new" \
         > "cleanup-new.log" 2>&1 &
    
    local new_pid=$!
    sleep 2
    
    local new_server_works=false
    if curl -s "http://localhost:$((BASE_HTTP_PORT + 3))/v1/status" >/dev/null 2>&1; then
        new_server_works=true
    fi
    
    kill $new_pid 2>/dev/null || true
    wait $new_pid 2>/dev/null || true
    
    if [ "$initially_bound" = "true" ] && [ "$port_released" = "true" ] && [ "$new_server_works" = "true" ]; then
        test_success "Port cleanup after process termination"
        test_info "Port released after crash, new server started successfully"
    else
        test_failure "Port cleanup after process termination"
        test_info "Issues with port cleanup: bound=$initially_bound, released=$port_released, new_works=$new_server_works"
    fi
    
    rm -rf test-crash-cleanup/ test-cleanup-new/ crash-test.log cleanup-new.log
}

test_multiple_port_ranges() {
    print_test "Multiple port range handling"
    ((TOTAL++))
    
    cleanup
    local pids=()
    local success_count=0
    local total_servers=3
    
    # Start servers on different port ranges
    for i in $(seq 1 $total_servers); do
        local http_port=$((BASE_HTTP_PORT + 10 + i))
        local raft_port=$((BASE_RAFT_PORT + 10 + i))
        
        ../bin/server \
            --node-id="multi-port-$i" \
            --http-addr=":$http_port" \
            --raft-addr="127.0.0.1:$raft_port" \
            --data-dir="./test-multi-port-$i" \
             > "multi-port-$i.log" 2>&1 &
        
        local pid=$!
        pids+=($pid)
        
        sleep 2
        
        # Check if this server is working
        if curl -s "http://localhost:$http_port/v1/status" >/dev/null 2>&1; then
            ((success_count++))
        fi
    done
    
    # Stop all servers
    for pid in "${pids[@]}"; do
        kill $pid 2>/dev/null || true
    done
    
    # Wait for cleanup
    sleep 2
    
    # Test restart capability for each port range
    local restart_success=0
    for i in $(seq 1 $total_servers); do
        local http_port=$((BASE_HTTP_PORT + 10 + i))
        local raft_port=$((BASE_RAFT_PORT + 10 + i))
        
        ../bin/server \
            --node-id="multi-restart-$i" \
            --http-addr=":$http_port" \
            --raft-addr="127.0.0.1:$raft_port" \
            --data-dir="./test-multi-restart-$i" \
             > "multi-restart-$i.log" 2>&1 &
        
        local restart_pid=$!
        sleep 2
        
        if curl -s "http://localhost:$http_port/v1/status" >/dev/null 2>&1; then
            ((restart_success++))
        fi
        
        kill $restart_pid 2>/dev/null || true
        wait $restart_pid 2>/dev/null || true
    done
    
    if [ $success_count -eq $total_servers ] && [ $restart_success -eq $total_servers ]; then
        test_success "Multiple port range handling"
        test_info "$success_count/$total_servers servers started, $restart_success/$total_servers restarted"
    else
        test_failure "Multiple port range handling"
        test_info "Initial: $success_count/$total_servers, Restart: $restart_success/$total_servers"
    fi
    
    rm -rf test-multi-*/ multi-*.log
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Port Reuse Test Results${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total Tests:    ${BOLD}$TOTAL${NC}"
    echo -e "  ${GREEN}Passed:         $PASSED${NC}"
    echo -e "  ${RED}Failed:         $FAILED${NC}"
    
    local pass_rate=$((PASSED * 100 / TOTAL))
    echo -e "  Pass Rate:      ${BOLD}${pass_rate}%${NC}"
    echo ""
    echo -e "  Test Config:    ${BOLD}${RESTART_CYCLES} restart cycles${NC}"
    echo ""
    
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL PORT REUSE TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper handles port management excellently${NC}"
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review port binding and cleanup mechanisms${NC}"
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
echo -e "${BOLD}Running port reuse capability tests...${NC}"
echo ""

test_basic_port_reuse
test_rapid_port_reuse
test_concurrent_port_binding
test_port_cleanup_after_crash
test_multiple_port_ranges

print_summary
