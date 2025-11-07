#!/bin/bash

# Keyper Long-term Stability Performance Test
# Tests system behavior under extended runtime conditions

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
STABILITY_DURATION=300  # 5 minutes
MONITORING_INTERVAL=10  # Check every 10 seconds
LOAD_INTERVAL=5         # Generate load every 5 seconds

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

# Monitoring arrays
declare -a TIMESTAMPS
declare -a MEMORY_SAMPLES
declare -a CPU_SAMPLES
declare -a REQUEST_COUNTS
declare -a ERROR_COUNTS

print_header() {
    echo ""
    echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}║           KEYPER LONG-TERM STABILITY TEST                    ║${NC}"
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
    pkill -9 -f "stability_client" 2>/dev/null || true
    sleep 2
    rm -rf test-stability-*/ stability-*.log stability-*.pid 2>/dev/null || true
}

check_prerequisites() {
    if [ ! -f "../bin/server" ]; then
        echo -e "${RED}Error: bin/server not found. Please build the server first:${NC}"
        echo "  go build -o bin/server ./cmd/server"
        exit 1
    fi
    
    echo -e "${BOLD}Stability test configuration:${NC}"
    echo -e "  Duration: ${STABILITY_DURATION}s ($(($STABILITY_DURATION/60)) minutes)"
    echo -e "  Monitoring interval: ${MONITORING_INTERVAL}s"
    echo -e "  Load generation: every ${LOAD_INTERVAL}s"
}

get_process_stats() {
    local pid=$1
    if [ -z "$pid" ] || ! kill -0 $pid 2>/dev/null; then
        echo "0 0"
        return
    fi
    
    # Get memory (RSS in KB) and CPU percentage
    local stats=$(ps -o rss=,pcpu= -p $pid 2>/dev/null | tr -s ' ' | sed 's/^ //')
    if [ -z "$stats" ]; then
        echo "0 0"
        return
    fi
    
    local memory_kb=$(echo "$stats" | cut -d' ' -f1)
    local cpu_percent=$(echo "$stats" | cut -d' ' -f2)
    
    # Validate we have values
    if [ -z "$memory_kb" ]; then
        memory_kb=0
    fi
    if [ -z "$cpu_percent" ] || [ "$cpu_percent" = "" ]; then
        cpu_percent=0
    fi
    
    local memory_mb=$((memory_kb / 1024))
    
    echo "$memory_mb $cpu_percent"
}

monitor_system() {
    local pids=("$@")
    local start_time=$(date +%s)
    local sample_count=0
    
    echo -e "${BOLD}Starting continuous monitoring (${STABILITY_DURATION}s)...${NC}"
    echo ""
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $STABILITY_DURATION ]; then
            break
        fi
        
        # Check if all processes are still running
        local running_count=0
        local total_memory=0
        local total_cpu=0
        
        for pid in "${pids[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                ((running_count++))
                local stats=$(get_process_stats $pid)
                local memory=$(echo $stats | cut -d' ' -f1)
                local cpu=$(echo $stats | cut -d' ' -f2)
                
                # Convert to integer to avoid floating point arithmetic errors
                local memory_int=$(printf "%.0f" "$memory" 2>/dev/null || echo "0")
                local cpu_int=$(printf "%.0f" "$cpu" 2>/dev/null || echo "0")
                
                total_memory=$((total_memory + memory_int))
                total_cpu=$((total_cpu + cpu_int))
            fi
        done
        
        # Record metrics
        TIMESTAMPS+=($elapsed)
        MEMORY_SAMPLES+=($total_memory)
        CPU_SAMPLES+=($total_cpu)
        ((sample_count++))
        
        # Progress indicator
        local progress=$((elapsed * 100 / STABILITY_DURATION))
        local remaining=$((STABILITY_DURATION - elapsed))
        echo -ne "\r${CYAN}Progress: ${progress}% | Elapsed: ${elapsed}s | Remaining: ${remaining}s | Nodes: $running_count | Memory: ${total_memory}MB${NC}"
        
        sleep $MONITORING_INTERVAL
    done
    
    echo ""
    echo -e "${GREEN}Monitoring completed (${sample_count} samples collected)${NC}"
}

generate_continuous_load() {
    local target_port=$1
    local duration=$2
    local client_id=$3
    
    local start_time=$(date +%s)
    local request_count=0
    local success_count=0
    local error_count=0
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $duration ]; then
            break
        fi
        
        # Generate different types of operations
        local operation=$((RANDOM % 10))
        local key="stability_key_${client_id}_${request_count}"
        local value="stability_value_${elapsed}"
        
        ((request_count++))
        
        if [ $operation -lt 6 ]; then
            # PUT operation (60%)
            if curl -s -X PUT "http://localhost:$target_port/v1/keys/$key" -d "$value" >/dev/null 2>&1; then
                ((success_count++))
            else
                ((error_count++))
            fi
        elif [ $operation -lt 9 ]; then
            # GET operation (30%)
            if curl -s "http://localhost:$target_port/v1/keys/$key" >/dev/null 2>&1; then
                ((success_count++))
            else
                ((error_count++))
            fi
        else
            # DELETE operation (10%)
            if curl -s -X DELETE "http://localhost:$target_port/v1/keys/$key" >/dev/null 2>&1; then
                ((success_count++))
            else
                ((error_count++))
            fi
        fi
        
        # Brief pause to avoid overwhelming
        sleep 0.1
    done
    
    echo "$request_count $success_count $error_count" > "stability_client_${client_id}_results.txt"
}

test_single_node_stability() {
    print_test "Single node stability (5 minutes)"
    ((TOTAL++))
    
    cleanup
    
    # Start single node
    ../bin/server \
        --node-id="stability-node1" \
        --http-addr=":$BASE_HTTP_PORT" \
        --raft-addr="127.0.0.1:$BASE_RAFT_PORT" \
        --data-dir="./test-stability-node1" \
         > "stability-node1.log" 2>&1 &
    
    local node_pid=$!
    echo $node_pid > "stability-node1.pid"
    
    sleep 5  # Let node initialize
    
    # Check if node started successfully
    if ! curl -s "http://localhost:$BASE_HTTP_PORT/v1/status" >/dev/null 2>&1; then
        kill $node_pid 2>/dev/null || true
        test_failure "Single node stability (5 minutes)"
        test_info "Node failed to start or become healthy"
        return
    fi
    
    # Start background load generator
    generate_continuous_load $BASE_HTTP_PORT $STABILITY_DURATION 1 &
    local load_pid=$!
    
    # Monitor the node
    monitor_system $node_pid
    
    # Stop load generator
    kill $load_pid 2>/dev/null || true
    wait $load_pid 2>/dev/null || true
    
    # Analyze results
    local final_stats=$(get_process_stats $node_pid)
    local final_memory=$(echo $final_stats | cut -d' ' -f1)
    local node_still_running=false
    
    if kill -0 $node_pid 2>/dev/null && curl -s "http://localhost:$BASE_HTTP_PORT/v1/status" >/dev/null 2>&1; then
        node_still_running=true
    fi
    
    # Get load test results
    local load_results=""
    if [ -f "stability_client_1_results.txt" ]; then
        load_results=$(cat "stability_client_1_results.txt")
    fi
    
    kill $node_pid 2>/dev/null || true
    
    if [ "$node_still_running" = "true" ] && [ ${#MEMORY_SAMPLES[@]} -gt 0 ]; then
        # Analyze memory growth
        local initial_memory=${MEMORY_SAMPLES[0]}
        local peak_memory=0
        for mem in "${MEMORY_SAMPLES[@]}"; do
            if [ $mem -gt $peak_memory ]; then
                peak_memory=$mem
            fi
        done
        
        local memory_growth=$((peak_memory - initial_memory))
        
        # Success criteria: node survives, memory growth <50MB
        if [ $memory_growth -lt 50 ]; then
            test_success "Single node stability (5 minutes)"
            test_info "Node stable for 5min, memory: ${initial_memory}MB → ${peak_memory}MB (+${memory_growth}MB)"
            if [ -n "$load_results" ]; then
                local total_req=$(echo $load_results | cut -d' ' -f1)
                local success_req=$(echo $load_results | cut -d' ' -f2)
                test_info "Load test: $success_req/$total_req requests successful"
            fi
        else
            test_failure "Single node stability (5 minutes)"
            test_info "Excessive memory growth: +${memory_growth}MB"
        fi
    else
        test_failure "Single node stability (5 minutes)"
        test_info "Node crashed or became unhealthy"
    fi
    
    rm -rf test-stability-node*/ stability-*.log stability-*.pid stability_client_*_results.txt
}

test_cluster_stability() {
    print_test "3-node cluster stability (5 minutes)"
    ((TOTAL++))
    
    cleanup
    local pids=()
    
    # Start 3-node cluster
    for i in {1..3}; do
        local http_port=$((BASE_HTTP_PORT + i - 1))
        local raft_port=$((BASE_RAFT_PORT + i - 1))
        local bootstrap_flag=""
        local join_flag=""
        
        if [ $i -eq 1 ]; then
            bootstrap_flag="--enable-raft"
        else
            join_flag="--enable-raft --join=http://127.0.0.1:$BASE_HTTP_PORT"
        fi
        
        ../bin/server \
            --node-id="stability-cluster-node$i" \
            --http-addr=":$http_port" \
            --raft-addr="127.0.0.1:$raft_port" \
            --data-dir="./test-stability-cluster-node$i" \
            $bootstrap_flag $join_flag > "stability-cluster-node$i.log" 2>&1 &
        
        local pid=$!
        pids+=($pid)
        echo $pid > "stability-cluster-node$i.pid"
        
        if [ $i -eq 1 ]; then
            sleep 5  # Let bootstrap node start
        else
            sleep 2
        fi
    done
    
    # Wait for cluster to be ready
    sleep 5
    
    # Check cluster health
    local healthy_nodes=0
    for i in {1..3}; do
        local port=$((BASE_HTTP_PORT + i - 1))
        if curl -s "http://localhost:$port/v1/status" >/dev/null 2>&1; then
            ((healthy_nodes++))
        fi
    done
    
    if [ $healthy_nodes -lt 3 ]; then
        kill "${pids[@]}" 2>/dev/null || true
        test_failure "3-node cluster stability (5 minutes)"
        test_info "Only $healthy_nodes/3 nodes became healthy"
        return
    fi
    
    # Start load generators for each node
    local load_pids=()
    for i in {1..3}; do
        local port=$((BASE_HTTP_PORT + i - 1))
        generate_continuous_load $port $STABILITY_DURATION $i &
        load_pids+=($!)
    done
    
    # Monitor cluster
    monitor_system "${pids[@]}"
    
    # Stop load generators
    kill "${load_pids[@]}" 2>/dev/null || true
    
    # Final health check
    local final_healthy_nodes=0
    local total_requests=0
    local total_success=0
    
    for i in {1..3}; do
        local port=$((BASE_HTTP_PORT + i - 1))
        if curl -s "http://localhost:$port/v1/status" >/dev/null 2>&1; then
            ((final_healthy_nodes++))
        fi
        
        # Aggregate load test results
        if [ -f "stability_client_${i}_results.txt" ]; then
            local results=$(cat "stability_client_${i}_results.txt")
            local req=$(echo $results | cut -d' ' -f1)
            local success=$(echo $results | cut -d' ' -f2)
            total_requests=$((total_requests + req))
            total_success=$((total_success + success))
        fi
    done
    
    kill "${pids[@]}" 2>/dev/null || true
    
    # Analyze cluster stability
    if [ $final_healthy_nodes -eq 3 ] && [ ${#MEMORY_SAMPLES[@]} -gt 0 ]; then
        local success_rate=0
        if [ $total_requests -gt 0 ]; then
            success_rate=$((total_success * 100 / total_requests))
        fi
        
        test_success "3-node cluster stability (5 minutes)"
        test_info "All 3 nodes remained healthy for 5 minutes"
        if [ $total_requests -gt 0 ]; then
            test_info "Load test: $total_success/$total_requests requests (${success_rate}% success)"
        fi
    else
        test_failure "3-node cluster stability (5 minutes)"
        test_info "Only $final_healthy_nodes/3 nodes remained healthy"
    fi
    
    rm -rf test-stability-cluster-node*/ stability-cluster-*.log stability-cluster-*.pid stability_client_*_results.txt
}

test_memory_leak_detection() {
    print_test "Memory leak detection (extended run)"
    ((TOTAL++))
    
    cleanup
    
    # Start node with moderate duration
    ../bin/server \
        --node-id="leak-test-node" \
        --http-addr=":$((BASE_HTTP_PORT + 10))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 10))" \
        --data-dir="./test-leak-node" \
         > "leak-test.log" 2>&1 &
    
    local pid=$!
    sleep 3
    
    # Get baseline memory
    local baseline_memory=$(get_process_stats $pid | cut -d' ' -f1)
    
    # Run cyclical operations for shorter duration
    local cycle_duration=60  # 1 minute cycles
    local num_cycles=3
    local memory_samples=()
    
    for cycle in $(seq 1 $num_cycles); do
        echo -ne "\r${CYAN}Memory leak test: cycle $cycle/$num_cycles${NC}"
        
        # Create load
        for i in {1..100}; do
            curl -s -X PUT "http://localhost:$((BASE_HTTP_PORT + 10))/v1/keys/leak_test_${cycle}_${i}" -d "test_data_$(date +%s)" >/dev/null 2>&1
        done
        
        # Delete some data
        for i in {1..50}; do
            curl -s -X DELETE "http://localhost:$((BASE_HTTP_PORT + 10))/v1/keys/leak_test_${cycle}_${i}" >/dev/null 2>&1
        done
        
        sleep 10  # Let system stabilize
        
        # Record memory
        local current_memory=$(get_process_stats $pid | cut -d' ' -f1)
        memory_samples+=($current_memory)
    done
    
    echo ""
    
    kill $pid 2>/dev/null || true
    
    # Analyze memory growth pattern
    local max_growth=0
    for memory in "${memory_samples[@]}"; do
        local growth=$((memory - baseline_memory))
        if [ $growth -gt $max_growth ]; then
            max_growth=$growth
        fi
    done
    
    # Allow up to 30MB growth for normal operations
    if [ $max_growth -le 30 ]; then
        test_success "Memory leak detection (extended run)"
        test_info "Memory stable: baseline ${baseline_memory}MB, max growth +${max_growth}MB"
    else
        test_failure "Memory leak detection (extended run)"
        test_info "Potential leak: growth +${max_growth}MB (threshold: 30MB)"
    fi
    
    rm -rf test-leak-node/ leak-test.log
}

test_restart_stability() {
    print_test "Restart stability and data persistence"
    ((TOTAL++))
    
    cleanup
    
    # Start node and add data
    ../bin/server \
        --node-id="restart-test" \
        --http-addr=":$((BASE_HTTP_PORT + 20))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 20))" \
        --data-dir="./test-restart-node" \
         > "restart-test1.log" 2>&1 &
    
    local pid=$!
    sleep 3
    
    # Add test data
    local test_keys=20
    local success_writes=0
    for i in $(seq 1 $test_keys); do
        if curl -s -X PUT "http://localhost:$((BASE_HTTP_PORT + 20))/v1/keys/restart_key_$i" -d "restart_value_$i" >/dev/null 2>&1; then
            ((success_writes++))
        fi
    done
    
    # Stop server
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    sleep 2
    
    # Restart server
    ../bin/server \
        --node-id="restart-test" \
        --http-addr=":$((BASE_HTTP_PORT + 20))" \
        --raft-addr="127.0.0.1:$((BASE_RAFT_PORT + 20))" \
        --data-dir="./test-restart-node" > "restart-test2.log" 2>&1 &
    
    pid=$!
    sleep 5  # Allow recovery
    
    # Check data persistence
    local success_reads=0
    for i in $(seq 1 $test_keys); do
        if curl -s "http://localhost:$((BASE_HTTP_PORT + 20))/v1/keys/restart_key_$i" | grep -q "restart_value_$i"; then
            ((success_reads++))
        fi
    done
    
    kill $pid 2>/dev/null || true
    
    local persistence_rate=$((success_reads * 100 / success_writes))
    
    if [ $persistence_rate -ge 95 ]; then
        test_success "Restart stability and data persistence"
        test_info "Data persistence: $success_reads/$success_writes keys recovered (${persistence_rate}%)"
    else
        test_failure "Restart stability and data persistence"
        test_info "Poor persistence: $success_reads/$success_writes keys (${persistence_rate}%)"
    fi
    
    rm -rf test-restart-node/ restart-test*.log
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Long-term Stability Test Results${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total Tests:    ${BOLD}$TOTAL${NC}"
    echo -e "  ${GREEN}Passed:         $PASSED${NC}"
    echo -e "  ${RED}Failed:         $FAILED${NC}"
    
    local pass_rate=$((PASSED * 100 / TOTAL))
    echo -e "  Pass Rate:      ${BOLD}${pass_rate}%${NC}"
    echo ""
    echo -e "  Test Duration:  ${BOLD}$(($STABILITY_DURATION/60)) minutes per test${NC}"
    
    if [ ${#MEMORY_SAMPLES[@]} -gt 0 ]; then
        echo ""
        echo -e "${BOLD}Memory Analysis (last test):${NC}"
        local min_mem=999999
        local max_mem=0
        local total_mem=0
        
        for mem in "${MEMORY_SAMPLES[@]}"; do
            if [ $mem -lt $min_mem ]; then min_mem=$mem; fi
            if [ $mem -gt $max_mem ]; then max_mem=$mem; fi
            total_mem=$((total_mem + mem))
        done
        
        local avg_mem=$((total_mem / ${#MEMORY_SAMPLES[@]}))
        local mem_variance=$((max_mem - min_mem))
        
        echo -e "  Memory range:   ${BOLD}${min_mem}MB - ${max_mem}MB${NC} (variance: ${mem_variance}MB)"
        echo -e "  Average usage:  ${BOLD}${avg_mem}MB${NC}"
    fi
    
    echo ""
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL STABILITY TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper demonstrates excellent long-term stability${NC}"
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review stability issues and resource management${NC}"
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
echo -e "${BOLD}Running long-term stability tests...${NC}"
echo -e "${YELLOW}Note: These tests take longer to complete${NC}"
echo ""

test_single_node_stability
test_cluster_stability
test_memory_leak_detection
test_restart_stability

print_summary
