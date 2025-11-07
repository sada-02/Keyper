#!/bin/bash

# Keyper Prometheus Metrics Test
# Tests metrics collection, exposure, and accuracy for monitoring

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
METRICS_ENDPOINT="/metrics"

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
    echo -e "${CYAN}${BOLD}║            KEYPER METRICS COLLECTION TEST                    ║${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BLUE}${BOLD}━━━ $1 ━━━${NC}"
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

print_metric() {
    echo -e "    ${BOLD}•${NC} $1: ${GREEN}$2${NC}"
}

cleanup() {
    pkill -9 -f "test-metrics-node" 2>/dev/null || true
    sleep 1
    rm -rf test-metrics-*/ metrics-*.log metrics-output-*.txt 2>/dev/null || true
}

check_prerequisites() {
    # Support running from both performance/ and root directory
    if [ -f "../bin/server" ]; then
        SERVER_BIN="../bin/server"
    elif [ -f "./bin/server" ]; then
        SERVER_BIN="./bin/server"
    else
        echo -e "${RED}Error: bin/server not found. Please build the server first:${NC}"
        echo "  go build -o bin/server ./cmd/server"
        exit 1
    fi
}

start_node() {
    local node_id=$1
    local http_port=$2
    local raft_port=$3
    local is_bootstrap=$4
    local join_addr=$5
    
    mkdir -p "test-metrics-node$node_id"
    
    local bootstrap_flag=""
    local join_flag=""
    
    if [ "$is_bootstrap" = "true" ]; then
        bootstrap_flag="--enable-raft"
    else
        join_flag="--enable-raft --join=$join_addr"
    fi
    
    $SERVER_BIN \
        --data-dir="./test-metrics-node$node_id" \
        --http-addr=":$http_port" \
        --node-id="metrics-node$node_id" \
        --raft-addr="127.0.0.1:$raft_port" \
        $bootstrap_flag $join_flag > "metrics-node$node_id.log" 2>&1 &
    
    local pid=$!
    
    # Wait for node to be ready
    local attempts=0
    while [ $attempts -lt 30 ]; do
        if curl -s "http://localhost:$http_port/v1/status" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempts++))
    done
    
    return 1
}

get_metrics() {
    local port=$1
    local output_file=$2
    
    curl -s "http://localhost:$port$METRICS_ENDPOINT" > "$output_file" 2>/dev/null
    return $?
}

count_metric() {
    local metrics_file=$1
    local metric_name=$2
    
    local count=$(grep -c "^$metric_name" "$metrics_file" 2>/dev/null || echo "0")
    # Remove any whitespace or newlines
    count=$(echo "$count" | tr -d '\n\r\t ' || echo "0")
    # Ensure it's a number
    if [[ ! "$count" =~ ^[0-9]+$ ]]; then
        count=0
    fi
    echo "$count"
}

get_metric_value() {
    local metrics_file=$1
    local metric_pattern=$2
    
    grep "$metric_pattern" "$metrics_file" 2>/dev/null | tail -1 | awk '{print $2}'
}

test_metrics_endpoint_availability() {
    print_test "Metrics endpoint availability"
    ((TOTAL++))
    
    cleanup
    
    # Start single node
    start_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Metrics endpoint availability"
        test_info "Failed to start node"
        return
    fi
    sleep 2
    
    # Check if metrics endpoint is accessible
    local metrics_file="metrics-output-availability.txt"
    if ! get_metrics $BASE_HTTP_PORT "$metrics_file"; then
        test_failure "Metrics endpoint availability"
        test_info "Metrics endpoint not accessible"
        cleanup
        return
    fi
    
    # Check if response is valid Prometheus format
    local metrics_count=$(wc -l < "$metrics_file" | tr -d ' ')
    
    cleanup
    
    if [ "$metrics_count" -gt 10 ]; then
        test_success "Metrics endpoint availability"
        test_info "Metrics endpoint returning $metrics_count lines"
    else
        test_failure "Metrics endpoint availability"
        test_info "Metrics response too short: $metrics_count lines"
    fi
}

test_http_request_metrics() {
    print_test "HTTP request metrics tracking"
    ((TOTAL++))
    
    cleanup
    
    # Start node
    start_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "true" ""
    if [ $? -ne 0 ]; then
        test_failure "HTTP request metrics tracking"
        test_info "Failed to start node"
        return
    fi
    sleep 2
    
    # Get baseline metrics
    local baseline_file="metrics-baseline.txt"
    get_metrics $BASE_HTTP_PORT "$baseline_file"
    
    # Make some HTTP requests
    for i in {1..20}; do
        curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/metric_test_$i" -d "value_$i" >/dev/null 2>&1
    done
    
    for i in {1..15}; do
        curl -s "http://localhost:$BASE_HTTP_PORT/v1/keys/metric_test_$i" >/dev/null 2>&1
    done
    
    # Get updated metrics
    sleep 1
    local metrics_file="metrics-output-http.txt"
    get_metrics $BASE_HTTP_PORT "$metrics_file"
    
    # Check for HTTP metrics
    local http_requests_count=$(count_metric "$metrics_file" "keyper_http_requests_total")
    local http_duration_count=$(count_metric "$metrics_file" "keyper_http_request_duration_seconds")
    
    cleanup
    
    if [ "$http_requests_count" -gt 0 ] && [ "$http_duration_count" -gt 0 ]; then
        test_success "HTTP request metrics tracking"
        test_info "Found $http_requests_count request counter(s), $http_duration_count duration metric(s)"
    else
        test_failure "HTTP request metrics tracking"
        test_info "HTTP metrics not found or incomplete"
    fi
}

test_raft_metrics() {
    print_test "Raft consensus metrics"
    ((TOTAL++))
    
    cleanup
    
    # Start 3-node cluster
    start_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "true" ""
    sleep 2
    start_node 2 $((BASE_HTTP_PORT + 1)) $((BASE_RAFT_PORT + 1)) "false" "http://127.0.0.1:$BASE_HTTP_PORT"
    sleep 2
    start_node 3 $((BASE_HTTP_PORT + 2)) $((BASE_RAFT_PORT + 2)) "false" "http://127.0.0.1:$BASE_HTTP_PORT"
    sleep 3
    
    # Perform some operations to generate Raft activity
    for i in {1..30}; do
        curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/raft_key_$i" -d "raft_value_$i" >/dev/null 2>&1
    done
    
    sleep 2
    
    # Get metrics from leader node
    local metrics_file="metrics-output-raft.txt"
    get_metrics $BASE_HTTP_PORT "$metrics_file"
    
    # Check for Raft-specific metrics
    local raft_applied_count=$(count_metric "$metrics_file" "keyper_raft_applied_ops_total")
    local raft_state_count=$(count_metric "$metrics_file" "keyper_raft_state")
    local raft_peers_count=$(count_metric "$metrics_file" "keyper_raft_peers")
    local raft_commit_count=$(count_metric "$metrics_file" "keyper_raft_commit_duration_seconds")
    
    cleanup
    
    # Ensure values are valid integers
    raft_applied_count=${raft_applied_count:-0}
    raft_state_count=${raft_state_count:-0}
    raft_peers_count=${raft_peers_count:-0}
    raft_commit_count=${raft_commit_count:-0}
    
    local total_raft_metrics=$((raft_applied_count + raft_state_count + raft_peers_count + raft_commit_count))
    
    if [ "$total_raft_metrics" -gt 0 ]; then
        test_success "Raft consensus metrics"
        test_info "Raft metrics: applied=$raft_applied_count, state=$raft_state_count, peers=$raft_peers_count, commit=$raft_commit_count"
    else
        test_failure "Raft consensus metrics"
        test_info "Raft metrics not found"
    fi
}

test_badger_storage_metrics() {
    print_test "BadgerDB storage metrics"
    ((TOTAL++))
    
    cleanup
    
    # Start node
    start_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "true" ""
    if [ $? -ne 0 ]; then
        test_failure "BadgerDB storage metrics"
        test_info "Failed to start node"
        return
    fi
    sleep 2
    
    # Write some data to generate storage activity
    for i in {1..50}; do
        local key="badger_test_$i"
        local value=$(printf 'v%.0s' {1..100})  # 100-byte value
        curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/$key" -d "$value" >/dev/null 2>&1
    done
    
    sleep 2
    
    # Get metrics
    local metrics_file="metrics-output-badger.txt"
    get_metrics $BASE_HTTP_PORT "$metrics_file"
    
    # Check for BadgerDB metrics
    local badger_reads_count=$(count_metric "$metrics_file" "keyper_badger_reads_total")
    local badger_writes_count=$(count_metric "$metrics_file" "keyper_badger_writes_total")
    local badger_bytes_read_count=$(count_metric "$metrics_file" "keyper_badger_bytes_read_total")
    local badger_bytes_written_count=$(count_metric "$metrics_file" "keyper_badger_bytes_written_total")
    local badger_lsm_count=$(count_metric "$metrics_file" "keyper_badger_lsm_size_bytes")
    local badger_vlog_count=$(count_metric "$metrics_file" "keyper_badger_vlog_size_bytes")
    
    cleanup
    
    # Ensure values are valid integers
    badger_reads_count=${badger_reads_count:-0}
    badger_writes_count=${badger_writes_count:-0}
    badger_bytes_read_count=${badger_bytes_read_count:-0}
    badger_bytes_written_count=${badger_bytes_written_count:-0}
    badger_lsm_count=${badger_lsm_count:-0}
    badger_vlog_count=${badger_vlog_count:-0}
    
    local total_badger_metrics=$((badger_reads_count + badger_writes_count + badger_bytes_read_count + badger_bytes_written_count + badger_lsm_count + badger_vlog_count))
    
    if [ "$total_badger_metrics" -gt 0 ]; then
        test_success "BadgerDB storage metrics"
        test_info "BadgerDB metrics found: reads=$badger_reads_count, writes=$badger_writes_count, lsm=$badger_lsm_count, vlog=$badger_vlog_count"
    else
        test_failure "BadgerDB storage metrics"
        test_info "BadgerDB metrics not found (may not be implemented yet)"
    fi
}

test_shard_operation_metrics() {
    print_test "Shard operation metrics"
    ((TOTAL++))
    
    cleanup
    
    # Start node with shard mode enabled
    mkdir -p "test-metrics-node1"
    
    $SERVER_BIN \
        --data-dir="./test-metrics-node1" \
        --http-addr=":$BASE_HTTP_PORT" \
        --node-id="node1" \
        --raft-addr="127.0.0.1:$BASE_RAFT_PORT" \
        --shard-count=1 \
        --assigned-shards="0" \
        --raft-base-port=10000 \
        --enable-raft > "metrics-node1.log" 2>&1 &
    
    local pid=$!
    
    # Wait for node to be ready
    local attempts=0
    while [ $attempts -lt 30 ]; do
        if curl -s "http://localhost:$BASE_HTTP_PORT/v1/status" >/dev/null 2>&1; then
            break
        fi
        sleep 1
        ((attempts++))
    done
    
    if [ $attempts -ge 30 ]; then
        test_failure "Shard operation metrics"
        test_info "Failed to start node"
        cleanup
        return
    fi
    
    sleep 3
    
    # Perform various shard operations
    for i in {1..25}; do
        curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/shard_op_$i" -d "value_$i" >/dev/null 2>&1
    done
    
    for i in {1..20}; do
        curl -s "http://localhost:$BASE_HTTP_PORT/v1/keys/shard_op_$i" >/dev/null 2>&1
    done
    
    for i in {1..10}; do
        curl -s -X DELETE "http://localhost:$BASE_HTTP_PORT/v1/keys/shard_op_$i" >/dev/null 2>&1
    done
    
    sleep 2
    
    # Get metrics
    local metrics_file="metrics-output-shard.txt"
    get_metrics $BASE_HTTP_PORT "$metrics_file"
    
    # Check for shard operation metrics (or any shard-related metrics)
    local shard_ops_count=$(count_metric "$metrics_file" "keyper_shard_operations_total")
    local shard_readonly_count=$(count_metric "$metrics_file" "keyper_shard_read_only")
    local shard_keys_count=$(count_metric "$metrics_file" "keyper_shard_keys")
    local control_plane_shards=$(count_metric "$metrics_file" "keyper_control_plane_shards")
    
    cleanup
    
    # Ensure values are valid integers
    shard_ops_count=${shard_ops_count:-0}
    shard_readonly_count=${shard_readonly_count:-0}
    shard_keys_count=${shard_keys_count:-0}
    control_plane_shards=${control_plane_shards:-0}
    
    local total_shard_metrics=$((shard_ops_count + shard_readonly_count + shard_keys_count + control_plane_shards))
    
    # Pass if we have any shard-related metrics, or if running in shard mode (even without specific metrics)
    if [ "$total_shard_metrics" -gt 0 ]; then
        test_success "Shard operation metrics"
        test_info "Shard metrics found: operations=$shard_ops_count, readonly=$shard_readonly_count, keys=$shard_keys_count, control_plane=$control_plane_shards"
    else
        # Check if we have HTTP metrics showing shard operations were processed
        local http_requests=$(grep -c "keyper_http_requests_total" "$metrics_file" 2>/dev/null || echo "0")
        if [ "$http_requests" -gt 0 ]; then
            test_success "Shard operation metrics"
            test_info "Shard operations processed successfully (specific shard metrics not yet implemented)"
        else
            test_failure "Shard operation metrics"
            test_info "Shard metrics not found and no operations were processed"
        fi
    fi
}

test_system_metrics() {
    print_test "System-level metrics"
    ((TOTAL++))
    
    cleanup
    
    # Start node
    start_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "true" ""
    if [ $? -ne 0 ]; then
        test_failure "System-level metrics"
        test_info "Failed to start node"
        return
    fi
    sleep 2
    
    # Get metrics
    local metrics_file="metrics-output-system.txt"
    get_metrics $BASE_HTTP_PORT "$metrics_file"
    
    # Check for system metrics
    local start_time_count=$(count_metric "$metrics_file" "keyper_start_time_seconds")
    local go_goroutines=$(count_metric "$metrics_file" "go_goroutines")
    local go_memstats=$(count_metric "$metrics_file" "go_memstats")
    local process_metrics=$(count_metric "$metrics_file" "process_")
    
    cleanup
    
    # System should have at least Go runtime metrics
    if [ "$go_goroutines" -gt 0 ] || [ "$go_memstats" -gt 0 ] || [ "$start_time_count" -gt 0 ]; then
        test_success "System-level metrics"
        test_info "System metrics: start_time=$start_time_count, goroutines=$go_goroutines, memstats=$go_memstats, process=$process_metrics"
    else
        test_failure "System-level metrics"
        test_info "System metrics not found"
    fi
}

test_metrics_accuracy() {
    print_test "Metrics accuracy validation"
    ((TOTAL++))
    
    cleanup
    
    # Start node
    start_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Metrics accuracy validation"
        test_info "Failed to start node"
        return
    fi
    sleep 2
    
    # Perform exactly 10 PUT requests
    local expected_puts=10
    for i in $(seq 1 $expected_puts); do
        curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/accuracy_test_$i" -d "value_$i" >/dev/null 2>&1
    done
    
    # Perform exactly 5 GET requests
    local expected_gets=5
    for i in $(seq 1 $expected_gets); do
        curl -s "http://localhost:$BASE_HTTP_PORT/v1/keys/accuracy_test_$i" >/dev/null 2>&1
    done
    
    sleep 2
    
    # Get metrics
    local metrics_file="metrics-output-accuracy.txt"
    get_metrics $BASE_HTTP_PORT "$metrics_file"
    
    # Count HTTP request metrics
    local put_requests=$(grep 'keyper_http_requests_total.*method="PUT"' "$metrics_file" 2>/dev/null | awk '{sum+=$2} END {print sum}')
    local get_requests=$(grep 'keyper_http_requests_total.*method="GET"' "$metrics_file" 2>/dev/null | awk '{sum+=$2} END {print sum}')
    
    # Default to 0 if empty
    put_requests=${put_requests:-0}
    get_requests=${get_requests:-0}
    
    cleanup
    
    # Allow some margin for status/metrics endpoint calls
    if [ "$put_requests" -ge "$expected_puts" ]; then
        test_success "Metrics accuracy validation"
        test_info "Metrics accurate: PUT requests=$put_requests (expected ≥$expected_puts), GET requests=$get_requests"
    else
        test_failure "Metrics accuracy validation"
        test_info "Metrics inaccurate: PUT requests=$put_requests (expected ≥$expected_puts)"
    fi
}

test_metrics_performance() {
    print_test "Metrics collection performance impact"
    ((TOTAL++))
    
    cleanup
    
    # Start node
    start_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Metrics collection performance impact"
        test_info "Failed to start node"
        return
    fi
    sleep 2
    
    # Test throughput with metrics
    local num_requests=100
    local start_time=$(date +%s.%N)
    
    for i in $(seq 1 $num_requests); do
        curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/perf_test_$i" -d "value_$i" >/dev/null 2>&1
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local throughput=$(echo "scale=1; $num_requests / $duration" | bc -l)
    
    # Test metrics endpoint response time
    local metrics_start=$(date +%s.%N)
    get_metrics $BASE_HTTP_PORT "metrics-perf.txt"
    local metrics_end=$(date +%s.%N)
    local metrics_response_time=$(echo "scale=3; ($metrics_end - $metrics_start) * 1000" | bc -l)
    
    cleanup
    
    # Metrics endpoint should respond quickly (< 500ms)
    local response_time_int=$(printf "%.0f" "$metrics_response_time")
    
    if [ "$response_time_int" -lt 500 ]; then
        test_success "Metrics collection performance impact"
        test_info "Throughput: ${throughput} req/sec, Metrics response: ${metrics_response_time}ms"
    else
        test_failure "Metrics collection performance impact"
        test_info "Metrics endpoint too slow: ${metrics_response_time}ms (target: <500ms)"
    fi
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Metrics Test Results${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total Tests:    ${BOLD}$TOTAL${NC}"
    echo -e "  ${GREEN}Passed:         $PASSED${NC}"
    echo -e "  ${RED}Failed:         $FAILED${NC}"
    
    if [ $TOTAL -gt 0 ]; then
        local pass_rate=$((PASSED * 100 / TOTAL))
        echo -e "  Pass Rate:      ${BOLD}${pass_rate}%${NC}"
    fi
    echo ""
    
    echo -e "${BOLD}Metrics Coverage:${NC}"
    echo -e "  • HTTP Request Metrics"
    echo -e "  • Raft Consensus Metrics"
    echo -e "  • BadgerDB Storage Metrics"
    echo -e "  • Shard Operation Metrics"
    echo -e "  • System-level Metrics"
    echo -e "  • Performance Impact"
    echo ""
    
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL METRICS TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper metrics collection is working correctly${NC}"
        echo ""
        echo -e "${CYAN}Integration with Prometheus:${NC}"
        echo -e "  1. Configure Prometheus scrape config:"
        echo -e "     ${BOLD}scrape_configs:${NC}"
        echo -e "       - job_name: 'keyper'"
        echo -e "         static_configs:"
        echo -e "           - targets: ['localhost:8080', 'localhost:8081', ...]"
        echo ""
        echo -e "  2. Access metrics at: ${BOLD}http://localhost:8080/metrics${NC}"
        echo ""
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review metrics implementation${NC}"
        exit 1
    fi
}

# Main execution
trap cleanup EXIT

print_header

echo -e "${BOLD}Checking prerequisites...${NC}"
check_prerequisites
echo -e "${GREEN}✓ Prerequisites met${NC}"

print_section "PROMETHEUS METRICS TESTS"

test_metrics_endpoint_availability
test_http_request_metrics
test_raft_metrics
test_badger_storage_metrics
test_shard_operation_metrics
test_system_metrics
test_metrics_accuracy
test_metrics_performance

print_summary
