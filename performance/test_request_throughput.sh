#!/bin/bash

# Keyper Request Throughput Performance Test
# Tests HTTP request processing rate and response times

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
TEST_DURATION=30
CONCURRENT_CLIENTS=5
REQUESTS_PER_CLIENT=100

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
    echo -e "${CYAN}${BOLD}║            KEYPER REQUEST THROUGHPUT TEST                    ║${NC}"
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
    rm -rf test-throughput-*/ throughput-*.log throughput-results-*.txt 2>/dev/null || true
}

check_prerequisites() {
    if [ ! -f "../bin/server" ]; then
        echo -e "${RED}Error: bin/server not found. Please build the server first:${NC}"
        echo "  go build -o bin/server ./cmd/server"
        exit 1
    fi
    
    # Check if curl supports timing
    if ! curl --version | grep -q "curl"; then
        echo -e "${RED}Error: curl is required for throughput testing${NC}"
        exit 1
    fi
}

setup_cluster() {
    local node_count=$1
    local pids=()
    
    # Start nodes
    for i in $(seq 1 $node_count); do
        local http_port=$((BASE_HTTP_PORT + i - 1))
        local raft_port=$((BASE_RAFT_PORT + i - 1))
        local bootstrap_flag=""
        local join_flag=""
        
        mkdir -p "test-throughput-node$i"
        if [ $i -eq 1 ]; then
            bootstrap_flag="--enable-raft"
            join_flag=""
        else
            bootstrap_flag=""
            join_flag="--enable-raft --join=http://127.0.0.1:$BASE_HTTP_PORT"
        fi
        
        ../bin/server \
            --data-dir="./test-throughput-node$i" \
            --http-addr=":$http_port" \
            --node-id="throughput-node$i" \
            --raft-addr="127.0.0.1:$raft_port" \
            $bootstrap_flag $join_flag > "throughput-node$i.log" 2>&1 &
        
        local pid=$!
        pids+=($pid)
        
        if [ $i -eq 1 ]; then
            sleep 3  # Let bootstrap node start
        else
            sleep 1
        fi
    done
    
    # Wait for all nodes to be ready
    for i in $(seq 1 $node_count); do
        local port=$((BASE_HTTP_PORT + i - 1))
        local attempts=0
        while [ $attempts -lt 30 ]; do
            if curl -s "http://localhost:$port/v1/status" >/dev/null 2>&1; then
                break
            fi
            sleep 1
            ((attempts++))
        done
    done
    
    echo "${pids[@]}"
}

run_throughput_client() {
    local client_id=$1
    local target_port=$2
    local num_requests=$3
    local operation_type=$4
    local result_file=$5
    
    local success_count=0
    local total_time=0
    local min_time=999999
    local max_time=0
    
    for i in $(seq 1 $num_requests); do
        local key="client${client_id}_key${i}"
        local value="client${client_id}_value${i}_$(date +%s%N)"
        
        local start_time=$(date +%s%N)
        
        case $operation_type in
            "put")
                if curl -s -X PUT "http://localhost:$target_port/v1/keys/$key" -d "$value" >/dev/null 2>&1; then
                    ((success_count++))
                fi
                ;;
            "get")
                if curl -s "http://localhost:$target_port/v1/keys/$key" >/dev/null 2>&1; then
                    ((success_count++))
                fi
                ;;
            "delete")
                if curl -s -X DELETE "http://localhost:$target_port/v1/keys/$key" >/dev/null 2>&1; then
                    ((success_count++))
                fi
                ;;
        esac
        
        local end_time=$(date +%s%N)
        local request_time=$((end_time - start_time))
        total_time=$((total_time + request_time))
        
        if [ $request_time -lt $min_time ]; then
            min_time=$request_time
        fi
        
        if [ $request_time -gt $max_time ]; then
            max_time=$request_time
        fi
    done
    
    local avg_time=$((total_time / num_requests))
    local avg_time_ms=$((avg_time / 1000000))
    local min_time_ms=$((min_time / 1000000))
    local max_time_ms=$((max_time / 1000000))
    
    echo "$client_id $success_count $num_requests $avg_time_ms $min_time_ms $max_time_ms" > "$result_file"
}

test_sequential_throughput() {
    print_test "Sequential request throughput"
    ((TOTAL++))
    
    cleanup
    local pids=($(setup_cluster 1))
    
    if [ ${#pids[@]} -eq 0 ]; then
        test_failure "Sequential request throughput"
        test_info "Failed to start cluster"
        return
    fi
    
    local port=$BASE_HTTP_PORT
    local total_requests=500
    local start_time=$(date +%s.%N)
    local success_count=0
    
    # Sequential PUT requests
    for i in $(seq 1 $total_requests); do
        if curl -s -X PUT "http://localhost:$port/v1/keys/seq_key_$i" -d "seq_value_$i" >/dev/null 2>&1; then
            ((success_count++))
        fi
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local throughput=$(echo "scale=1; $success_count / $duration" | bc -l)
    
    kill ${pids[@]} 2>/dev/null || true
    
    # Target: >100 req/sec for sequential requests
    local throughput_int=$(printf "%.0f" "$throughput")
    if [ $throughput_int -ge 100 ] && [ $success_count -eq $total_requests ]; then
        test_success "Sequential request throughput"
        test_info "$success_count/$total_requests requests, ${throughput} req/sec"
    else
        test_failure "Sequential request throughput"
        test_info "$success_count/$total_requests requests, ${throughput} req/sec (target: ≥100)"
    fi
    
    rm -rf test-throughput-node*/ throughput-node*.log
}

test_concurrent_throughput() {
    print_test "Concurrent request throughput"
    ((TOTAL++))
    
    cleanup
    local pids=($(setup_cluster 1))
    
    if [ ${#pids[@]} -eq 0 ]; then
        test_failure "Concurrent request throughput"
        test_info "Failed to start cluster"
        return
    fi
    
    local port=$BASE_HTTP_PORT
    local start_time=$(date +%s.%N)
    
    # Start concurrent clients
    local client_pids=()
    for client in $(seq 1 $CONCURRENT_CLIENTS); do
        run_throughput_client $client $port $REQUESTS_PER_CLIENT "put" "throughput-results-$client.txt" &
        client_pids+=($!)
    done
    
    # Wait for all clients to complete
    wait "${client_pids[@]}"
    
    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc -l)
    
    # Aggregate results
    local total_success=0
    local total_requests=0
    local total_avg_latency=0
    local min_latency=999999
    local max_latency=0
    
    for client in $(seq 1 $CONCURRENT_CLIENTS); do
        local result_file="throughput-results-$client.txt"
        if [ -f "$result_file" ]; then
            local client_data=$(cat "$result_file")
            local success=$(echo $client_data | cut -d' ' -f2)
            local requests=$(echo $client_data | cut -d' ' -f3)
            local avg_lat=$(echo $client_data | cut -d' ' -f4)
            local min_lat=$(echo $client_data | cut -d' ' -f5)
            local max_lat=$(echo $client_data | cut -d' ' -f6)
            
            total_success=$((total_success + success))
            total_requests=$((total_requests + requests))
            total_avg_latency=$((total_avg_latency + avg_lat))
            
            if [ $min_lat -lt $min_latency ]; then
                min_latency=$min_lat
            fi
            
            if [ $max_lat -gt $max_latency ]; then
                max_latency=$max_lat
            fi
        fi
    done
    
    local avg_latency=$((total_avg_latency / CONCURRENT_CLIENTS))
    local throughput=$(echo "scale=1; $total_success / $total_duration" | bc -l)
    local success_rate=$((total_success * 100 / total_requests))
    
    kill ${pids[@]} 2>/dev/null || true
    
    # Target: >200 req/sec with >95% success rate
    local throughput_int=$(printf "%.0f" "$throughput")
    if [ $throughput_int -ge 200 ] && [ $success_rate -ge 95 ]; then
        test_success "Concurrent request throughput"
        test_info "${throughput} req/sec, ${success_rate}% success, ${avg_latency}ms avg latency"
    else
        test_failure "Concurrent request throughput"
        test_info "${throughput} req/sec, ${success_rate}% success (target: ≥200 req/sec, ≥95%)"
    fi
    
    rm -rf test-throughput-node*/ throughput-node*.log throughput-results-*.txt
}

test_mixed_workload() {
    print_test "Mixed workload performance"
    ((TOTAL++))
    
    cleanup
    local pids=($(setup_cluster 1))
    
    if [ ${#pids[@]} -eq 0 ]; then
        test_failure "Mixed workload performance"
        test_info "Failed to start cluster"
        return
    fi
    
    local port=$BASE_HTTP_PORT
    
    # Populate some initial data
    for i in {1..50}; do
        curl -s -X PUT "http://localhost:$port/v1/keys/mixed_key_$i" -d "mixed_value_$i" >/dev/null 2>&1
    done
    
    local start_time=$(date +%s.%N)
    local puts=0
    local gets=0
    local deletes=0
    local put_success=0
    local get_success=0
    local delete_success=0
    
    # Run mixed workload: 50% gets, 30% puts, 20% deletes
    for i in {1..300}; do
        local operation=$((RANDOM % 10))
        local key_id=$((RANDOM % 100 + 1))
        
        if [ $operation -lt 5 ]; then
            # GET operation (50%)
            ((gets++))
            if curl -s "http://localhost:$port/v1/keys/mixed_key_$key_id" >/dev/null 2>&1; then
                ((get_success++))
            fi
        elif [ $operation -lt 8 ]; then
            # PUT operation (30%)
            ((puts++))
            if curl -s -X PUT "http://localhost:$port/v1/keys/mixed_key_$key_id" -d "updated_value_$i" >/dev/null 2>&1; then
                ((put_success++))
            fi
        else
            # DELETE operation (20%)
            ((deletes++))
            if curl -s -X DELETE "http://localhost:$port/v1/keys/mixed_key_$key_id" >/dev/null 2>&1; then
                ((delete_success++))
            fi
        fi
    done
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    local total_ops=$((puts + gets + deletes))
    local total_success=$((put_success + get_success + delete_success))
    local throughput=$(echo "scale=1; $total_success / $duration" | bc -l)
    local success_rate=$((total_success * 100 / total_ops))
    
    kill ${pids[@]} 2>/dev/null || true
    
    # Target: >150 req/sec with >90% success rate for mixed workload
    local throughput_int=$(printf "%.0f" "$throughput")
    if [ $throughput_int -ge 150 ] && [ $success_rate -ge 90 ]; then
        test_success "Mixed workload performance"
        test_info "${throughput} req/sec, ${success_rate}% success"
        test_info "PUT: $put_success/$puts, GET: $get_success/$gets, DELETE: $delete_success/$deletes"
    else
        test_failure "Mixed workload performance"
        test_info "${throughput} req/sec, ${success_rate}% success (target: ≥150 req/sec, ≥90%)"
    fi
    
    rm -rf test-throughput-node*/ throughput-node*.log
}

test_cluster_throughput() {
    print_test "3-node cluster throughput"
    ((TOTAL++))
    
    cleanup
    local pids=($(setup_cluster 3))
    
    if [ ${#pids[@]} -lt 3 ]; then
        test_failure "3-node cluster throughput"
        test_info "Failed to start 3-node cluster"
        return
    fi
    
    # Test load balancing across nodes
    local start_time=$(date +%s.%N)
    local client_pids=()
    
    # Start clients targeting different nodes
    for client in $(seq 1 3); do
        local target_port=$((BASE_HTTP_PORT + client - 1))
        run_throughput_client $client $target_port 50 "put" "cluster-results-$client.txt" &
        client_pids+=($!)
    done
    
    wait "${client_pids[@]}"
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    
    # Aggregate cluster results
    local cluster_success=0
    local cluster_requests=0
    
    for client in $(seq 1 3); do
        local result_file="cluster-results-$client.txt"
        if [ -f "$result_file" ]; then
            local client_data=$(cat "$result_file")
            local success=$(echo $client_data | cut -d' ' -f2)
            local requests=$(echo $client_data | cut -d' ' -f3)
            
            cluster_success=$((cluster_success + success))
            cluster_requests=$((cluster_requests + requests))
        fi
    done
    
    local cluster_throughput=$(echo "scale=1; $cluster_success / $duration" | bc -l)
    local cluster_success_rate=$((cluster_success * 100 / cluster_requests))
    
    kill ${pids[@]} 2>/dev/null || true
    
    # Target: cluster should handle requests efficiently
    local throughput_int=$(printf "%.0f" "$cluster_throughput")
    if [ $throughput_int -ge 100 ] && [ $cluster_success_rate -ge 95 ]; then
        test_success "3-node cluster throughput"
        test_info "${cluster_throughput} req/sec across 3 nodes, ${cluster_success_rate}% success"
    else
        test_failure "3-node cluster throughput"
        test_info "${cluster_throughput} req/sec, ${cluster_success_rate}% success (target: ≥100 req/sec, ≥95%)"
    fi
    
    rm -rf test-throughput-node*/ throughput-node*.log cluster-results-*.txt
}

test_latency_consistency() {
    print_test "Response latency consistency"
    ((TOTAL++))
    
    cleanup
    local pids=($(setup_cluster 1))
    
    if [ ${#pids[@]} -eq 0 ]; then
        test_failure "Response latency consistency"
        test_info "Failed to start cluster"
        return
    fi
    
    local port=$BASE_HTTP_PORT
    local latencies=()
    local num_samples=100
    
    # Measure individual request latencies
    for i in $(seq 1 $num_samples); do
        local start_time=$(date +%s%N)
        curl -s -X PUT "http://localhost:$port/v1/keys/latency_test_$i" -d "test_value" >/dev/null 2>&1
        local end_time=$(date +%s%N)
        
        local latency=$((end_time - start_time))
        local latency_ms=$((latency / 1000000))
        latencies+=($latency_ms)
    done
    
    # Calculate statistics
    local sum=0
    local min_lat=999999
    local max_lat=0
    
    for lat in "${latencies[@]}"; do
        sum=$((sum + lat))
        if [ $lat -lt $min_lat ]; then
            min_lat=$lat
        fi
        if [ $lat -gt $max_lat ]; then
            max_lat=$lat
        fi
    done
    
    local avg_lat=$((sum / num_samples))
    
    kill ${pids[@]} 2>/dev/null || true
    
    # Target: Average latency <100ms, max latency <500ms
    if [ $avg_lat -lt 100 ] && [ $max_lat -lt 500 ]; then
        test_success "Response latency consistency"
        test_info "Avg: ${avg_lat}ms, Min: ${min_lat}ms, Max: ${max_lat}ms"
    else
        test_failure "Response latency consistency"
        test_info "Avg: ${avg_lat}ms, Max: ${max_lat}ms (target: avg <100ms, max <500ms)"
    fi
    
    rm -rf test-throughput-node*/ throughput-node*.log
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Throughput Test Results${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total Tests:    ${BOLD}$TOTAL${NC}"
    echo -e "  ${GREEN}Passed:         $PASSED${NC}"
    echo -e "  ${RED}Failed:         $FAILED${NC}"
    
    local pass_rate=$((PASSED * 100 / TOTAL))
    echo -e "  Pass Rate:      ${BOLD}${pass_rate}%${NC}"
    echo ""
    echo -e "  Test Config:    ${BOLD}${CONCURRENT_CLIENTS} concurrent clients, ${REQUESTS_PER_CLIENT} requests each${NC}"
    echo ""
    
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL THROUGHPUT TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper request processing performance is excellent${NC}"
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review throughput bottlenecks${NC}"
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
echo -e "${BOLD}Running throughput performance tests...${NC}"
echo ""

test_sequential_throughput
test_concurrent_throughput
test_mixed_workload
test_cluster_throughput
test_latency_consistency

print_summary
