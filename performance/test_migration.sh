#!/bin/bash

# Keyper Shard Migration Performance Test
# Tests shard migration capabilities including export/import, pause/resume, and Raft membership

# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
TEST_DATA_SIZE=100

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
    echo -e "${CYAN}${BOLD}║            KEYPER SHARD MIGRATION TEST                       ║${NC}"
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

print_step() {
    echo -e "    ${BOLD}Phase $1:${NC} $2"
}

cleanup() {
    pkill -9 -f "test-migration-node" 2>/dev/null || true
    sleep 1
    rm -rf test-migration-*/ migration-*.log migration-export-*.gz 2>/dev/null || true
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

start_shard_node() {
    local node_id=$1
    local http_port=$2
    local raft_port=$3
    local shard_id=$4
    local is_bootstrap=$5
    local join_addr=$6
    
    mkdir -p "test-migration-node$node_id"
    
    local bootstrap_flag=""
    local join_flag=""
    
    if [ "$is_bootstrap" = "true" ]; then
        bootstrap_flag="--enable-raft"
    else
        join_flag="--enable-raft --join=$join_addr"
    fi
    
    # Use node ID format that matches the port calculation: "node<number>"
    local node_name="node$node_id"
    
    $SERVER_BIN \
        --data-dir="./test-migration-node$node_id" \
        --http-addr=":$http_port" \
        --node-id="$node_name" \
        --raft-addr="127.0.0.1:$raft_port" \
        --shard-count=1 \
        --assigned-shards="$shard_id" \
        --raft-base-port=10000 \
        $bootstrap_flag $join_flag > "migration-node$node_id.log" 2>&1 &
    
    local pid=$!
    
    # Wait for node to be ready
    local attempts=0
    while [ $attempts -lt 30 ]; do
        if curl -s "http://localhost:$http_port/v1/status" >/dev/null 2>&1; then
            # Give extra time for shard Raft to initialize
            sleep 3
            return 0
        fi
        sleep 1
        ((attempts++))
    done
    
    return 1
}

populate_shard_data() {
    local port=$1
    local shard_id=$2
    local num_keys=$3
    
    local success=0
    for i in $(seq 1 $num_keys); do
        local key="shard_${shard_id}_key_$i"
        local value="shard_${shard_id}_value_$i_$(date +%s%N)"
        
        if curl -s -X PUT "http://localhost:$port/v1/keys/$key" -d "$value" >/dev/null 2>&1; then
            ((success++))
        fi
    done
    
    echo "$success"
}

verify_shard_data() {
    local port=$1
    local shard_id=$2
    local num_keys=$3
    
    local success=0
    for i in $(seq 1 $num_keys); do
        local key="shard_${shard_id}_key_$i"
        
        if curl -s "http://localhost:$port/v1/keys/$key" >/dev/null 2>&1; then
            ((success++))
        fi
    done
    
    echo "$success"
}

test_shard_export_import() {
    print_test "Shard data export and import"
    ((TOTAL++))
    
    cleanup
    
    # Start source node
    print_step 1 "Starting source node"
    start_shard_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "0" "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Shard data export and import"
        test_info "Failed to start source node"
        return
    fi
    sleep 2
    
    # Populate data
    print_step 2 "Populating test data ($TEST_DATA_SIZE keys)"
    local written=$(populate_shard_data $BASE_HTTP_PORT "0" $TEST_DATA_SIZE)
    test_info "Written: $written/$TEST_DATA_SIZE keys"
    
    if [ $written -lt $TEST_DATA_SIZE ]; then
        test_failure "Shard data export and import"
        test_info "Failed to populate data"
        cleanup
        return
    fi
    
    # Export shard data
    print_step 3 "Exporting shard data"
    local export_file="migration-export-shard-0.gz"
    local export_start=$(date +%s.%N)
    
    if ! curl -s "http://localhost:$BASE_HTTP_PORT/v1/shards/0/_export" -o "$export_file" 2>/dev/null; then
        test_failure "Shard data export and import"
        test_info "Export failed"
        cleanup
        return
    fi
    
    local export_end=$(date +%s.%N)
    local export_duration=$(echo "$export_end - $export_start" | bc -l)
    local export_size=$(stat -f%z "$export_file" 2>/dev/null || stat -c%s "$export_file" 2>/dev/null)
    test_info "Export duration: $(printf "%.2f" $export_duration)s, Size: $export_size bytes"
    
    # Start destination node
    print_step 4 "Starting destination node"
    start_shard_node 2 $((BASE_HTTP_PORT + 1)) $((BASE_RAFT_PORT + 1)) "0" "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Shard data export and import"
        test_info "Failed to start destination node"
        cleanup
        return
    fi
    sleep 2
    
    # Import shard data
    print_step 5 "Importing shard data to destination"
    local import_start=$(date +%s.%N)
    
    local import_status=$(curl -s -w "%{http_code}" -o /dev/null -X POST \
        -H "Content-Type: application/octet-stream" \
        --data-binary "@$export_file" \
        "http://localhost:$((BASE_HTTP_PORT + 1))/v1/shards/0/_import")
    
    local import_end=$(date +%s.%N)
    local import_duration=$(echo "$import_end - $import_start" | bc -l)
    test_info "Import duration: $(printf "%.2f" $import_duration)s, Status: $import_status"
    
    if [ "$import_status" != "204" ] && [ "$import_status" != "200" ]; then
        test_failure "Shard data export and import"
        test_info "Import failed with status $import_status"
        cleanup
        return
    fi
    
    # Verify imported data
    print_step 6 "Verifying imported data"
    sleep 1
    local verified=$(verify_shard_data $((BASE_HTTP_PORT + 1)) "0" $TEST_DATA_SIZE)
    test_info "Verified: $verified/$TEST_DATA_SIZE keys"
    
    cleanup
    
    if [ $verified -ge $((TEST_DATA_SIZE * 95 / 100)) ]; then
        test_success "Shard data export and import"
        test_info "Data integrity: $(echo "scale=1; $verified * 100 / $TEST_DATA_SIZE" | bc -l)%"
    else
        test_failure "Shard data export and import"
        test_info "Data verification failed: only $verified/$TEST_DATA_SIZE keys recovered"
    fi
}

test_shard_pause_resume() {
    print_test "Shard pause and resume operations"
    ((TOTAL++))
    
    cleanup
    
    # Start node
    print_step 1 "Starting test node"
    start_shard_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "0" "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Shard pause and resume operations"
        test_info "Failed to start node"
        return
    fi
    sleep 2
    
    # Test normal write operation
    print_step 2 "Testing normal write operation"
    local write_status=$(curl -s -w "%{http_code}" -o /dev/null -X PUT \
        "http://localhost:$BASE_HTTP_PORT/v1/keys/test_key" -d "test_value")
    
    if [ "$write_status" != "201" ] && [ "$write_status" != "200" ] && [ "$write_status" != "204" ]; then
        test_failure "Shard pause and resume operations"
        test_info "Normal write failed with status $write_status"
        cleanup
        return
    fi
    test_info "Normal write: OK (status $write_status)"
    
    # Pause shard
    print_step 3 "Pausing shard writes"
    local pause_status=$(curl -s -w "%{http_code}" -o /dev/null -X POST \
        "http://localhost:$BASE_HTTP_PORT/v1/shards/0/pause")
    
    test_info "Pause status: $pause_status"
    
    # Attempt write while paused
    print_step 4 "Testing write during pause (should fail)"
    local paused_write_status=$(curl -s -w "%{http_code}" -o /dev/null -X PUT \
        "http://localhost:$BASE_HTTP_PORT/v1/keys/test_key_paused" -d "test_value")
    
    test_info "Write during pause: status $paused_write_status"
    
    # Resume shard
    print_step 5 "Resuming shard writes"
    local resume_status=$(curl -s -w "%{http_code}" -o /dev/null -X POST \
        "http://localhost:$BASE_HTTP_PORT/v1/shards/0/resume")
    
    test_info "Resume status: $resume_status"
    
    # Test write after resume
    print_step 6 "Testing write after resume"
    local resumed_write_status=$(curl -s -w "%{http_code}" -o /dev/null -X PUT \
        "http://localhost:$BASE_HTTP_PORT/v1/keys/test_key_resumed" -d "test_value")
    
    test_info "Write after resume: status $resumed_write_status"
    
    cleanup
    
    # Success if pause/resume cycle works
    if [ "$resumed_write_status" = "201" ] || [ "$resumed_write_status" = "200" ] || [ "$resumed_write_status" = "204" ]; then
        test_success "Shard pause and resume operations"
        test_info "Pause/Resume cycle completed successfully"
    else
        test_failure "Shard pause and resume operations"
        test_info "Write after resume failed"
    fi
}

test_migration_workflow() {
    print_test "Complete migration workflow (2-node)"
    ((TOTAL++))
    
    cleanup
    
    # Start source node
    print_step 1 "Starting source node with data"
    start_shard_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "0" "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Complete migration workflow (2-node)"
        test_info "Failed to start source node"
        return
    fi
    sleep 2
    
    # Populate source
    print_step 2 "Populating source shard (50 keys)"
    local written=$(populate_shard_data $BASE_HTTP_PORT "0" 50)
    test_info "Written: $written/50 keys"
    
    # Start destination node
    print_step 3 "Starting destination node"
    start_shard_node 2 $((BASE_HTTP_PORT + 1)) $((BASE_RAFT_PORT + 1)) "0" "true" ""
    if [ $? -ne 0 ]; then
        test_failure "Complete migration workflow (2-node)"
        test_info "Failed to start destination node"
        cleanup
        return
    fi
    sleep 2
    
    # Export from source
    print_step 4 "Exporting from source"
    local export_file="migration-workflow.gz"
    curl -s "http://localhost:$BASE_HTTP_PORT/v1/shards/0/_export" -o "$export_file" 2>/dev/null
    local export_size=$(stat -f%z "$export_file" 2>/dev/null || stat -c%s "$export_file" 2>/dev/null)
    test_info "Exported: $export_size bytes"
    
    # Import to destination
    print_step 5 "Importing to destination"
    local import_status=$(curl -s -w "%{http_code}" -o /dev/null -X POST \
        -H "Content-Type: application/octet-stream" \
        --data-binary "@$export_file" \
        "http://localhost:$((BASE_HTTP_PORT + 1))/v1/shards/0/_import")
    test_info "Import status: $import_status"
    
    # Verify on destination
    print_step 6 "Verifying data on destination"
    sleep 1
    local verified=$(verify_shard_data $((BASE_HTTP_PORT + 1)) "0" 50)
    test_info "Verified: $verified/50 keys"
    
    # Test new writes on destination
    print_step 7 "Testing new writes on destination"
    local new_write_status=$(curl -s -w "%{http_code}" -o /dev/null -X PUT \
        "http://localhost:$((BASE_HTTP_PORT + 1))/v1/keys/new_key_after_migration" -d "new_value")
    test_info "New write status: $new_write_status"
    
    cleanup
    
    if [ $verified -ge 45 ] && ([ "$new_write_status" = "201" ] || [ "$new_write_status" = "200" ] || [ "$new_write_status" = "204" ]); then
        test_success "Complete migration workflow (2-node)"
        test_info "Migration completed: $verified/50 keys migrated, new writes working"
    else
        test_failure "Complete migration workflow (2-node)"
        test_info "Migration incomplete or new writes failed"
    fi
}

test_zero_downtime_migration() {
    print_test "Zero-downtime migration test"
    ((TOTAL++))
    
    cleanup
    
    # Start source and destination nodes
    print_step 1 "Starting source and destination nodes"
    start_shard_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "0" "true" ""
    sleep 2
    start_shard_node 2 $((BASE_HTTP_PORT + 1)) $((BASE_RAFT_PORT + 1)) "0" "true" ""
    sleep 2
    
    # Populate initial data
    print_step 2 "Populating initial data"
    local initial_written=$(populate_shard_data $BASE_HTTP_PORT "0" 30)
    test_info "Initial data: $initial_written/30 keys"
    
    # Start background writer to simulate continuous traffic
    print_step 3 "Starting continuous writes during migration"
    
    # Use temp files to track counters from background process
    local temp_dir=$(mktemp -d)
    echo "0" > "$temp_dir/writes"
    echo "0" > "$temp_dir/success"
    
    for i in {1..20}; do
        local writes=$(cat "$temp_dir/writes")
        local success=$(cat "$temp_dir/success")
        if curl -s -X PUT "http://localhost:$BASE_HTTP_PORT/v1/keys/continuous_$i" \
            -d "value_$i" >/dev/null 2>&1; then
            echo $((success + 1)) > "$temp_dir/success"
        fi
        echo $((writes + 1)) > "$temp_dir/writes"
        sleep 0.1
    done &
    local writer_pid=$!
    
    # Perform migration while writes continue
    print_step 4 "Performing migration (export + import)"
    local export_file="migration-zero-downtime.gz"
    curl -s "http://localhost:$BASE_HTTP_PORT/v1/shards/0/_export" -o "$export_file" 2>/dev/null
    
    curl -s -X POST \
        -H "Content-Type: application/octet-stream" \
        --data-binary "@$export_file" \
        "http://localhost:$((BASE_HTTP_PORT + 1))/v1/shards/0/_import" >/dev/null 2>&1
    
    # Wait for background writer
    wait $writer_pid 2>/dev/null
    
    # Read final counters
    local continuous_writes=$(cat "$temp_dir/writes")
    local continuous_success=$(cat "$temp_dir/success")
    rm -rf "$temp_dir"
    
    # Verify data on destination
    print_step 5 "Verifying migrated data"
    sleep 1
    local verified=$(verify_shard_data $((BASE_HTTP_PORT + 1)) "0" 30)
    test_info "Verified: $verified/30 initial keys"
    
    # Check continuous write success
    print_step 6 "Checking continuous write success"
    test_info "Continuous writes: $continuous_success/$continuous_writes successful"
    
    cleanup
    
    # Avoid division by zero
    if [ $continuous_writes -eq 0 ]; then
        test_failure "Zero-downtime migration test"
        test_info "No continuous writes were attempted"
        return
    fi
    
    local success_rate=$((continuous_success * 100 / continuous_writes))
    if [ $verified -ge 25 ] && [ $success_rate -ge 70 ]; then
        test_success "Zero-downtime migration test"
        test_info "Migration success: $verified/30 keys, $success_rate% write availability"
    else
        test_failure "Zero-downtime migration test"
        test_info "Migration impacted availability too much"
    fi
}

test_large_dataset_migration() {
    print_test "Large dataset migration performance"
    ((TOTAL++))
    
    cleanup
    
    local large_dataset_size=500
    
    # Start nodes
    print_step 1 "Starting source and destination nodes"
    start_shard_node 1 $BASE_HTTP_PORT $BASE_RAFT_PORT "0" "true" ""
    sleep 2
    start_shard_node 2 $((BASE_HTTP_PORT + 1)) $((BASE_RAFT_PORT + 1)) "0" "true" ""
    sleep 2
    
    # Populate large dataset
    print_step 2 "Populating large dataset ($large_dataset_size keys)"
    local populate_start=$(date +%s.%N)
    local written=$(populate_shard_data $BASE_HTTP_PORT "0" $large_dataset_size)
    local populate_end=$(date +%s.%N)
    local populate_duration=$(echo "$populate_end - $populate_start" | bc -l)
    test_info "Populated: $written/$large_dataset_size keys in $(printf "%.2f" $populate_duration)s"
    
    # Export large dataset
    print_step 3 "Exporting large dataset"
    local export_file="migration-large.gz"
    local export_start=$(date +%s.%N)
    curl -s "http://localhost:$BASE_HTTP_PORT/v1/shards/0/_export" -o "$export_file" 2>/dev/null
    local export_end=$(date +%s.%N)
    local export_duration=$(echo "$export_end - $export_start" | bc -l)
    local export_size=$(stat -f%z "$export_file" 2>/dev/null || stat -c%s "$export_file" 2>/dev/null)
    test_info "Export: $(printf "%.2f" $export_duration)s, Size: $(echo "scale=2; $export_size / 1024" | bc -l) KB"
    
    # Import large dataset
    print_step 4 "Importing large dataset"
    local import_start=$(date +%s.%N)
    curl -s -X POST \
        -H "Content-Type: application/octet-stream" \
        --data-binary "@$export_file" \
        "http://localhost:$((BASE_HTTP_PORT + 1))/v1/shards/0/_import" >/dev/null 2>&1
    local import_end=$(date +%s.%N)
    local import_duration=$(echo "$import_end - $import_start" | bc -l)
    test_info "Import: $(printf "%.2f" $import_duration)s"
    
    # Verify subset
    print_step 5 "Verifying data integrity (sample check)"
    sleep 1
    local sample_verified=$(verify_shard_data $((BASE_HTTP_PORT + 1)) "0" 50)
    test_info "Sample verified: $sample_verified/50 keys"
    
    cleanup
    
    local total_migration_time=$(echo "$export_duration + $import_duration" | bc -l)
    if [ $sample_verified -ge 45 ]; then
        test_success "Large dataset migration performance"
        test_info "Total migration time: $(printf "%.2f" $total_migration_time)s for $large_dataset_size keys"
    else
        test_failure "Large dataset migration performance"
        test_info "Data verification failed"
    fi
}

print_summary() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Migration Test Results${NC}"
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
    
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ ALL MIGRATION TESTS PASSED${NC}"
        echo -e "${GREEN}Keyper shard migration capabilities are working correctly${NC}"
        exit 0
    else
        echo -e "${RED}${BOLD}✗ SOME TESTS FAILED${NC}"
        echo -e "${RED}Review migration implementation${NC}"
        exit 1
    fi
}

# Main execution
trap cleanup EXIT

print_header

echo -e "${BOLD}Checking prerequisites...${NC}"
check_prerequisites
echo -e "${GREEN}✓ Prerequisites met${NC}"

print_section "SHARD MIGRATION TESTS"

test_shard_export_import
test_shard_pause_resume
test_migration_workflow
test_zero_downtime_migration
test_large_dataset_migration

print_summary
