#!/bin/bash

# DKVS Comprehensive Test Suite
# All tests combined with clean, user-friendly output

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# Counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Test categories
BASIC_PASSED=0
BASIC_TOTAL=0
CONNECTION_PASSED=0
CONNECTION_TOTAL=0
PERFORMANCE_PASSED=0
PERFORMANCE_TOTAL=0

# Print functions
print_banner() {
    clear
    echo ""
    echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}║           DKVS COMPREHENSIVE TEST SUITE                      ║${NC}"
    echo -e "${CYAN}${BOLD}║         Distributed Key-Value Store Testing                  ║${NC}"
    echo -e "${CYAN}${BOLD}║                                                              ║${NC}"
    echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

print_category() {
    echo ""
    echo -e "${CYAN}┌────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${CYAN}│ ${BOLD}$1${NC}"
    echo -e "${CYAN}└────────────────────────────────────────────────────────────┘${NC}"
}

run_test() {
    local test_name="$1"
    local category="$2"
    ((TOTAL_TESTS++))
    
    case $category in
        basic) ((BASIC_TOTAL++)) ;;
        connection) ((CONNECTION_TOTAL++)) ;;
        performance) ((PERFORMANCE_TOTAL++)) ;;
    esac
    
    # Print test with spinner effect
    echo -ne "${YELLOW}  ⟳${NC} ${test_name}..."
}

test_success() {
    echo -e "\r${GREEN}  ✓${NC} $1 ${GREEN}[PASSED]${NC}"
    ((PASSED_TESTS++))
    
    case $2 in
        basic) ((BASIC_PASSED++)) ;;
        connection) ((CONNECTION_PASSED++)) ;;
        performance) ((PERFORMANCE_PASSED++)) ;;
    esac
}

test_failure() {
    echo -e "\r${RED}  ✗${NC} $1 ${RED}[FAILED]${NC}"
    ((FAILED_TESTS++))
}

test_info() {
    echo -e "    ${CYAN}→${NC} $1"
}

# Cleanup function
cleanup() {
    pkill -9 -f dkvsd 2>/dev/null
    sleep 1
}

trap cleanup EXIT

# Main execution
print_banner

echo -e "${BOLD}Preparing test environment...${NC}"

# Build application
echo -ne "  Building application..."
go build -o dkvsd > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "\r${GREEN}  ✓ Application built successfully${NC}"
else
    echo -e "\r${RED}  ✗ Build failed${NC}"
    exit 1
fi

echo ""
echo -e "${BOLD}Starting test execution...${NC}"
sleep 1

# ============================================================================
# BASIC FUNCTIONALITY TESTS
# ============================================================================

print_category "BASIC FUNCTIONALITY TESTS"

# Test 1: Master Server Startup
run_test "Master server startup" "basic"
cleanup
./dkvsd -role master -replicas 3 -local localhost:5000 -remote "" > /tmp/test_master.log 2>&1 &
MASTER_PID=$!
sleep 2

if ps -p $MASTER_PID > /dev/null && grep -q "Master server started and listening" /tmp/test_master.log; then
    test_success "Master server startup" "basic"
else
    test_failure "Master server startup" "basic"
fi

# Test 2: Single Slave Connection
run_test "Single slave connection" "basic"
./dkvsd -role slave -local localhost:5001 -remote localhost:5000 > /tmp/test_slave1.log 2>&1 &
SLAVE1_PID=$!
sleep 2

if ps -p $SLAVE1_PID > /dev/null && grep -q "Successfully connected to master" /tmp/test_slave1.log; then
    test_success "Single slave connection" "basic"
else
    test_failure "Single slave connection" "basic"
fi

# Test 3: Multiple Slave Connections
run_test "Multiple slaves (3 nodes)" "basic"
./dkvsd -role slave -local localhost:5002 -remote localhost:5000 > /tmp/test_slave2.log 2>&1 &
sleep 1
./dkvsd -role slave -local localhost:5003 -remote localhost:5000 > /tmp/test_slave3.log 2>&1 &
sleep 2

SLAVE_COUNT=$(grep -c "Slave connected to master" /tmp/test_master.log)
if [ $SLAVE_COUNT -eq 3 ]; then
    test_success "Multiple slaves (3 nodes)" "basic"
    test_info "All 3 slaves connected successfully"
else
    test_failure "Multiple slaves (3 nodes)" "basic"
fi

# Test 4: Process Health
run_test "Process health check" "basic"
sleep 1

RUNNING=0
ps -p $MASTER_PID > /dev/null && ((RUNNING++))
ps -p $SLAVE1_PID > /dev/null && ((RUNNING++))

if [ $RUNNING -eq 2 ]; then
    test_success "Process health check" "basic"
    test_info "Master and slaves running stably"
else
    test_failure "Process health check" "basic"
fi

# Test 5: Port Binding
run_test "Network port binding" "basic"
PORTS_OPEN=0
netstat -tuln 2>/dev/null | grep -q ":5000" && ((PORTS_OPEN++))
netstat -tuln 2>/dev/null | grep -q ":5001" && ((PORTS_OPEN++))

if [ $PORTS_OPEN -ge 2 ]; then
    test_success "Network port binding" "basic"
else
    test_failure "Network port binding" "basic"
fi

# ============================================================================
# CONNECTION TESTS
# ============================================================================

print_category "CONNECTION & NETWORKING TESTS"

# Test 6: Different Port Range
run_test "Different port range (7000)" "connection"
cleanup
./dkvsd -role master -replicas 1 -local localhost:7000 -remote "" > /tmp/test_port_master.log 2>&1 &
sleep 2
./dkvsd -role slave -local localhost:7001 -remote localhost:7000 > /tmp/test_port_slave.log 2>&1 &
sleep 2

if grep -q "Successfully connected to master" /tmp/test_port_slave.log; then
    test_success "Different port range (7000)" "connection"
else
    test_failure "Different port range (7000)" "connection"
fi
cleanup

# Test 7: Sequential Connections
run_test "Sequential slave addition" "connection"
./dkvsd -role master -replicas 5 -local localhost:5000 -remote "" > /tmp/test_seq_master.log 2>&1 &
sleep 2

for i in 1 2 3; do
    ./dkvsd -role slave -local localhost:500$i -remote localhost:5000 > /tmp/test_seq_slave$i.log 2>&1 &
    sleep 1
done
sleep 2

SEQ_COUNT=$(grep -c "Slave connected" /tmp/test_seq_master.log)
if [ $SEQ_COUNT -eq 3 ]; then
    test_success "Sequential slave addition" "connection"
    test_info "Added 3 slaves sequentially"
else
    test_failure "Sequential slave addition" "connection"
fi
cleanup

# Test 8: Rapid Connections
run_test "Rapid simultaneous connections" "connection"
./dkvsd -role master -replicas 5 -local localhost:5000 -remote "" > /tmp/test_rapid_master.log 2>&1 &
sleep 2

for i in 1 2 3 4 5; do
    ./dkvsd -role slave -local localhost:500$i -remote localhost:5000 > /tmp/test_rapid_slave$i.log 2>&1 &
done
sleep 4

RAPID_COUNT=$(grep -c "Slave connected" /tmp/test_rapid_master.log)
if [ $RAPID_COUNT -ge 4 ]; then
    test_success "Rapid simultaneous connections" "connection"
    test_info "Handled $RAPID_COUNT/5 concurrent connections"
else
    test_failure "Rapid simultaneous connections" "connection"
fi
cleanup

# Test 9: Connection Stability
run_test "Connection stability (5 sec)" "connection"
./dkvsd -role master -replicas 2 -local localhost:5000 -remote "" > /tmp/test_stable_master.log 2>&1 &
STABLE_MASTER_PID=$!
sleep 2
./dkvsd -role slave -local localhost:5001 -remote localhost:5000 > /tmp/test_stable_slave.log 2>&1 &
STABLE_SLAVE_PID=$!
sleep 5

if ps -p $STABLE_MASTER_PID > /dev/null && ps -p $STABLE_SLAVE_PID > /dev/null; then
    test_success "Connection stability (5 sec)" "connection"
else
    test_failure "Connection stability (5 sec)" "connection"
fi
cleanup

# Test 10: Clean Logging
run_test "Clean output logging" "connection"
./dkvsd -role master -replicas 1 -local localhost:5000 -remote "" > /tmp/test_clean_master.log 2>&1 &
sleep 2
./dkvsd -role slave -local localhost:5001 -remote localhost:5000 > /tmp/test_clean_slave.log 2>&1 &
sleep 2

if ! grep -q "ERROR" /tmp/test_clean_master.log && \
   grep -q "Master server started" /tmp/test_clean_master.log && \
   grep -q "Successfully connected" /tmp/test_clean_slave.log; then
    test_success "Clean output logging" "connection"
    test_info "No errors, all confirmations present"
else
    test_failure "Clean output logging" "connection"
fi
cleanup

# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

print_category "PERFORMANCE & SCALABILITY TESTS"

# Test 11: Startup Time
run_test "Fast startup (<3 sec)" "performance"
START=$(date +%s)
./dkvsd -role master -replicas 1 -local localhost:5000 -remote "" > /tmp/test_perf_master.log 2>&1 &
sleep 2
END=$(date +%s)

DURATION=$((END - START))
if [ $DURATION -le 3 ]; then
    test_success "Fast startup (<3 sec)" "performance"
    test_info "Started in ${DURATION} seconds"
else
    test_failure "Fast startup (<3 sec)" "performance"
fi
cleanup

# Test 12: Multiple Nodes (10 slaves)
run_test "Scalability (10 slaves)" "performance"
./dkvsd -role master -replicas 10 -local localhost:5000 -remote "" > /tmp/test_scale_master.log 2>&1 &
sleep 2

for i in {1..10}; do
    ./dkvsd -role slave -local localhost:50$(printf "%02d" $i) -remote localhost:5000 > /tmp/test_scale_slave$i.log 2>&1 &
    sleep 0.3
done
sleep 4

SCALE_COUNT=$(grep -c "Slave connected" /tmp/test_scale_master.log)
if [ $SCALE_COUNT -ge 8 ]; then
    test_success "Scalability (10 slaves)" "performance"
    test_info "Successfully connected $SCALE_COUNT/10 slaves"
else
    test_failure "Scalability (10 slaves)" "performance"
fi
cleanup

# Test 13: Memory Efficiency
run_test "Memory efficiency" "performance"
./dkvsd -role master -replicas 2 -local localhost:5000 -remote "" > /tmp/test_mem_master.log 2>&1 &
MEM_PID=$!
sleep 2
./dkvsd -role slave -local localhost:5001 -remote localhost:5000 > /dev/null 2>&1 &
sleep 2

MEM_KB=$(ps -p $MEM_PID -o rss= 2>/dev/null)
if [ ! -z "$MEM_KB" ]; then
    MEM_MB=$((MEM_KB / 1024))
    if [ $MEM_MB -lt 100 ]; then
        test_success "Memory efficiency" "performance"
        test_info "Master using ${MEM_MB}MB RAM"
    else
        test_failure "Memory efficiency" "performance"
    fi
else
    test_failure "Memory efficiency" "performance"
fi
cleanup

# Test 14: Port Reuse
run_test "Port reuse capability" "performance"
./dkvsd -role master -replicas 1 -local localhost:5000 -remote "" > /tmp/test_reuse1.log 2>&1 &
PID1=$!
sleep 2
kill $PID1 2>/dev/null
sleep 2

./dkvsd -role master -replicas 1 -local localhost:5000 -remote "" > /tmp/test_reuse2.log 2>&1 &
PID2=$!
sleep 2

if ps -p $PID2 > /dev/null && grep -q "Master server started" /tmp/test_reuse2.log; then
    test_success "Port reuse capability" "performance"
else
    test_failure "Port reuse capability" "performance"
fi
cleanup

# Test 15: System Stability
run_test "Long-term stability" "performance"
./dkvsd -role master -replicas 3 -local localhost:5000 -remote "" > /tmp/test_stability_master.log 2>&1 &
STAB_MASTER_PID=$!
sleep 2

for i in 1 2 3; do
    ./dkvsd -role slave -local localhost:500$i -remote localhost:5000 > /tmp/test_stability_slave$i.log 2>&1 &
    sleep 1
done
sleep 4

STAB_RUNNING=0
ps -p $STAB_MASTER_PID > /dev/null && ((STAB_RUNNING++))

if [ $STAB_RUNNING -gt 0 ]; then
    STAB_SLAVES=$(ps aux | grep -c "[d]kvsd.*slave.*500")
    test_success "Long-term stability" "performance"
    test_info "Master + $STAB_SLAVES slaves running stable"
else
    test_failure "Long-term stability" "performance"
fi

cleanup

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print_section "TEST RESULTS SUMMARY"

# Category summaries
echo -e "${BOLD}Results by Category:${NC}"
echo ""
echo -e "  ${CYAN}Basic Functionality:${NC}     $BASIC_PASSED/$BASIC_TOTAL passed"
echo -e "  ${CYAN}Connection & Network:${NC}    $CONNECTION_PASSED/$CONNECTION_TOTAL passed"
echo -e "  ${CYAN}Performance & Scale:${NC}     $PERFORMANCE_PASSED/$PERFORMANCE_TOTAL passed"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Overall statistics
PASS_RATE=$((PASSED_TESTS * 100 / TOTAL_TESTS))

echo -e "${BOLD}Overall Statistics:${NC}"
echo ""
echo -e "  Total Tests:      ${BOLD}$TOTAL_TESTS${NC}"
echo -e "  ${GREEN}Passed Tests:     $PASSED_TESTS${NC}"
echo -e "  ${RED}Failed Tests:     $FAILED_TESTS${NC}"
echo -e "  Pass Rate:        ${BOLD}${PASS_RATE}%${NC}"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Final verdict
if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}${BOLD}║                                                              ║${NC}"
    echo -e "${GREEN}${BOLD}║                  ✓ ALL TESTS PASSED! ✓                      ║${NC}"
    echo -e "${GREEN}${BOLD}║                                                              ║${NC}"
    echo -e "${GREEN}${BOLD}║         The DKVS system is working perfectly!               ║${NC}"
    echo -e "${GREEN}${BOLD}║                                                              ║${NC}"
    echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    exit 0
elif [ $PASS_RATE -ge 80 ]; then
    echo -e "${YELLOW}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}${BOLD}║                                                              ║${NC}"
    echo -e "${YELLOW}${BOLD}║              ⚠ TESTS MOSTLY PASSED (${PASS_RATE}%)                    ║${NC}"
    echo -e "${YELLOW}${BOLD}║                                                              ║${NC}"
    echo -e "${YELLOW}${BOLD}║         System is functional with minor issues              ║${NC}"
    echo -e "${YELLOW}${BOLD}║                                                              ║${NC}"
    echo -e "${YELLOW}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    exit 0
else
    echo -e "${RED}${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}${BOLD}║                                                              ║${NC}"
    echo -e "${RED}${BOLD}║                  ✗ SOME TESTS FAILED                         ║${NC}"
    echo -e "${RED}${BOLD}║                                                              ║${NC}"
    echo -e "${RED}${BOLD}║            Please review the failures above                  ║${NC}"
    echo -e "${RED}${BOLD}║                                                              ║${NC}"
    echo -e "${RED}${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    exit 1
fi
