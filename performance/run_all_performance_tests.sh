#!/bin/bash

# Keyper Performance Test Suite Runner
# Executes all performance tests and generates comprehensive report

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# Test configuration
PERFORMANCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$PERFORMANCE_DIR/.." && pwd)"
REPORT_FILE="performance_report_$(date +%Y%m%d_%H%M%S).txt"

# Test results tracking
declare -a TEST_NAMES
declare -a TEST_RESULTS
declare -a TEST_TIMES
declare -a TEST_DETAILS

print_header() {
    clear
    echo ""
    echo -e "${MAGENTA}${BOLD}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}${BOLD}║                                                                          ║${NC}"
    echo -e "${MAGENTA}${BOLD}║                    KEYPER PERFORMANCE TEST SUITE                         ║${NC}"
    echo -e "${MAGENTA}${BOLD}║                      Comprehensive Testing Runner                        ║${NC}"
    echo -e "${MAGENTA}${BOLD}║                                                                          ║${NC}"
    echo -e "${MAGENTA}${BOLD}╚══════════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

run_test() {
    local test_name="$1"
    local test_script="$2"
    local description="$3"
    
    echo -e "${CYAN}${BOLD}Running: $test_name${NC}"
    echo -e "${CYAN}Description: $description${NC}"
    echo ""
    
    local start_time=$(date +%s)
    
    if [ ! -f "$test_script" ]; then
        echo -e "${RED}ERROR: Test script not found: $test_script${NC}"
        TEST_NAMES+=("$test_name")
        TEST_RESULTS+=("MISSING")
        TEST_TIMES+=("0")
        TEST_DETAILS+=("Test script not found")
        return 1
    fi
    
    if [ ! -x "$test_script" ]; then
        chmod +x "$test_script"
    fi
    
    # Run the test and capture output
    local test_output=""
    if $test_script > "temp_test_output.log" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        TEST_NAMES+=("$test_name")
        TEST_RESULTS+=("PASSED")
        TEST_TIMES+=("$duration")
        
        # Extract key metrics from output
        local metrics=$(tail -20 "temp_test_output.log" | grep -E "(req/sec|MB|ms|nodes|%)" | head -3 | tr '\n' '; ')
        TEST_DETAILS+=("$metrics")
        
        echo -e "${GREEN}✓ PASSED${NC} (${duration}s)"
        echo ""
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        TEST_NAMES+=("$test_name")
        TEST_RESULTS+=("FAILED")
        TEST_TIMES+=("$duration")
        
        # Extract error information
        local error_info=$(tail -10 "temp_test_output.log" | grep -E "(FAILED|ERROR|error|failed)" | head -2 | tr '\n' '; ')
        TEST_DETAILS+=("$error_info")
        
        echo -e "${RED}✗ FAILED${NC} (${duration}s)"
        echo -e "${RED}See temp_test_output.log for details${NC}"
        echo ""
    fi
    
    # Archive test output
    mv "temp_test_output.log" "${test_name// /_}_$(date +%Y%m%d_%H%M%S).log" 2>/dev/null || true
}

check_prerequisites() {
    echo -e "${BOLD}Checking prerequisites...${NC}"
    
    # Check if we're in the right directory
    if [ ! -f "$PROJECT_ROOT/go.mod" ]; then
        echo -e "${RED}Error: Must run from Keyper project root${NC}"
        echo "Current directory: $(pwd)"
        echo "Expected: Directory containing go.mod"
        exit 1
    fi
    
    # Build the server binary
    echo -ne "  Building Keyper server..."
    cd "$PROJECT_ROOT"
    if go build -o bin/server ./cmd/server > build.log 2>&1; then
        echo -e "\r${GREEN}  ✓ Server binary built successfully${NC}"
    else
        echo -e "\r${RED}  ✗ Failed to build server binary${NC}"
        echo "Build log:"
        cat build.log
        exit 1
    fi
    
    # Check system requirements
    echo -ne "  Checking system requirements..."
    local missing_tools=""
    
    for tool in curl netstat ps bc; do
        if ! command -v $tool >/dev/null 2>&1; then
            missing_tools="$missing_tools $tool"
        fi
    done
    
    if [ -n "$missing_tools" ]; then
        echo -e "\r${RED}  ✗ Missing required tools:$missing_tools${NC}"
        exit 1
    else
        echo -e "\r${GREEN}  ✓ System requirements met${NC}"
    fi
    
    # Check available ports
    echo -ne "  Checking port availability..."
    local ports_in_use=0
    for port in {8080..8100} {9080..9100}; do
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            ((ports_in_use++))
        fi
    done
    
    if [ $ports_in_use -gt 5 ]; then
        echo -e "\r${YELLOW}  ⚠ Warning: $ports_in_use test ports already in use${NC}"
    else
        echo -e "\r${GREEN}  ✓ Ports available for testing${NC}"
    fi
    
    echo -e "${GREEN}✓ All prerequisites met${NC}"
}

generate_report() {
    local total_tests=${#TEST_NAMES[@]}
    local passed_tests=0
    local failed_tests=0
    local total_time=0
    
    # Count results
    for result in "${TEST_RESULTS[@]}"; do
        case $result in
            "PASSED") ((passed_tests++)) ;;
            "FAILED") ((failed_tests++)) ;;
        esac
    done
    
    for time in "${TEST_TIMES[@]}"; do
        total_time=$((total_time + time))
    done
    
    local pass_rate=$((passed_tests * 100 / total_tests))
    
    # Generate comprehensive report
    {
        echo "KEYPER PERFORMANCE TEST REPORT"
        echo "=============================="
        echo ""
        echo "Generated: $(date)"
        echo "Test Duration: ${total_time}s ($(($total_time/60))m $(($total_time%60))s)"
        echo "Project: Keyper Distributed Key-Value Store"
        echo ""
        echo "SUMMARY"
        echo "-------"
        echo "Total Tests: $total_tests"
        echo "Passed: $passed_tests"
        echo "Failed: $failed_tests"
        echo "Pass Rate: ${pass_rate}%"
        echo ""
        echo "TEST RESULTS"
        echo "------------"
        
        for i in "${!TEST_NAMES[@]}"; do
            local name="${TEST_NAMES[$i]}"
            local result="${TEST_RESULTS[$i]}"
            local time="${TEST_TIMES[$i]}"
            local details="${TEST_DETAILS[$i]}"
            
            echo ""
            echo "Test: $name"
            echo "Result: $result"
            echo "Duration: ${time}s"
            if [ -n "$details" ]; then
                echo "Details: $details"
            fi
        done
        
        echo ""
        echo "PERFORMANCE ANALYSIS"
        echo "==================="
        echo ""
        
        # Analyze test categories
        echo "Test Category Breakdown:"
        local startup_tests=0
        local memory_tests=0
        local scalability_tests=0
        local throughput_tests=0
        local stability_tests=0
        
        for name in "${TEST_NAMES[@]}"; do
            case $name in
                *"Startup"*|*"startup"*) ((startup_tests++)) ;;
                *"Memory"*|*"memory"*) ((memory_tests++)) ;;
                *"Scalability"*|*"scalability"*|*"Cluster"*) ((scalability_tests++)) ;;
                *"Throughput"*|*"throughput"*|*"Request"*) ((throughput_tests++)) ;;
                *"Stability"*|*"stability"*) ((stability_tests++)) ;;
            esac
        done
        
        echo "  - Startup Performance: $startup_tests tests"
        echo "  - Memory Efficiency: $memory_tests tests"
        echo "  - Scalability: $scalability_tests tests"
        echo "  - Throughput: $throughput_tests tests"
        echo "  - Stability: $stability_tests tests"
        echo ""
        
        # Performance recommendations
        echo "RECOMMENDATIONS"
        echo "==============="
        
        if [ $pass_rate -eq 100 ]; then
            echo "✓ Excellent: All performance tests passed"
            echo "  - System is ready for production deployment"
            echo "  - Performance characteristics are within acceptable limits"
            echo "  - Continue monitoring in production environment"
        elif [ $pass_rate -ge 80 ]; then
            echo "⚠ Good: Most performance tests passed ($pass_rate%)"
            echo "  - System shows good overall performance"
            echo "  - Review failed tests for optimization opportunities"
            echo "  - Consider load testing in staging environment"
        else
            echo "✗ Needs Attention: Performance issues detected ($pass_rate% pass rate)"
            echo "  - Critical performance problems need to be addressed"
            echo "  - Review failed tests and optimize before production"
            echo "  - Consider infrastructure scaling or code optimization"
        fi
        
        echo ""
        echo "SYSTEM INFORMATION"
        echo "=================="
        echo "OS: $(uname -s) $(uname -r)"
        echo "Architecture: $(uname -m)"
        echo "CPU Cores: $(nproc 2>/dev/null || echo "Unknown")"
        echo "Memory: $(free -h 2>/dev/null | grep "Mem:" | awk '{print $2}' || echo "Unknown")"
        echo "Go Version: $(go version 2>/dev/null || echo "Unknown")"
        echo ""
        echo "End of Report"
        
    } > "$REPORT_FILE"
}

cleanup() {
    # Kill any remaining test processes
    pkill -9 -f "bin/server" 2>/dev/null || true
    pkill -9 -f "keyper" 2>/dev/null || true
    sleep 1
    
    # Clean up test data
    rm -rf test-*/ *-test*.log temp_test_output.log build.log 2>/dev/null || true
}

show_final_summary() {
    local total_tests=${#TEST_NAMES[@]}
    local passed_tests=0
    local failed_tests=0
    
    for result in "${TEST_RESULTS[@]}"; do
        case $result in
            "PASSED") ((passed_tests++)) ;;
            "FAILED") ((failed_tests++)) ;;
        esac
    done
    
    local pass_rate=$((passed_tests * 100 / total_tests))
    
    print_section "FINAL RESULTS"
    
    echo -e "${BOLD}Performance Test Suite Completed${NC}"
    echo ""
    echo -e "  Total Tests:      ${BOLD}$total_tests${NC}"
    echo -e "  ${GREEN}Passed:           $passed_tests${NC}"
    echo -e "  ${RED}Failed:           $failed_tests${NC}"
    echo -e "  Pass Rate:        ${BOLD}${pass_rate}%${NC}"
    echo ""
    echo -e "  Report File:      ${BOLD}$REPORT_FILE${NC}"
    echo ""
    
    # Detailed results table
    echo -e "${BOLD}Test Results Summary:${NC}"
    echo ""
    printf "%-35s %-8s %-8s %s\n" "Test Name" "Result" "Time(s)" "Key Metrics"
    echo "$(printf '%.0s─' {1..80})"
    
    for i in "${!TEST_NAMES[@]}"; do
        local name="${TEST_NAMES[$i]}"
        local result="${TEST_RESULTS[$i]}"
        local time="${TEST_TIMES[$i]}"
        local details="${TEST_DETAILS[$i]}"
        
        # Truncate long names and details
        local short_name="${name:0:33}"
        local short_details="${details:0:25}"
        
        local color=""
        case $result in
            "PASSED") color="${GREEN}" ;;
            "FAILED") color="${RED}" ;;
            "MISSING") color="${YELLOW}" ;;
        esac
        
        printf "%-35s ${color}%-8s${NC} %-8s %s\n" "$short_name" "$result" "${time}s" "$short_details"
    done
    
    echo ""
    
    if [ $failed_tests -eq 0 ]; then
        echo -e "${GREEN}${BOLD}🎉 ALL PERFORMANCE TESTS PASSED! 🎉${NC}"
        echo -e "${GREEN}Keyper is performing excellently across all test categories.${NC}"
        echo ""
        exit 0
    elif [ $pass_rate -ge 80 ]; then
        echo -e "${YELLOW}${BOLD}⚠️  PERFORMANCE TESTS MOSTLY SUCCESSFUL (${pass_rate}%) ⚠️${NC}"
        echo -e "${YELLOW}System shows good performance with some areas for improvement.${NC}"
        echo ""
        exit 0
    else
        echo -e "${RED}${BOLD}❌ PERFORMANCE ISSUES DETECTED (${pass_rate}% pass rate) ❌${NC}"
        echo -e "${RED}Significant performance problems need attention before production use.${NC}"
        echo ""
        exit 1
    fi
}

# Main execution
trap cleanup EXIT

print_header

echo -e "${BOLD}Keyper Performance Test Suite${NC}"
echo -e "${CYAN}This suite will run comprehensive performance tests including:${NC}"
echo -e "  • Startup Time Performance"
echo -e "  • Memory Efficiency Analysis"  
echo -e "  • Cluster Scalability Testing"
echo -e "  • Request Throughput Measurement"
echo -e "  • Long-term Stability Validation"
echo ""
echo -e "${YELLOW}Estimated total runtime: 15-25 minutes${NC}"
echo ""

read -p "Press Enter to continue or Ctrl+C to cancel..."

check_prerequisites

cd "$PERFORMANCE_DIR"

print_section "PERFORMANCE TEST EXECUTION"

# Run all performance tests
run_test "Startup Time Performance" \
    "./test_startup_time.sh" \
    "Tests server startup speed, cold/warm starts, and concurrent initialization"

run_test "Memory Efficiency Analysis" \
    "./test_memory_efficiency.sh" \
    "Analyzes memory usage patterns, leak detection, and efficiency under load"

run_test "Cluster Scalability Testing" \
    "./test_cluster_scalability.sh" \
    "Evaluates performance with increasing node counts from 1-10 nodes"

run_test "Request Throughput Measurement" \
    "./test_request_throughput.sh" \
    "Measures HTTP request processing rates and response latencies"

run_test "Long-term Stability Validation" \
    "./test_long_term_stability.sh" \
    "Tests system behavior under extended runtime and continuous load"

print_section "GENERATING PERFORMANCE REPORT"

echo -e "${BOLD}Analyzing results and generating report...${NC}"
generate_report
echo -e "${GREEN}✓ Performance report generated: $REPORT_FILE${NC}"

show_final_summary
