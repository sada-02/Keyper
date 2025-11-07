# Keyper Performance Test Suite

This directory contains individual performance test scripts for the Keyper distributed key-value store system.

## Overview

The performance tests are designed to evaluate different aspects of the Keyper system:
- **Startup Performance**: How quickly nodes can start and join clusters
- **Connection Performance**: Network connectivity and cluster formation
- **Scalability**: Performance with increasing number of nodes
- **Memory Efficiency**: Resource usage monitoring
- **Throughput**: Request processing capacity
- **Latency**: Response time measurements

## Test Scripts

### 1. Core Performance Tests
- `test_startup_time.sh` - Server startup performance (cold/warm starts, concurrent initialization)
- `test_memory_efficiency.sh` - Memory usage analysis (leak detection, efficiency under load)
- `test_cluster_scalability.sh` - Scalability testing (1-10 nodes, failure recovery)
- `test_request_throughput.sh` - HTTP request performance (sequential/concurrent, latency)
- `test_long_term_stability.sh` - Extended runtime testing (5min stability, data persistence)
- `test_port_reuse.sh` - Port management testing (reuse capability, cleanup after crashes)
- `test_migration.sh` - Shard migration testing (export/import, pause/resume, zero-downtime)
- `test_metrics.sh` - Prometheus metrics testing (endpoint availability, accuracy, performance)

### 2. Execution Scripts
- `run_all_performance_tests.sh` - Master test runner (executes all tests, generates reports)
- `quick_test.sh` - Rapid feedback tests (essential checks in 2-3 minutes)

### 3. Configuration
- `config.conf` - Test configuration parameters (ports, thresholds, test selection)

## Test Coverage

The performance test suite covers:

### ✅ **Available Tests**
- **Startup Performance**: Cold starts, warm restarts, concurrent initialization
- **Memory Efficiency**: Idle usage, load testing, leak detection, recovery
- **Cluster Scalability**: 1-10 nodes, connection handling, failure recovery
- **Request Throughput**: Sequential/concurrent requests, mixed workloads, latency
- **Long-term Stability**: Extended runtime, memory monitoring, data persistence  
- **Port Management**: Reuse capability, rapid restarts, cleanup verification
- **Shard Migration**: Export/import, pause/resume, zero-downtime migration, large datasets
- **Metrics Collection**: Prometheus endpoint, HTTP/Raft/BadgerDB metrics, accuracy validation

### ✅ **Recent Fixes Applied (November 2025)**
- **✅ Concurrency Issues**: Fixed concurrent startup test parameters and cluster formation
- **✅ Server Parameters**: Corrected --enable-raft and --join parameter formats  
- **✅ Health Endpoints**: Updated all tests to use /v1/status endpoint (was /v1/health)
- **✅ Path Resolution**: Fixed server binary paths (../bin/server)
- **✅ Memory Tests**: Fixed load testing to avoid overwhelming servers
- **✅ Arithmetic Errors**: Fixed floating point arithmetic in stability tests  
- **✅ Leader Detection**: Updated to use "is_leader" field in status responses
- **✅ Port Reuse**: Fixed function name mismatch in port reuse tests
- **✅ Cleanup**: Removed temporary fix scripts and unnecessary backup files

### 🔧 **Current Test Status**
- **✅ Startup Performance**: 100% pass rate (5/5 tests)
- **✅ Memory Efficiency**: 100% pass rate (5/5 tests) 
- **✅ Request Throughput**: 80% pass rate (4/5 tests) - cluster throughput needs tuning
- **✅ Port Reuse**: 100% pass rate (5/5 tests)
- **⚠️ Cluster Scalability**: Needs optimization for large clusters (1+ nodes work)
- **⚠️ Long-term Stability**: Fixed arithmetic errors, needs full testing

### 🔄 **Future Enhancements** (Not Yet Implemented)
- Multi-cluster performance evaluation
- Advanced cluster resilience testing

## Usage

### Quick Start
```bash
# Navigate to performance directory
cd /path/to/keyper/performance

# Quick performance check (2-3 minutes)
./quick_test.sh

# Comprehensive test suite (15-25 minutes)
./run_all_performance_tests.sh
```

### Individual Test Categories
```bash
# Core performance tests (run independently)
./test_startup_time.sh          # Startup performance
./test_memory_efficiency.sh     # Memory analysis  
./test_cluster_scalability.sh   # Scalability testing
./test_request_throughput.sh    # Throughput measurement
./test_long_term_stability.sh   # Stability validation
./test_port_reuse.sh            # Port management
./test_migration.sh             # Shard migration testing
./test_metrics.sh               # Metrics collection testing
```

## Prerequisites

1. **Go Environment**: Go 1.21+ installed
2. **Keyper Binary**: Built server binary (`go build -o bin/server ./cmd/server`)
3. **Network Ports**: Ports 8080-8100 and 9080-9100 available
4. **System Tools**: `netstat`, `ps`, `time` commands available

## Test Configuration

### Default Settings
- **Base HTTP Port**: 8080
- **Base Raft Port**: 9080
- **Test Timeout**: 30 seconds per test
- **Node Count Range**: 1-10 nodes
- **Memory Threshold**: 100MB per node

### Customization
Edit the configuration section in each script to adjust:
```bash
# Configuration
BASE_HTTP_PORT=8080
BASE_RAFT_PORT=9080
MAX_NODES=10
MEMORY_LIMIT_MB=100
TEST_TIMEOUT=30
```

## Output Format

Each test provides:
- **Pass/Fail Status**: Clear success/failure indication
- **Performance Metrics**: Timing, memory usage, throughput numbers
- **Detailed Logs**: Error messages and diagnostic information
- **Summary Statistics**: Aggregated results across test runs

## Interpreting Results

### Performance Benchmarks
- **Startup Time**: < 3 seconds (Good), < 5 seconds (Acceptable)
- **Memory Usage**: < 50MB per node (Excellent), < 100MB (Good)
- **Connection Time**: < 2 seconds for 10 nodes (Good)
- **Request Throughput**: > 1000 req/sec (Good), > 500 req/sec (Acceptable)

### Success Criteria
- All basic functionality tests pass
- Memory usage within limits
- No memory leaks over extended runs
- Consistent performance across multiple test runs

## Troubleshooting

### Common Issues
1. **Port Conflicts**: Check if ports 8080-8100, 9080-9100 are available
2. **Build Failures**: Ensure `bin/server` binary exists and is current
3. **Permission Issues**: Ensure scripts have execute permissions
4. **Resource Limits**: Check system memory and file descriptor limits

### Debug Mode
Run tests with debug output:
```bash
DEBUG=1 ./test_startup_time.sh
```

## Integration with CI/CD

These tests can be integrated into continuous integration pipelines:
```bash
# Example CI script
#!/bin/bash
set -e
go build -o bin/server ./cmd/server
./performance/run_all_performance_tests.sh
echo "Performance tests completed successfully"
```
