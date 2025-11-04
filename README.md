# DKVSD - Distributed Key-Value Store

An implementation of an in-memory distributed key-value storage system written in Go. This project demonstrates a master-slave architecture with support for distributed data storage, replication, and cluster management.

## Project Overview

DKVSD is a distributed key-value store that provides:
- **Master-Slave Architecture**: Hierarchical cluster management with master coordination
- **Data Replication**: Configurable replica count for data redundancy
- **Distributed Storage**: Hash-based data distribution across cluster nodes
- **In-Memory Storage**: Fast data access with in-memory hash tables
- **Network Communication**: TCP-based communication between nodes
- **Cluster Management**: Dynamic node discovery and cluster formation

## Features

### Core Functionality
- ✅ **Master Node**: Coordinates cluster operations and manages slave nodes
- ✅ **Slave Nodes**: Store replicated data and respond to master commands
- ✅ **Consistent Hashing**: Distributed hash table for data partitioning
- ✅ **Replication**: Configurable number of data replicas across nodes
- ✅ **Remote RPC**: Custom remote procedure call mechanism for node communication

### Architecture Components
```
lib/
├── node.go               # Core node interface
├── local/                # Local node implementations
│   ├── master.go        # Master node logic
│   ├── slave.go         # Slave node logic
│   ├── cluster/         # Cluster management
│   ├── hashtable/       # Hash table storage
│   └── replicas/        # Replication logic
└── remote/              # Remote communication
    └── remote.go        # RPC implementation
```

## Getting Started

### Prerequisites
- Go 1.16 or higher
- Linux/Unix environment (for shell scripts)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/sada-02/Keyper
cd Keyper
```

2. Build the application:
```bash
go build -o dkvsd
```

### Basic Usage

#### Starting a Master Instance
```bash
./dkvsd -role master -replicas 3 -local localhost:5000 -remote ""
```

#### Adding Slave Nodes
```bash
# Start first slave
./dkvsd -role slave -local localhost:5001 -remote localhost:5000

# Start second slave
./dkvsd -role slave -local localhost:5002 -remote localhost:5000
```

### Command-Line Options

```
-role string
    Set node role: master, slave (default "master")

-local string
    Set the local address (default "localhost:5000")

-remote string
    Set the remote master address (default "localhost:5000")

-replicas int
    Number of replicas in cluster (default 1)

-v
    Display version information
```

## Running the Demo

The project includes a demo script that automatically starts a master-slave cluster:

```bash
chmod +x sample.sh
./sample.sh
```

### What `sample.sh` Demonstrates:
1. **Automatic Cleanup**: Kills any existing DKVSD instances
2. **Build Process**: Compiles the application from source
3. **Master Startup**: Launches a master node with 3 replicas on port 5000
4. **Slave Registration**: Starts 2 slave nodes on ports 5001 and 5002
5. **Cluster Formation**: All slaves automatically connect to the master
6. **Process Monitoring**: Displays PIDs and status of all running nodes

**Features Demonstrated:**
- ✓ Master-slave communication
- ✓ Automatic node discovery
- ✓ Multiple concurrent slaves
- ✓ Process lifecycle management
- ✓ Clean shutdown handling (Ctrl+C)

## Running Tests

The project includes a comprehensive test suite that validates all aspects of the system:

```bash
chmod +x tests.sh
./tests.sh
```

### Test Categories

#### 1. Basic Functionality Tests (5 tests)
- ✓ **Master Server Startup**: Verifies master node initialization and port binding
- ✓ **Single Slave Connection**: Tests basic master-slave connectivity
- ✓ **Multiple Slave Connections**: Validates 3+ simultaneous slave connections
- ✓ **Process Health Check**: Ensures all nodes remain running stably
- ✓ **Network Port Binding**: Confirms proper TCP port allocation

#### 2. Connection & Networking Tests (5 tests)
- ✓ **Different Port Range**: Tests flexibility in port configuration (7000+)
- ✓ **Sequential Slave Addition**: Validates slaves joining one at a time
- ✓ **Rapid Simultaneous Connections**: Stress tests concurrent slave connections (5 nodes)
- ✓ **Connection Stability**: Monitors sustained connections over 5 seconds
- ✓ **Clean Output Logging**: Verifies proper logging without errors

#### 3. Performance & Scalability Tests (5 tests)
- ✓ **Fast Startup**: Measures startup time (<3 seconds)
- ✓ **Scalability**: Tests system with 10 slave nodes
- ✓ **Memory Efficiency**: Monitors RAM usage (should be <100MB)
- ✓ **Port Reuse Capability**: Validates clean restart and port reuse
- ✓ **Long-term Stability**: Extended runtime test with multiple nodes

### Test Output
The test suite provides:
- Color-coded results (green for pass, red for fail)
- Per-category statistics
- Overall pass rate percentage
- Detailed diagnostic information
- Memory and performance metrics

Example output:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  BASIC FUNCTIONALITY TESTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✓ Master server startup [PASSED]
  ✓ Single slave connection [PASSED]
  ✓ Multiple slaves (3 nodes) [PASSED]
    → All 3 slaves connected successfully
  ...

Overall Statistics:
  Total Tests:      15
  Passed Tests:     15
  Failed Tests:     0
  Pass Rate:        100%
```

## Project Structure

```
.
├── main.go              # Application entry point
├── go.mod               # Go module dependencies
├── sample.sh            # Demo script for quick testing
├── tests.sh             # Comprehensive test suite
├── README.md            # This file
├── LICENSE              # MIT License
└── lib/                 # Core library code
    ├── node.go          # Node interface definition
    ├── local/           # Local node implementations
    │   ├── master.go    # Master node implementation
    │   ├── slave.go     # Slave node implementation
    │   ├── cluster/     # Cluster coordination
    │   ├── hashtable/   # Distributed hash table
    │   └── replicas/    # Data replication logic
    └── remote/          # Remote communication layer
        └── remote.go    # RPC implementation
```

## Technical Details

### Master Node
- Listens for incoming slave connections
- Manages cluster membership
- Coordinates data distribution
- Handles replica placement
- Monitors node health

### Slave Node
- Connects to master on startup
- Stores replicated data
- Responds to RPC requests
- Maintains local hash table
- Reports status to master

### Data Distribution
- Uses consistent hashing for key distribution
- Configurable replication factor
- Automatic replica placement across slaves
- Hash-based partitioning for load balancing

## Contributors
- Adesh Palker 23114004
- Kartik Sarda 23114047
- Nisarg Prajapati 231140073
- Nitin Agiwal 23114074
- Pradyuman Shekhawat 23115107
- Utkarsh Kumar 23114101