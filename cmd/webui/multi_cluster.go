package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"html/template"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Total number of shards the system can support (must match start-web-demo.sh --shard-count)
const MaxShardCount = 6

// ClusterStatus represents a Raft cluster
type ClusterStatus struct {
	ClusterID  string
	ShardID    int
	Nodes      []*NodeStatus
	LeaderNode string
	QuorumSize int
	Color      string
}

// MultiClusterUI manages the multi-cluster web interface
type MultiClusterUI struct {
	clusters      map[string]*ClusterStatus // clusterID -> ClusterStatus
	nodes         map[string]*NodeStatus
	requestLogs   []RequestLog
	clients       map[string]*AutoClient
	mu            sync.RWMutex
	projectRoot   string
	nextClusterID int
	nextShardID   int
	nextNodeNum   int
	nextHTTPPort  int
	nextRaftPort  int
}

func NewMultiClusterUI(projectRoot string) *MultiClusterUI {
	return &MultiClusterUI{
		clusters:      make(map[string]*ClusterStatus),
		nodes:         make(map[string]*NodeStatus),
		requestLogs:   make([]RequestLog, 0),
		clients:       make(map[string]*AutoClient),
		projectRoot:   projectRoot,
		nextClusterID: 3, // Already have cluster0, cluster1, cluster2
		nextShardID:   3,
		nextNodeNum:   10, // node1-9 already used
		nextHTTPPort:  8089,
		nextRaftPort:  9089,
	}
}

const multiClusterTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>Keyper Multi-Cluster Dashboard</title>
    <meta http-equiv="refresh" content="3">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%);
            color: #333;
            padding: 20px;
        }
        .container { max-width: 1800px; margin: 0 auto; }
        h1 {
            color: white;
            text-align: center;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            font-size: 2.5em;
        }
        .subtitle {
            color: rgba(255,255,255,0.9);
            text-align: center;
            margin-bottom: 30px;
            font-size: 1.1em;
        }

        /* Stats Bar */
        .stats-bar {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 15px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .stat-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #3b82f6;
        }
        .stat-label {
            color: #666;
            margin-top: 5px;
        }

        /* Main Grid */
        .main-grid {
            display: grid;
            grid-template-columns: 2.5fr 1fr;
            gap: 20px;
        }

        /* Clusters Section */
        .clusters-container {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .section-title {
            font-size: 1.8em;
            font-weight: bold;
            color: #1e3a8a;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #e5e7eb;
        }

        /* Cluster Card */
        .cluster-card {
            background: #f9fafb;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            border: 3px solid #e5e7eb;
        }
        .cluster-card.cluster-0 { border-color: #3b82f6; }
        .cluster-card.cluster-1 { border-color: #f59e0b; }
        .cluster-card.cluster-2 { border-color: #10b981; }

        .cluster-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e5e7eb;
        }
        .cluster-title {
            font-size: 1.5em;
            font-weight: bold;
        }
        .cluster-title.cluster-0 { color: #3b82f6; }
        .cluster-title.cluster-1 { color: #f59e0b; }
        .cluster-title.cluster-2 { color: #10b981; }

        .election-btn {
            background: #ef4444;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            font-weight: bold;
            cursor: pointer;
            transition: all 0.3s;
        }
        .election-btn:hover {
            background: #dc2626;
            transform: scale(1.05);
        }

        /* Nodes in Cluster */
        .cluster-nodes {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 15px;
        }
        .node-card {
            background: white;
            border-radius: 8px;
            padding: 15px;
            border-left: 5px solid #9ca3af;
            transition: all 0.2s;
        }
        .node-card.leader { border-left-color: #fbbf24; box-shadow: 0 0 10px rgba(251, 191, 36, 0.5); }
        .node-card:hover { transform: translateY(-3px); box-shadow: 0 4px 12px rgba(0,0,0,0.15); }

        .node-title {
            font-weight: bold;
            font-size: 1.1em;
            color: #1e3a8a;
            margin-bottom: 8px;
        }
        .status-badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
            margin-left: 8px;
        }
        .status-leader {
            background: #fbbf24;
            color: #78350f;
        }
        .status-follower {
            background: #3b82f6;
            color: white;
        }
        .status-candidate {
            background: #f97316;
            color: white;
        }
        .node-info {
            font-size: 0.9em;
            line-height: 1.8;
            color: #666;
        }
        .keys-badge {
            background: #3b82f6;
            color: white;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.85em;
            display: inline-block;
            margin-top: 5px;
        }

        /* Right Column */
        .right-column {
            display: flex;
            flex-direction: column;
            gap: 20px;
        }
        .section {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }

        /* Client Section */
        .client-item {
            background: #f9fafb;
            padding: 12px;
            margin: 8px 0;
            border-radius: 5px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-left: 4px solid #10b981;
        }
        .client-inactive { border-left-color: #9ca3af; opacity: 0.6; }
        .client-name {
            font-weight: bold;
            color: #1e3a8a;
            margin-bottom: 3px;
        }
        .client-stats {
            font-size: 0.85em;
            color: #666;
        }
        .btn {
            padding: 6px 12px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-weight: bold;
            transition: all 0.2s;
        }
        .btn-start { background: #10b981; color: white; }
        .btn-stop { background: #ef4444; color: white; }
        .btn-add { background: #3b82f6; color: white; padding: 12px 20px; width: 100%; font-size: 1em; }
        .btn:hover { transform: scale(1.05); }

        /* Logs */
        .logs-container {
            max-height: 300px;
            overflow-y: auto;
        }
        .log-entry {
            background: #f9fafb;
            border-left: 3px solid #3b82f6;
            padding: 10px;
            margin: 5px 0;
            border-radius: 3px;
            font-size: 0.85em;
        }
        .log-success { border-left-color: #10b981; }
        .log-error { border-left-color: #ef4444; }
        .log-time {
            color: #9ca3af;
            font-size: 0.85em;
        }

        .add-cluster-btn {
            background: linear-gradient(135deg, #3b82f6, #1e3a8a);
            color: white;
            border: none;
            padding: 15px 30px;
            border-radius: 8px;
            font-size: 1.1em;
            font-weight: bold;
            cursor: pointer;
            width: 100%;
            margin-top: 20px;
            transition: all 0.3s;
        }
        .add-cluster-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(59, 130, 246, 0.4);
        }

        .timestamp {
            text-align: center;
            color: white;
            margin-top: 20px;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏛️ Keyper Multi-Cluster Dashboard</h1>
        <div class="subtitle">
            Independent Raft Clusters with Distributed Key Sharding
        </div>

        <!-- Stats Bar -->
        <div class="stats-bar">
            <div class="stat-card">
                <div class="stat-value">{{.TotalClusters}}</div>
                <div class="stat-label">Clusters</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.TotalNodes}}</div>
                <div class="stat-label">Total Nodes</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.TotalLeaders}}</div>
                <div class="stat-label">Leaders</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.ActiveClients}}</div>
                <div class="stat-label">Active Clients</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.TotalRequests}}</div>
                <div class="stat-label">Total Requests</div>
            </div>
        </div>

        <!-- Main Grid -->
        <div class="main-grid">
            <!-- Left: Clusters -->
            <div class="clusters-container">
                <div class="section-title">🗳️ Raft Clusters ({{len .Clusters}})</div>

                {{range .Clusters}}
                <div class="cluster-card cluster-{{.ShardID}}">
                    <div class="cluster-header">
                        <div>
                            <span class="cluster-title cluster-{{.ShardID}}">
                                {{.ClusterID}} (Shard {{.ShardID}})
                            </span>
                            <div style="color: #666; font-size: 0.9em; margin-top: 5px;">
                                {{len .Nodes}} nodes | Leader: {{.LeaderNode}} | Quorum: {{.QuorumSize}}/{{len .Nodes}}
                            </div>
                        </div>
                        <div style="display: flex; gap: 10px; align-items: center;">
                            <button class="election-btn" onclick="conductElection('{{.ClusterID}}')">
                                🗳️ Graceful Election
                            </button>
                            <button class="election-btn" style="background: #3b82f6;" onclick="showElectionDetails('{{.ClusterID}}')">
                                📊 Details
                            </button>
                        </div>
                    </div>

                    <div class="cluster-nodes">
                        {{range .Nodes}}
                        <div class="node-card {{if .IsLeader}}leader{{end}}" id="node-{{.NodeID}}">
                            <div class="node-title">
                                {{.NodeID}}
                                <span class="status-badge status-{{if .IsLeader}}leader{{else}}{{.RaftState}}{{end}}">
                                    {{if .IsLeader}}👑 LEADER{{else}}{{.RaftState}}{{end}}
                                </span>
                            </div>
                            <div class="node-info">
                                <div>HTTP: {{.HTTPAddr}}</div>
                                <div>Raft: {{.RaftAddr}}</div>
                                <div>PID: {{.PID}}</div>
                                <span class="keys-badge">{{.NumKeys}} keys</span>
                            </div>
                        </div>
                        {{end}}
                    </div>
                </div>
                {{end}}

                <button class="add-cluster-btn" onclick="addCluster()">
                    ➕ Add New Cluster (3 nodes)
                </button>
            </div>

            <!-- Right: Clients and Logs -->
            <div class="right-column">
                <!-- Clients -->
                <div class="section">
                    <div class="section-title" style="font-size: 1.3em;">🎮 Automated Clients</div>
                    
                    {{range .Clients}}
                    <div class="client-item {{if not .Active}}client-inactive{{end}}">
                        <div class="client-info">
                            <div class="client-name">{{.ID}}</div>
                            <div class="client-stats">
                                {{if .Active}}
                                ✅ {{.Counter}} requests | {{.KeyPrefix}}:*
                                {{else}}
                                ⏸️ Stopped
                                {{end}}
                            </div>
                        </div>
                        {{if .Active}}
                        <button class="btn btn-stop" onclick="stopClient('{{.ID}}')">Stop</button>
                        {{else}}
                        <button class="btn btn-start" onclick="startClient('{{.ID}}')">Start</button>
                        {{end}}
                    </div>
                    {{end}}

                    <button class="btn btn-add" onclick="addClient()">➕ Add New Client</button>
                </div>

                <!-- Request Logs -->
                <div class="section">
                    <div class="section-title" style="font-size: 1.3em;">📋 Recent Requests</div>
                    <div class="logs-container">
                        {{range .RecentLogs}}
                        <div class="log-entry {{if eq .Status 200}}log-success{{else if eq .Status 204}}log-success{{else}}log-error{{end}}">
                            <div style="font-weight: bold;">
                                {{.ClientID}} → {{.Method}} {{.Key}} @ {{.TargetNode}}
                                <span class="log-time">{{.Timestamp.Format "15:04:05"}}</span>
                            </div>
                            <div style="color: #666; font-size: 0.9em;">
                                {{if eq .Status 200}}✅{{else if eq .Status 204}}✅{{else}}❌{{end}}
                                Status: {{.Status}}
                                {{if .Error}}| Error: {{.Error}}{{end}}
                            </div>
                        </div>
                        {{end}}
                    </div>
                </div>
            </div>
        </div>

        <div class="timestamp">
            Last updated: {{.Timestamp}}
        </div>
    </div>

    <script>
        function addClient() {
            fetch('/api/client/add', { method: 'POST' })
                .then(() => location.reload());
        }

        function startClient(id) {
            fetch('/api/client/start?id=' + id, { method: 'POST' })
                .then(() => location.reload());
        }

        function stopClient(id) {
            fetch('/api/client/stop?id=' + id, { method: 'POST' })
                .then(() => location.reload());
        }

        function addCluster() {
            if (confirm('Add a new cluster with 3 nodes?')) {
                fetch('/api/cluster/add', { method: 'POST' })
                    .then(res => res.json())
                    .then(data => {
                        alert('Cluster ' + data.cluster_id + ' created with nodes: ' + data.nodes.join(', '));
                        location.reload();
                    });
            }
        }

        function conductElection(clusterID) {
            console.log('conductElection called with clusterID:', clusterID);
            
            if (confirm('Trigger graceful leader stepdown in ' + clusterID + '?\n\nThe current leader will step down gracefully (no process kill), and a new election will occur among the remaining nodes.')) {
                console.log('User confirmed, making API call...');
                
                fetch('/api/cluster/election?cluster=' + clusterID, { method: 'POST' })
                    .then(res => {
                        console.log('Response received:', res.status, res.ok);
                        
                        if (!res.ok) {
                            // Try to parse error as JSON
                            return res.json().then(errData => {
                                console.error('Error data:', errData);
                                throw new Error(errData.error || 'HTTP error ' + res.status);
                            }).catch((jsonErr) => {
                                console.error('JSON parse error:', jsonErr);
                                // If JSON parsing fails, try text
                                return res.text().then(text => {
                                    console.error('Error text:', text);
                                    throw new Error(text || 'HTTP error ' + res.status);
                                });
                            });
                        }
                        return res.json();
                    })
                    .then(data => {
                        console.log('Success data:', data);
                        
                        if (data.success === false) {
                            throw new Error(data.error || 'Election failed');
                        }
                        
                        let msg = 'Election completed!\n\n';
                        msg += 'Old Leader: ' + data.old_leader + '\n';
                        if (data.election_info && data.election_info.new_leader) {
                            msg += 'New Leader: ' + data.election_info.new_leader + '\n';
                            msg += 'Term: ' + data.election_info.term;
                        }
                        alert(msg);
                        setTimeout(() => location.reload(), 1000);
                    })
                    .catch(err => {
                        console.error('Final error:', err);
                        alert('Election failed: ' + err.message);
                    });
            } else {
                console.log('User cancelled election');
            }
        }

        function showElectionDetails(clusterID) {
            console.log('showElectionDetails called with clusterID:', clusterID);
            
            // Fetch detailed election status for all nodes in the cluster
            fetch('/api/cluster/status?cluster=' + clusterID)
                .then(res => res.json())
                .then(data => {
                    let msg = 'Election Details for ' + clusterID + '\n\n';
                    msg += 'Cluster Size: ' + data.cluster_size + '\n';
                    msg += 'Quorum Needed: ' + data.quorum_size + '\n';
                    msg += 'Current Term: ' + data.term + '\n';
                    msg += 'Current Leader: ' + data.leader + '\n\n';
                    msg += 'Nodes:\n';
                    if (data.nodes) {
                        data.nodes.forEach(node => {
                            msg += '  • ' + node.node_id + ': ' + node.state;
                            if (node.is_leader) msg += ' (LEADER)';
                            msg += '\n';
                        });
                    }
                    alert(msg);
                })
                .catch(err => {
                    console.error('Error fetching election details:', err);
                    alert('Failed to fetch election details: ' + err.message);
                });
        }
    </script>
</body>
</html>
`

// Dashboard data structure
type DashboardData struct {
	Clusters      []*ClusterStatus
	Clients       map[string]*AutoClient
	RecentLogs    []RequestLog
	TotalClusters int
	TotalNodes    int
	TotalLeaders  int
	ActiveClients int
	TotalRequests int
	Timestamp     string
}

func (ui *MultiClusterUI) dashboardHandler(w http.ResponseWriter, r *http.Request) {
	// Prevent browser caching
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	ui.mu.RLock()
	defer ui.mu.RUnlock()

	// Count stats
	totalLeaders := 0
	activeClients := 0
	totalRequests := 0

	for _, cluster := range ui.clusters {
		for _, node := range cluster.Nodes {
			if node.IsLeader {
				totalLeaders++
			}
		}
	}

	for _, client := range ui.clients {
		if client.Active {
			activeClients++
		}
		totalRequests += client.Counter
	}

	// Get recent logs (last 15)
	recentLogs := ui.requestLogs
	if len(recentLogs) > 15 {
		recentLogs = recentLogs[len(recentLogs)-15:]
	}

	// Convert clusters map to slice and sort by cluster ID
	clustersList := make([]*ClusterStatus, 0, len(ui.clusters))
	for _, cluster := range ui.clusters {
		clustersList = append(clustersList, cluster)
	}

	// Sort clusters by cluster ID to maintain consistent order
	sort.Slice(clustersList, func(i, j int) bool {
		return clustersList[i].ClusterID < clustersList[j].ClusterID
	})

	data := DashboardData{
		Clusters:      clustersList,
		Clients:       ui.clients,
		RecentLogs:    recentLogs,
		TotalClusters: len(ui.clusters),
		TotalNodes:    len(ui.nodes),
		TotalLeaders:  totalLeaders,
		ActiveClients: activeClients,
		TotalRequests: totalRequests,
		Timestamp:     time.Now().Format("2006-01-02 15:04:05"),
	}

	tmpl := template.Must(template.New("dashboard").Parse(multiClusterTemplate))
	tmpl.Execute(w, data)
}

// Refresh all nodes - organizes them into clusters
func (ui *MultiClusterUI) refreshAllNodes() {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	// Clear existing
	ui.clusters = make(map[string]*ClusterStatus)
	ui.nodes = make(map[string]*NodeStatus)

	// Scan for all running nodes by checking PID files
	logDir := filepath.Join(ui.projectRoot, "logs")
	files, err := os.ReadDir(logDir)
	if err != nil {
		// If logs dir doesn't exist, fall back to hardcoded clusters
		ui.refreshHardcodedClusters()
		return
	}

	// Map to collect nodes by cluster
	clusterNodes := make(map[string][]struct {
		nodeID   string
		httpPort int
		raftPort int
	})

	// Scan PID files to discover running nodes
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".pid") {
			continue
		}

		// Extract node ID from filename (e.g., "cluster0-node1.pid" -> "cluster0-node1")
		nodeID := strings.TrimSuffix(file.Name(), ".pid")

		// Skip non-cluster nodes
		if !strings.Contains(nodeID, "cluster") {
			continue
		}

		// Extract cluster ID (e.g., "cluster0-node1" -> "cluster0")
		parts := strings.Split(nodeID, "-")
		if len(parts) < 2 {
			continue
		}
		clusterID := parts[0]

		// Try to determine ports from node ID
		// Format: clusterX-nodeY where node Y is at port 8080 + (Y-1) for cluster0, etc.
		var httpPort, raftPort int

		// Parse node number from node ID
		nodeNumStr := strings.TrimPrefix(parts[len(parts)-1], "node")
		nodeNum, err := strconv.Atoi(nodeNumStr)
		if err != nil {
			continue
		}

		// Calculate ports based on node number
		// cluster0 nodes: 8080-8082 (nodes 1-3)
		// cluster1 nodes: 8083-8085 (nodes 4-6)
		// cluster2 nodes: 8086-8088 (nodes 7-9)
		// cluster3 nodes: 8089-8091 (nodes 10-12)
		httpPort = 8080 + (nodeNum - 1)
		raftPort = 9080 + (nodeNum - 1)

		// Verify the PID file exists and the process is actually running
		pidFile := filepath.Join(logDir, file.Name())
		pidBytes, err := os.ReadFile(pidFile)
		if err != nil {
			continue
		}

		pidStr := strings.TrimSpace(string(pidBytes))
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			continue
		}

		// Check if process is actually running by checking /proc/<pid>
		procPath := fmt.Sprintf("/proc/%d", pid)
		if _, err := os.Stat(procPath); err == nil {
			// Process is running, add it to the list
			clusterNodes[clusterID] = append(clusterNodes[clusterID], struct {
				nodeID   string
				httpPort int
				raftPort int
			}{nodeID, httpPort, raftPort})
		}
		// If process not running, skip it (stale PID file will be ignored)
	}

	// If no dynamic clusters found, use hardcoded ones
	if len(clusterNodes) == 0 {
		ui.refreshHardcodedClusters()
		return
	}

	// Create clusters from discovered nodes
	colors := []string{"#3b82f6", "#f59e0b", "#10b981", "#8b5cf6", "#ec4899", "#f97316"}
	shardID := 0

	// Sort cluster IDs for consistent ordering
	clusterIDs := make([]string, 0, len(clusterNodes))
	for clusterID := range clusterNodes {
		clusterIDs = append(clusterIDs, clusterID)
	}
	sort.Strings(clusterIDs)

	for _, clusterID := range clusterIDs {
		nodes := clusterNodes[clusterID]

		// Extract shard ID from cluster name (e.g., "cluster0" -> 0)
		shardNumStr := strings.TrimPrefix(clusterID, "cluster")
		if shardNum, err := strconv.Atoi(shardNumStr); err == nil {
			shardID = shardNum
		}

		color := colors[shardID%len(colors)]

		cluster := &ClusterStatus{
			ClusterID: clusterID,
			ShardID:   shardID,
			Nodes:     make([]*NodeStatus, 0),
			Color:     color,
		}

		for _, nodeConfig := range nodes {
			nodeStatus := ui.fetchNodeStatus(nodeConfig.nodeID, nodeConfig.httpPort, nodeConfig.raftPort)
			if nodeStatus != nil {
				cluster.Nodes = append(cluster.Nodes, nodeStatus)
				ui.nodes[nodeConfig.nodeID] = nodeStatus

				if nodeStatus.IsLeader {
					cluster.LeaderNode = nodeConfig.nodeID
				}
			}
		}

		// Calculate quorum: (n/2) + 1
		cluster.QuorumSize = (len(cluster.Nodes) / 2) + 1

		ui.clusters[clusterID] = cluster
	}
}

// refreshHardcodedClusters is the fallback for when no dynamic discovery is possible
func (ui *MultiClusterUI) refreshHardcodedClusters() {
	// Define the 9 nodes across 3 clusters (original hardcoded config)
	clusterConfigs := map[string][]struct {
		nodeID   string
		httpPort int
		raftPort int
	}{
		"cluster0": {
			{"cluster0-node1", 8080, 9080},
			{"cluster0-node2", 8081, 9081},
			{"cluster0-node3", 8082, 9082},
		},
		"cluster1": {
			{"cluster1-node4", 8083, 9083},
			{"cluster1-node5", 8084, 9084},
			{"cluster1-node6", 8085, 9085},
		},
		"cluster2": {
			{"cluster2-node7", 8086, 9086},
			{"cluster2-node8", 8087, 9087},
			{"cluster2-node9", 8088, 9088},
		},
	}

	shardID := 0
	colors := []string{"#3b82f6", "#f59e0b", "#10b981"}

	for clusterID, nodes := range clusterConfigs {
		cluster := &ClusterStatus{
			ClusterID: clusterID,
			ShardID:   shardID,
			Nodes:     make([]*NodeStatus, 0),
			Color:     colors[shardID],
		}

		for _, nodeConfig := range nodes {
			nodeStatus := ui.fetchNodeStatus(nodeConfig.nodeID, nodeConfig.httpPort, nodeConfig.raftPort)
			if nodeStatus != nil {
				cluster.Nodes = append(cluster.Nodes, nodeStatus)
				ui.nodes[nodeConfig.nodeID] = nodeStatus

				if nodeStatus.IsLeader {
					cluster.LeaderNode = nodeConfig.nodeID
				}
			}
		}

		// Calculate quorum: (n/2) + 1
		cluster.QuorumSize = (len(cluster.Nodes) / 2) + 1

		ui.clusters[clusterID] = cluster
		shardID++
	}
}

func (ui *MultiClusterUI) fetchNodeStatus(nodeID string, httpPort, raftPort int) *NodeStatus {
	client := &http.Client{Timeout: 1 * time.Second}

	// Fetch /v1/status
	resp, err := client.Get(fmt.Sprintf("http://localhost:%d/v1/status", httpPort))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var status struct {
		NodeID    string `json:"node_id"`
		IsLeader  bool   `json:"is_leader"`
		RaftState string `json:"raft_state"`
		NumKeys   int    `json:"num_keys"`
	}
	json.NewDecoder(resp.Body).Decode(&status)

	// Get PID
	pidFile := fmt.Sprintf("logs/%s.pid", nodeID)
	pidBytes, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(string(bytes.TrimSpace(pidBytes)))

	return &NodeStatus{
		NodeID:      nodeID,
		HTTPAddr:    fmt.Sprintf(":%d", httpPort),
		RaftAddr:    fmt.Sprintf("127.0.0.1:%d", raftPort),
		IsLeader:    status.IsLeader,
		RaftState:   status.RaftState,
		Keys:        make(map[string]string),
		NumKeys:     status.NumKeys,
		LastUpdated: time.Now(),
		Healthy:     true,
		PID:         pid,
	}
}

// API Handlers
func (ui *MultiClusterUI) addClusterHandler(w http.ResponseWriter, r *http.Request) {
	ui.mu.Lock()
	clusterID := fmt.Sprintf("cluster%d", ui.nextClusterID)
	shardID := ui.nextShardID
	startNodeNum := ui.nextNodeNum
	startHTTPPort := ui.nextHTTPPort
	startRaftPort := ui.nextRaftPort

	ui.nextClusterID++
	ui.nextShardID++
	ui.nextNodeNum += 3
	ui.nextHTTPPort += 3
	ui.nextRaftPort += 3
	ui.mu.Unlock()

	// Start 3 nodes for this cluster
	nodes := []string{}
	for i := 0; i < 3; i++ {
		nodeNum := startNodeNum + i
		nodeID := fmt.Sprintf("%s-node%d", clusterID, nodeNum)
		httpPort := startHTTPPort + i
		raftPort := startRaftPort + i

		// Start node in background
		go ui.startNode(nodeID, httpPort, raftPort, shardID, i == 0, startHTTPPort)
		nodes = append(nodes, nodeID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cluster_id": clusterID,
		"shard_id":   shardID,
		"nodes":      nodes,
		"message":    "Cluster created successfully",
	})
}

func (ui *MultiClusterUI) startNode(nodeID string, httpPort, raftPort, shardID int, isBootstrap bool, joinPort int) {
	args := []string{
		fmt.Sprintf("--node-id=%s", nodeID),
		fmt.Sprintf("--http-addr=:%d", httpPort),
		fmt.Sprintf("--raft-addr=127.0.0.1:%d", raftPort),
		fmt.Sprintf("--data-dir=%s-data", nodeID),
		"--enable-raft",
		fmt.Sprintf("--shard-count=%d", MaxShardCount), // Use global max shard count
		fmt.Sprintf("--assigned-shards=%d", shardID),
	}

	if !isBootstrap {
		args = append(args, fmt.Sprintf("--join=http://localhost:%d", joinPort))
	}

	cmd := exec.Command(filepath.Join(ui.projectRoot, "bin", "server"), args...)

	// Setup logging
	logFile, err := os.Create(filepath.Join(ui.projectRoot, "logs", fmt.Sprintf("%s.log", nodeID)))
	if err != nil {
		log.Printf("Failed to create log file for %s: %v", nodeID, err)
		return
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start node %s: %v", nodeID, err)
		return
	}

	// Save PID
	pidFile := filepath.Join(ui.projectRoot, "logs", fmt.Sprintf("%s.pid", nodeID))
	os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644)

	log.Printf("Started node %s (PID: %d, HTTP: %d, Raft: %d)", nodeID, cmd.Process.Pid, httpPort, raftPort)
}

func (ui *MultiClusterUI) conductElectionHandler(w http.ResponseWriter, r *http.Request) {
	clusterID := r.URL.Query().Get("cluster")

	ui.mu.RLock()
	cluster, exists := ui.clusters[clusterID]
	ui.mu.RUnlock()

	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Cluster not found",
		})
		return
	}

	// Find current leader
	var leaderPort int
	var leaderNodeID string
	for _, node := range cluster.Nodes {
		if node.IsLeader {
			// Extract port from HTTPAddr (format ":8080")
			fmt.Sscanf(node.HTTPAddr, ":%d", &leaderPort)
			leaderNodeID = node.NodeID
			break
		}
	}

	if leaderPort == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "No leader found in cluster",
		})
		return
	}

	// Trigger graceful leader stepdown via new election API
	electionReq := map[string]string{"method": "stepdown"}
	reqBody, _ := json.Marshal(electionReq)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(
		fmt.Sprintf("http://localhost:%d/v1/election/trigger", leaderPort),
		"application/json",
		bytes.NewBuffer(reqBody),
	)

	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Failed to trigger election: %v", err),
		})
		return
	}
	defer resp.Body.Close()

	// Check if the election trigger was successful
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   fmt.Sprintf("Election trigger failed: %s", string(bodyBytes)),
		})
		return
	}

	// Wait for election to complete
	time.Sleep(2 * time.Second)

	// Get election status from cluster nodes
	electionInfo := make(map[string]interface{})
	for _, node := range cluster.Nodes {
		var port int
		fmt.Sscanf(node.HTTPAddr, ":%d", &port)

		statusResp, err := client.Get(fmt.Sprintf("http://localhost:%d/v1/election/status", port))
		if err == nil {
			var status map[string]interface{}
			json.NewDecoder(statusResp.Body).Decode(&status)
			statusResp.Body.Close()

			if status["is_leader"] == true {
				electionInfo["new_leader"] = status["node_id"]
				electionInfo["term"] = status["term"]
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"message":       fmt.Sprintf("Leader %s stepped down gracefully, new election completed", leaderNodeID),
		"old_leader":    leaderNodeID,
		"election_info": electionInfo,
	})
}

// Client management handlers (reuse from existing code)
func (ui *MultiClusterUI) addClientHandler(w http.ResponseWriter, r *http.Request) {
	ui.mu.Lock()
	defer ui.mu.Unlock()

	clientNum := len(ui.clients) + 1
	clientID := fmt.Sprintf("client%d", clientNum)

	client := &AutoClient{
		ID:          clientID,
		Active:      true,
		RequestRate: 2 * time.Second,
		StopChan:    make(chan bool),
		KeyPrefix:   fmt.Sprintf("c%d", clientNum),
		Counter:     0,
	}

	ui.clients[clientID] = client
	go ui.runClient(client)

	w.WriteHeader(http.StatusOK)
}

func (ui *MultiClusterUI) startClientHandler(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("id")

	ui.mu.Lock()
	defer ui.mu.Unlock()

	client, exists := ui.clients[clientID]
	if !exists {
		http.Error(w, "Client not found", http.StatusNotFound)
		return
	}

	if !client.Active {
		client.Active = true
		client.StopChan = make(chan bool)
		go ui.runClient(client)
	}

	w.WriteHeader(http.StatusOK)
}

func (ui *MultiClusterUI) stopClientHandler(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("id")

	ui.mu.Lock()
	defer ui.mu.Unlock()

	client, exists := ui.clients[clientID]
	if !exists {
		http.Error(w, "Client not found", http.StatusNotFound)
		return
	}

	if client.Active {
		client.Active = false
		close(client.StopChan)
	}

	w.WriteHeader(http.StatusOK)
}

func (ui *MultiClusterUI) clusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	clusterID := r.URL.Query().Get("cluster")

	ui.mu.RLock()
	cluster, exists := ui.clusters[clusterID]
	ui.mu.RUnlock()

	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Cluster not found",
		})
		return
	}

	// Calculate quorum
	quorumSize := (len(cluster.Nodes) / 2) + 1

	// Find current leader and term
	var leaderNodeID string
	var currentTerm string

	for _, node := range cluster.Nodes {
		if node.IsLeader {
			leaderNodeID = node.NodeID
			// Get the term from the first leader found
			var port int
			fmt.Sscanf(node.HTTPAddr, ":%d", &port)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Get(fmt.Sprintf("http://localhost:%d/v1/election/status", port))
			if err == nil {
				var status map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&status)
				resp.Body.Close()
				if term, ok := status["term"].(string); ok {
					currentTerm = term
				}
			}
			break
		}
	}

	// Collect node details
	nodes := make([]map[string]interface{}, 0)
	for _, node := range cluster.Nodes {
		nodes = append(nodes, map[string]interface{}{
			"node_id":   node.NodeID,
			"http_addr": node.HTTPAddr,
			"raft_addr": node.RaftAddr,
			"state":     node.RaftState,
			"is_leader": node.IsLeader,
			"num_keys":  node.NumKeys,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":      true,
		"cluster_id":   clusterID,
		"cluster_size": len(cluster.Nodes),
		"quorum_size":  quorumSize,
		"leader":       leaderNodeID,
		"term":         currentTerm,
		"nodes":        nodes,
	})
}

func (ui *MultiClusterUI) runClient(client *AutoClient) {
	ticker := time.NewTicker(client.RequestRate)
	defer ticker.Stop()

	for {
		select {
		case <-client.StopChan:
			return
		case <-ticker.C:
			ui.generateRequest(client)
		}
	}
}

func (ui *MultiClusterUI) generateRequest(client *AutoClient) {
	client.Counter++
	key := fmt.Sprintf("%s:key%d", client.KeyPrefix, client.Counter)
	value := fmt.Sprintf("value%d_t%d", client.Counter, time.Now().Unix())

	// Determine which cluster owns this key (shard routing)
	// Hash the key to get a cluster index (0 to numClusters-1)
	ui.mu.RLock()
	numClusters := len(ui.clusters)
	if numClusters == 0 {
		ui.mu.RUnlock()
		return
	}

	// Get sorted list of cluster IDs for consistent routing
	clusterIDs := make([]string, 0, numClusters)
	for clusterID := range ui.clusters {
		clusterIDs = append(clusterIDs, clusterID)
	}
	sort.Strings(clusterIDs)

	// Hash key to determine cluster index
	hash := crc32.ChecksumIEEE([]byte(key))
	clusterIndex := int(hash % uint32(numClusters))
	targetClusterID := clusterIDs[clusterIndex]

	// Get the leader of the target cluster
	cluster := ui.clusters[targetClusterID]
	var targetNode *NodeStatus
	for _, node := range cluster.Nodes {
		if node.IsLeader && node.Healthy {
			targetNode = node
			break
		}
	}
	ui.mu.RUnlock()

	if targetNode == nil {
		return
	}

	// Extract port
	var port int
	fmt.Sscanf(targetNode.HTTPAddr, ":%d", &port)

	// Random operation: 80% PUT, 10% GET, 10% DELETE
	op := rand.Intn(100)
	var method, endpoint string
	var body io.Reader

	if op < 80 {
		method = "PUT"
		endpoint = fmt.Sprintf("http://localhost:%d/v1/keys/%s", port, key)
		body = bytes.NewBufferString(value)
	} else if op < 90 {
		method = "GET"
		endpoint = fmt.Sprintf("http://localhost:%d/v1/keys/%s", port, key)
	} else {
		method = "DELETE"
		endpoint = fmt.Sprintf("http://localhost:%d/v1/keys/%s", port, key)
	}

	req, _ := http.NewRequest(method, endpoint, body)
	httpClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Do(req)

	logEntry := RequestLog{
		Timestamp:  time.Now(),
		Method:     method,
		Key:        key,
		Value:      value,
		TargetNode: targetNode.NodeID,
		ClientID:   client.ID,
	}

	if err != nil {
		logEntry.Error = err.Error()
		logEntry.Status = 500
	} else {
		logEntry.Status = resp.StatusCode
		resp.Body.Close()
	}

	ui.mu.Lock()
	ui.requestLogs = append(ui.requestLogs, logEntry)
	if len(ui.requestLogs) > 100 {
		ui.requestLogs = ui.requestLogs[len(ui.requestLogs)-100:]
	}
	ui.mu.Unlock()
}

func RunMultiClusterUI(projectRoot string, port int) {
	ui := NewMultiClusterUI(projectRoot)

	// Start background refresh
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ui.refreshAllNodes()
		}
	}()

	// Setup routes
	http.HandleFunc("/", ui.dashboardHandler)
	http.HandleFunc("/api/client/add", ui.addClientHandler)
	http.HandleFunc("/api/client/start", ui.startClientHandler)
	http.HandleFunc("/api/client/stop", ui.stopClientHandler)
	http.HandleFunc("/api/cluster/add", ui.addClusterHandler)
	http.HandleFunc("/api/cluster/election", ui.conductElectionHandler)
	http.HandleFunc("/api/cluster/status", ui.clusterStatusHandler)

	addr := fmt.Sprintf(":%d", port)
	log.Printf("🌐 Multi-Cluster Dashboard starting on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
