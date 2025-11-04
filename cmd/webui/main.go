package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// NodeStatus represents the status of a Keyper node
type NodeStatus struct {
	NodeID      string            `json:"node_id"`
	HTTPAddr    string            `json:"http_addr"`
	RaftAddr    string            `json:"raft_addr"`
	IsLeader    bool              `json:"is_leader"`
	RaftState   string            `json:"raft_state"`
	Shards      []ShardInfo       `json:"shards"`
	Keys        map[string]string `json:"keys"`
	NumKeys     int               `json:"num_keys"`
	LastUpdated time.Time         `json:"last_updated"`
	Healthy     bool              `json:"healthy"`
	PID         int               `json:"pid"`
}

// ShardInfo represents shard-level information
type ShardInfo struct {
	ShardID   string `json:"shard_id"`
	RaftState string `json:"raft_state"`
	Leader    string `json:"leader"`
	ReadOnly  bool   `json:"read_only"`
}

// RequestLog represents a client request and response
type RequestLog struct {
	Timestamp  time.Time `json:"timestamp"`
	Method     string    `json:"method"`
	Key        string    `json:"key"`
	Value      string    `json:"value"`
	TargetNode string    `json:"target_node"`
	Status     int       `json:"status"`
	Response   string    `json:"response"`
	Error      string    `json:"error"`
	ClientID   string    `json:"client_id"`
}

// Client represents an automated client
type AutoClient struct {
	ID          string
	Active      bool
	RequestRate time.Duration
	StopChan    chan bool
	KeyPrefix   string
	Counter     int
}

// NodeConfig represents a node configuration
type NodeConfig struct {
	NodeID   string `json:"node_id"`
	HTTPPort int    `json:"http_port"`
	RaftPort int    `json:"raft_port"`
	Running  bool   `json:"running"`
	PID      int    `json:"pid"`
}

// ClusterInfo represents a cluster of nodes
type ClusterInfo struct {
	ClusterID string
	ShardID   int
	Nodes     []*NodeStatus
	Leader    *NodeStatus
	StartPort int
}

// WebUI manages the web interface
type WebUI struct {
	nodes          map[string]*NodeStatus
	nodeConfigs    map[string]*NodeConfig
	clusters       map[string]*ClusterInfo
	requestLogs    []RequestLog
	clients        map[string]*AutoClient
	mu             sync.RWMutex
	projectRoot    string
	nextClusterNum int
	nextNodeNum    int
	nextHTTPPort   int
	nextRaftPort   int
}

func NewWebUI(projectRoot string) *WebUI {
	return &WebUI{
		nodes:          make(map[string]*NodeStatus),
		nodeConfigs:    make(map[string]*NodeConfig),
		clusters:       make(map[string]*ClusterInfo),
		requestLogs:    make([]RequestLog, 0),
		clients:        make(map[string]*AutoClient),
		projectRoot:    projectRoot,
		nextClusterNum: 3,  // Start from cluster3 (0, 1, 2 already exist)
		nextNodeNum:    10, // Start from node10 (1-9 already used)
		nextHTTPPort:   8089,
		nextRaftPort:   9089,
	}
}

const dashboardTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>Keyper Distributed System Dashboard</title>
    <meta http-equiv="refresh" content="3">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
            padding: 20px;
        }
        .container { max-width: 1600px; margin: 0 auto; }
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
        
        /* Grid Layout */
        .dashboard-grid {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }
        
        /* Sections */
        .section {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .section-title {
            font-size: 1.5em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #f0f0f0;
        }
        
        /* Nodes Grid */
        .nodes-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 15px;
        }
        .node-card {
            background: #f9fafb;
            border-radius: 8px;
            padding: 15px;
            border-left: 4px solid #667eea;
            transition: transform 0.2s;
        }
        .node-card:hover { transform: translateX(5px); }
        .node-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .node-title {
            font-weight: bold;
            font-size: 1.2em;
            color: #667eea;
        }
        .status-badge {
            padding: 4px 12px;
            border-radius: 15px;
            font-size: 0.85em;
            font-weight: bold;
        }
        .status-leader { background: #10b981; color: white; }
        .status-follower { background: #3b82f6; color: white; }
        .status-offline { background: #ef4444; color: white; }
        .node-info { font-size: 0.9em; line-height: 1.8; color: #666; }
        .keys-count {
            background: #667eea;
            color: white;
            padding: 5px 10px;
            border-radius: 5px;
            display: inline-block;
            margin-top: 5px;
            font-weight: bold;
        }
        
        /* Clients Section */
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
        .client-info { flex: 1; }
        .client-name {
            font-weight: bold;
            color: #667eea;
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
        .btn-add { background: #667eea; color: white; padding: 10px 20px; width: 100%; }
        .btn:hover { transform: scale(1.05); }
        
        /* Request Logs */
        .logs-container {
            max-height: 400px;
            overflow-y: auto;
        }
        .log-entry {
            background: #f9fafb;
            border-left: 3px solid #667eea;
            padding: 10px;
            margin: 5px 0;
            border-radius: 3px;
            font-size: 0.9em;
        }
        .log-success { border-left-color: #10b981; }
        .log-error { border-left-color: #ef4444; }
        .log-header {
            display: flex;
            justify-content: space-between;
            font-weight: bold;
            margin-bottom: 5px;
        }
        .log-details { color: #666; font-size: 0.9em; }
        
        /* Node Management */
        .node-mgmt {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }
        .node-mgmt input {
            flex: 1;
            padding: 8px;
            border: 2px solid #e5e7eb;
            border-radius: 5px;
        }
        
        /* Stats Bar */
        .stats-bar {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
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
            color: #667eea;
        }
        .stat-label {
            color: #666;
            margin-top: 5px;
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
        <h1>🚀 Keyper Distributed System Dashboard</h1>
        <div class="subtitle">Real-time monitoring with automated clients</div>
        
        <!-- Stats Bar -->
        <div class="stats-bar">
            <div class="stat-card">
                <div class="stat-value">{{.Stats.TotalNodes}}</div>
                <div class="stat-label">Active Nodes</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Stats.TotalShards}}</div>
                <div class="stat-label">Total Shards</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Stats.ActiveClients}}</div>
                <div class="stat-label">Active Clients</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{{.Stats.TotalRequests}}</div>
                <div class="stat-label">Total Requests</div>
            </div>
        </div>
        
        <!-- Main Dashboard Grid -->
        <div class="dashboard-grid">
            <!-- Left Column: Servers -->
            <div>
                <div class="section">
                    <div class="section-title">📡 Server Nodes</div>
                    
                    <!-- Add Node Form -->
                    <form action="/api/node/add" method="POST" style="margin-bottom: 20px;">
                        <div class="node-mgmt">
                            <button type="submit" class="btn btn-add">➕ Add New Node</button>
                        </div>
                    </form>
                    
                    <div class="nodes-grid">
                        {{range .Nodes}}
                        <div class="node-card">
                            <div class="node-header">
                                <div class="node-title">{{.NodeID}}</div>
                                <div class="status-badge {{if .Healthy}}{{if .IsLeader}}status-leader{{else}}status-follower{{end}}{{else}}status-offline{{end}}">
                                    {{if .Healthy}}{{if .IsLeader}}LEADER{{else}}{{.RaftState}}{{end}}{{else}}OFFLINE{{end}}
                                </div>
                            </div>
                            <div class="node-info">
                                <div>🌐 HTTP: {{.HTTPAddr}}</div>
                                <div>🔗 Raft: {{.RaftAddr}}</div>
                                {{if .PID}}<div>⚙️ PID: {{.PID}}</div>{{end}}
                            </div>
                            
                            {{if .Shards}}
                            <div style="margin-top: 10px; padding: 10px; background: #fef3c7; border-radius: 4px; border: 2px solid #f59e0b;">
                                <strong>🗂️ Shards ({{len .Shards}}):</strong>
                                <div style="margin-top: 5px; font-size: 11px; display: grid; grid-template-columns: repeat(2, 1fr); gap: 5px;">
                                    {{range .Shards}}
                                    <div style="padding: 4px; background: white; border-radius: 3px; border: 1px solid #d97706;">
                                        <div style="font-weight: bold; color: #b45309;">Shard {{.ShardID}}</div>
                                        <div style="color: #666;">{{.RaftState}}</div>
                                        {{if .Leader}}<div style="color: #16a34a; font-size: 10px;">Leader: {{.Leader}}</div>{{end}}
                                    </div>
                                    {{end}}
                                </div>
                            </div>
                            {{end}}
                            
                            {{if .IsLeader}}
                                <span class="keys-count">{{len .Keys}} keys</span>
                            {{else}}
                                <span class="keys-count" style="background: #3b82f6;">{{len .Keys}} keys (replicated from leader)</span>
                            {{end}}
                            {{if .Keys}}
                            <div style="grid-column: 1/-1; margin-top: 10px; padding: 10px; background: #f5f5f5; border-radius: 4px;">
                                <strong>🔑 Stored Keys:</strong>
                                <div style="margin-top: 5px; font-size: 12px; max-height: 150px; overflow-y: auto;">
                                    {{range $key, $value := .Keys}}
                                    <div style="padding: 2px 0; border-bottom: 1px solid #ddd;">
                                        <span style="color: #0066cc; font-weight: 500;">{{$key}}</span> = 
                                        <span style="color: #666;">{{$value}}</span>
                                    </div>
                                    {{end}}
                                </div>
                            </div>
                            {{else if not .IsLeader}}
                            <div style="grid-column: 1/-1; margin-top: 10px; padding: 10px; background: #e3f2fd; border-radius: 4px; font-size: 12px; color: #1976d2;">
                                ℹ️ <strong>Raft Replication:</strong> This follower has the same data as the leader, but reads must go through the leader for linearizability.
                            </div>
                            {{end}}
                        </div>
                        {{else}}
                        <div style="grid-column: 1/-1; text-align: center; padding: 40px; color: #999;">
                            No nodes active. Start some nodes!
                        </div>
                        {{end}}
                    </div>
                </div>
                
                <!-- Request Logs -->
                <div class="section" style="margin-top: 20px;">
                    <div class="section-title">📋 Request History (Last 20)</div>
                    <div class="logs-container">
                        {{range .Logs}}
                        <div class="log-entry {{if eq .Status 200}}log-success{{else if eq .Status 204}}log-success{{else}}log-error{{end}}">
                            <div class="log-header">
                                <span>{{.ClientID}} → {{.Method}} {{.Key}}</span>
                                <span>{{.Timestamp.Format "15:04:05"}}</span>
                            </div>
                            <div class="log-details">
                                Target: {{.TargetNode}} | Status: {{.Status}}
                                {{if .Value}} | Value: {{.Value}}{{end}}
                                {{if .Error}} | Error: {{.Error}}{{end}}
                            </div>
                        </div>
                        {{else}}
                        <div style="text-align: center; padding: 20px; color: #999;">
                            No requests yet. Start some clients!
                        </div>
                        {{end}}
                    </div>
                </div>
            </div>
            
            <!-- Right Column: Clients -->
            <div>
                <div class="section">
                    <div class="section-title">🎮 Automated Clients</div>
                    
                    <!-- Add Client Form -->
                    <form action="/api/client/add" method="POST" style="margin-bottom: 15px;">
                        <button type="submit" class="btn btn-add">➕ Add New Client</button>
                    </form>
                    
                    {{range .Clients}}
                    <div class="client-item {{if not .Active}}client-inactive{{end}}">
                        <div class="client-info">
                            <div class="client-name">{{.ID}}</div>
                            <div class="client-stats">
                                {{if .Active}}
                                ✅ Active | {{.Counter}} requests | {{.KeyPrefix}}:*
                                {{else}}
                                ⏸️ Stopped
                                {{end}}
                            </div>
                        </div>
                        {{if .Active}}
                        <form action="/api/client/stop" method="POST" style="display: inline;">
                            <input type="hidden" name="client_id" value="{{.ID}}">
                            <button type="submit" class="btn btn-stop">Stop</button>
                        </form>
                        {{else}}
                        <form action="/api/client/start" method="POST" style="display: inline;">
                            <input type="hidden" name="client_id" value="{{.ID}}">
                            <button type="submit" class="btn btn-start">Start</button>
                        </form>
                        {{end}}
                    </div>
                    {{else}}
                    <div style="text-align: center; padding: 30px; color: #999;">
                        No clients yet. Add one to start generating traffic!
                    </div>
                    {{end}}
                </div>
            </div>
        </div>
        
        <div class="timestamp">
            Last updated: {{.Timestamp}} | Auto-refresh: 3s
        </div>
    </div>
</body>
</html>
`

func (ui *WebUI) dashboardHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.New("dashboard").Parse(dashboardTemplate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ui.mu.RLock()
	defer ui.mu.RUnlock()

	// Calculate stats
	stats := struct {
		TotalNodes    int
		ActiveClients int
		TotalRequests int
		TotalShards   int
	}{
		TotalNodes:    len(ui.nodes),
		ActiveClients: 0,
		TotalRequests: len(ui.requestLogs),
		TotalShards:   0,
	}

	for _, client := range ui.clients {
		if client.Active {
			stats.ActiveClients++
		}
	}

	// Count unique shards across all nodes
	shardSet := make(map[string]bool)
	for _, node := range ui.nodes {
		for _, shard := range node.Shards {
			shardSet[shard.ShardID] = true
		}
	}
	stats.TotalShards = len(shardSet)

	nodes := make([]*NodeStatus, 0, len(ui.nodes))
	for _, node := range ui.nodes {
		nodes = append(nodes, node)
	}

	clients := make([]*AutoClient, 0, len(ui.clients))
	for _, client := range ui.clients {
		clients = append(clients, client)
	}

	logs := ui.requestLogs
	if len(logs) > 20 {
		logs = logs[:20]
	}

	data := struct {
		Nodes     []*NodeStatus
		Clients   []*AutoClient
		Logs      []RequestLog
		Stats     interface{}
		Timestamp string
	}{
		Nodes:     nodes,
		Clients:   clients,
		Logs:      logs,
		Stats:     stats,
		Timestamp: time.Now().Format("2006-01-02 15:04:05"),
	}

	tmpl.Execute(w, data)
}

func (ui *WebUI) addClientHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ui.mu.Lock()
	clientID := fmt.Sprintf("client%d", len(ui.clients)+1)
	prefix := fmt.Sprintf("c%d", len(ui.clients)+1)

	client := &AutoClient{
		ID:          clientID,
		Active:      false,
		RequestRate: 2 * time.Second,
		StopChan:    make(chan bool),
		KeyPrefix:   prefix,
		Counter:     0,
	}
	ui.clients[clientID] = client
	ui.mu.Unlock()

	// Auto-start the client
	ui.startClient(clientID)

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (ui *WebUI) startClient(clientID string) {
	ui.mu.Lock()
	client, exists := ui.clients[clientID]
	if !exists || client.Active {
		ui.mu.Unlock()
		return
	}
	client.Active = true
	client.StopChan = make(chan bool)
	ui.mu.Unlock()

	// Start background goroutine
	go func() {
		ticker := time.NewTicker(client.RequestRate)
		defer ticker.Stop()

		for {
			select {
			case <-client.StopChan:
				return
			case <-ticker.C:
				ui.generateAutomatedRequest(clientID)
			}
		}
	}()
}

func (ui *WebUI) startClientHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientID := r.FormValue("client_id")
	ui.startClient(clientID)

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (ui *WebUI) stopClientHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientID := r.FormValue("client_id")

	ui.mu.Lock()
	client, exists := ui.clients[clientID]
	if exists && client.Active {
		client.Active = false
		close(client.StopChan)
	}
	ui.mu.Unlock()

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (ui *WebUI) generateAutomatedRequest(clientID string) {
	ui.mu.Lock()
	client, exists := ui.clients[clientID]
	if !exists || !client.Active {
		ui.mu.Unlock()
		return
	}
	client.Counter++
	counter := client.Counter
	prefix := client.KeyPrefix
	ui.mu.Unlock()

	// Get available nodes
	ui.mu.RLock()
	nodes := make([]string, 0)
	for _, node := range ui.nodes {
		if node.Healthy {
			nodes = append(nodes, node.HTTPAddr)
		}
	}
	ui.mu.RUnlock()

	if len(nodes) == 0 {
		return
	}

	// Pick random node
	targetNode := nodes[rand.Intn(len(nodes))]

	// Generate key-value pair
	key := fmt.Sprintf("%s:key%d", prefix, counter)
	value := fmt.Sprintf("value%d_t%d", counter, time.Now().Unix())

	// Random operation (80% PUT, 20% GET/DELETE of existing keys)
	operation := "PUT"
	if counter > 3 && rand.Float32() < 0.2 {
		if rand.Float32() < 0.5 {
			operation = "GET"
			key = fmt.Sprintf("%s:key%d", prefix, rand.Intn(counter))
		} else {
			operation = "DELETE"
			key = fmt.Sprintf("%s:key%d", prefix, rand.Intn(counter))
		}
	}

	// Execute request
	logEntry := ui.executeRequest(targetNode, operation, key, value, clientID)

	ui.mu.Lock()
	ui.requestLogs = append([]RequestLog{logEntry}, ui.requestLogs...)
	if len(ui.requestLogs) > 100 {
		ui.requestLogs = ui.requestLogs[:100]
	}
	ui.mu.Unlock()
}

func (ui *WebUI) executeRequest(nodeAddr, method, key, value, clientID string) RequestLog {
	logEntry := RequestLog{
		Timestamp:  time.Now(),
		Method:     method,
		Key:        key,
		Value:      value,
		TargetNode: nodeAddr,
		ClientID:   clientID,
	}

	url := fmt.Sprintf("%s/v1/keys/%s", nodeAddr, key)
	var req *http.Request
	var err error

	switch method {
	case "PUT":
		req, err = http.NewRequest(http.MethodPut, url, bytes.NewBufferString(value))
		if err == nil {
			req.Header.Set("Content-Type", "text/plain")
		}
	case "GET":
		req, err = http.NewRequest(http.MethodGet, url, nil)
	case "DELETE":
		req, err = http.NewRequest(http.MethodDelete, url, nil)
	}

	if err != nil {
		logEntry.Error = err.Error()
		logEntry.Status = 0
		return logEntry
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
		// Don't follow redirects automatically, we'll handle 307 manually
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		logEntry.Error = err.Error()
		logEntry.Status = 0
		return logEntry
	}
	defer resp.Body.Close()

	logEntry.Status = resp.StatusCode

	// Handle 307 redirect (follower -> leader)
	if resp.StatusCode == http.StatusTemporaryRedirect && (method == "PUT" || method == "DELETE") {
		leaderRaftAddr := resp.Header.Get("X-Raft-Leader")
		if leaderRaftAddr != "" {
			// Find the HTTP address for this Raft address
			ui.mu.RLock()
			var leaderHTTPAddr string
			for _, node := range ui.nodes {
				if node.RaftAddr == leaderRaftAddr {
					leaderHTTPAddr = node.HTTPAddr
					break
				}
			}
			ui.mu.RUnlock()

			if leaderHTTPAddr != "" {
				// Retry on leader
				leaderURL := fmt.Sprintf("%s/v1/keys/%s", leaderHTTPAddr, key)
				var retryReq *http.Request
				if method == "PUT" {
					retryReq, err = http.NewRequest(http.MethodPut, leaderURL, bytes.NewBufferString(value))
					if err == nil {
						retryReq.Header.Set("Content-Type", "text/plain")
					}
				} else {
					retryReq, err = http.NewRequest(http.MethodDelete, leaderURL, nil)
				}

				if err == nil {
					retryResp, retryErr := client.Do(retryReq)
					if retryErr == nil {
						defer retryResp.Body.Close()
						logEntry.Status = retryResp.StatusCode
						logEntry.TargetNode = fmt.Sprintf("%s -> %s", nodeAddr, leaderHTTPAddr)
						buf, _ := io.ReadAll(retryResp.Body)
						if len(buf) > 0 && len(buf) < 256 {
							logEntry.Response = string(buf)
						}
						return logEntry
					}
				}
			}
		}
	}

	// Read response
	buf, _ := io.ReadAll(resp.Body)
	if len(buf) > 0 && len(buf) < 256 {
		logEntry.Response = string(buf)
	}

	return logEntry
}

func (ui *WebUI) addNodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ui.mu.Lock()
	nodeID := fmt.Sprintf("node%d", ui.nextNodeNum)
	httpPort := ui.nextHTTPPort
	raftPort := ui.nextRaftPort
	ui.nextNodeNum++
	ui.nextHTTPPort++
	ui.nextRaftPort++
	ui.mu.Unlock()

	// Start the node
	go ui.startNode(nodeID, httpPort, raftPort)

	time.Sleep(2 * time.Second) // Give it time to start
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (ui *WebUI) startNode(nodeID string, httpPort, raftPort int) {
	dataDir := filepath.Join(ui.projectRoot, fmt.Sprintf("%s-data", nodeID))
	logFile := filepath.Join(ui.projectRoot, "logs", fmt.Sprintf("%s.log", nodeID))

	// Create log directory
	os.MkdirAll(filepath.Join(ui.projectRoot, "logs"), 0755)

	// Build command
	serverBin := filepath.Join(ui.projectRoot, "bin", "server")
	cmd := exec.Command(serverBin,
		fmt.Sprintf("--node-id=%s", nodeID),
		fmt.Sprintf("--http-addr=:%d", httpPort),
		fmt.Sprintf("--raft-addr=127.0.0.1:%d", raftPort),
		fmt.Sprintf("--data-dir=%s", dataDir),
		"--join=http://localhost:8080",
		"--enable-raft",
	)

	// Setup log file
	logF, err := os.Create(logFile)
	if err != nil {
		log.Printf("Failed to create log file for %s: %v", nodeID, err)
		return
	}
	defer logF.Close()

	cmd.Stdout = logF
	cmd.Stderr = logF

	// Start the process
	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start %s: %v", nodeID, err)
		return
	}

	// Save PID
	pidFile := filepath.Join(ui.projectRoot, "logs", fmt.Sprintf("%s.pid", nodeID))
	os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644)

	// Save node config
	ui.mu.Lock()
	ui.nodeConfigs[nodeID] = &NodeConfig{
		NodeID:   nodeID,
		HTTPPort: httpPort,
		RaftPort: raftPort,
		Running:  true,
		PID:      cmd.Process.Pid,
	}
	ui.mu.Unlock()

	log.Printf("Started %s on HTTP:%d Raft:%d PID:%d", nodeID, httpPort, raftPort, cmd.Process.Pid)

	// Wait for process
	cmd.Wait()
}

func (ui *WebUI) refreshAllNodes() {
	// Refresh static nodes - now supports 6 nodes for distributed sharding
	staticNodes := []struct {
		id       string
		httpAddr string
		raftAddr string
	}{
		{"node1", "http://localhost:8080", "localhost:9080"},
		{"node2", "http://localhost:8081", "localhost:9081"},
		{"node3", "http://localhost:8082", "localhost:9082"},
		{"node4", "http://localhost:8083", "localhost:9083"},
		{"node5", "http://localhost:8084", "localhost:9084"},
		{"node6", "http://localhost:8085", "localhost:9085"},
	}

	for _, node := range staticNodes {
		ui.refreshNode(node.id, node.httpAddr, node.raftAddr)
	}

	// Refresh dynamic nodes
	ui.mu.RLock()
	configs := make([]*NodeConfig, 0)
	for _, cfg := range ui.nodeConfigs {
		configs = append(configs, cfg)
	}
	ui.mu.RUnlock()

	for _, cfg := range configs {
		httpAddr := fmt.Sprintf("http://localhost:%d", cfg.HTTPPort)
		raftAddr := fmt.Sprintf("localhost:%d", cfg.RaftPort)
		ui.refreshNode(cfg.NodeID, httpAddr, raftAddr)
	}
}

func (ui *WebUI) refreshNode(nodeID, httpAddr, raftAddr string) {
	status := &NodeStatus{
		NodeID:      nodeID,
		HTTPAddr:    httpAddr,
		RaftAddr:    raftAddr,
		LastUpdated: time.Now(),
		Keys:        make(map[string]string),
		Shards:      make([]ShardInfo, 0),
	}

	// Get PID if available
	ui.mu.RLock()
	if cfg, exists := ui.nodeConfigs[nodeID]; exists {
		status.PID = cfg.PID
	}
	ui.mu.RUnlock()

	// Get node status
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/v1/status", httpAddr))
	if err != nil {
		status.Healthy = false
		ui.mu.Lock()
		ui.nodes[nodeID] = status
		ui.mu.Unlock()
		return
	}
	defer resp.Body.Close()

	status.Healthy = true

	var statusData struct {
		NodeID    string `json:"node_id"`
		IsLeader  bool   `json:"is_leader"`
		RaftState string `json:"raft_state"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&statusData); err == nil {
		status.IsLeader = statusData.IsLeader
		status.RaftState = statusData.RaftState
	}

	// Fetch shard information
	shardResp, err := client.Get(fmt.Sprintf("%s/v1/shards/status", httpAddr))
	if err == nil {
		defer shardResp.Body.Close()
		var shardStatuses []struct {
			ShardID  string `json:"shard_id"`
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
			IsLeader bool   `json:"is_leader"`
		}
		if err := json.NewDecoder(shardResp.Body).Decode(&shardStatuses); err == nil {
			for _, shardStatus := range shardStatuses {
				raftState := "Follower"
				if shardStatus.IsLeader {
					raftState = "Leader"
				}
				status.Shards = append(status.Shards, ShardInfo{
					ShardID:   shardStatus.ShardID,
					RaftState: raftState,
					Leader:    shardStatus.RaftAddr,
					ReadOnly:  !shardStatus.IsLeader,
				})
			}
		}
	}

	// Only try to get keys if this is the leader (followers redirect reads to leader)
	// In Raft, all nodes have the same data (replicated), but reads must go through leader
	if status.IsLeader {
		// Check ALL keys generated by automated clients (no limit)
		ui.mu.RLock()
		var testKeys []string
		for _, client := range ui.clients {
			// Check all keys this client has generated
			for i := 1; i <= client.Counter; i++ {
				testKeys = append(testKeys, fmt.Sprintf("%s:key%d", client.KeyPrefix, i))
			}
		}
		// Also check some common test keys
		testKeys = append(testKeys, "test", "user:1", "user:2")
		ui.mu.RUnlock()

		// Try to retrieve all keys (no 20-key limit)
		for _, key := range testKeys {
			resp2, err := client.Get(fmt.Sprintf("%s/v1/keys/%s", httpAddr, key))
			if err == nil && resp2.StatusCode == http.StatusOK {
				buf, _ := io.ReadAll(resp2.Body)
				if len(buf) > 0 && len(buf) < 256 {
					status.Keys[key] = string(buf)
				}
				resp2.Body.Close()
			}
		}
	}

	ui.mu.Lock()
	ui.nodes[nodeID] = status
	ui.mu.Unlock()
}

func (ui *WebUI) backgroundRefresh() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		ui.refreshAllNodes()
	}
}

func main() {
	// Get project root
	projectRoot, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	port := 9000
	portEnv := os.Getenv("WEBUI_PORT")
	if portEnv != "" {
		port, _ = strconv.Atoi(portEnv)
	}

	// Use the new multi-cluster UI
	RunMultiClusterUI(projectRoot, port)
}
