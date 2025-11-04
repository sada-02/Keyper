package control

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// RegisterControlRoutes registers the control-plane HTTP endpoints on the given mux.
// Endpoints:
//   - GET  /v1/control/shards                    => list all shard assignments
//   - GET  /v1/control/shards/{shardID}          => get assignment for shard
//   - POST /v1/control/shards/assign             => assign shard
//   - GET  /v1/control/shards/{shardID}/members  => get Raft members for shard
//   - POST /v1/control/shards/{shardID}/members  => set Raft members for shard
//   - GET  /v1/control/nodes                     => list registered nodes
//   - POST /v1/control/nodes                     => register a node
func (c *ControlNode) RegisterControlRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/control/shards", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			c.handleListAssignments(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/control/shards/assign", c.handleAssignShard)
	mux.HandleFunc("/v1/control/shards/get", c.handleGetShard)

	// Node registration
	mux.HandleFunc("/v1/control/nodes", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			c.handleListNodes(w, r)
		case http.MethodPost:
			c.handleRegisterNode(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// New: Shard membership management
	mux.HandleFunc("/v1/control/shards/", func(w http.ResponseWriter, r *http.Request) {
		// Parse path: /v1/control/shards/{shardID}/members
		path := r.URL.Path
		if len(path) > len("/v1/control/shards/") {
			// Check if it ends with /members
			if len(path) > 8 && path[len(path)-8:] == "/members" {
				shardID := path[len("/v1/control/shards/") : len(path)-8]
				if r.Method == http.MethodGet {
					c.handleGetShardMembers(w, r, shardID)
				} else if r.Method == http.MethodPost {
					c.handleSetShardMembers(w, r, shardID)
				} else {
					http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				}
				return
			}
		}
		http.Error(w, "not found", http.StatusNotFound)
	})
}

type assignReq struct {
	ShardID string   `json:"shard_id"`
	Nodes   []string `json:"nodes"`
}

// handleAssignShard writes an assignment via Raft (replicated).
func (c *ControlNode) handleAssignShard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var req assignReq
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.ShardID == "" {
		http.Error(w, "shard_id required", http.StatusBadRequest)
		return
	}
	// serialize nodes list to JSON value
	val, _ := json.Marshal(req.Nodes)
	key := fmt.Sprintf("shard:%s", req.ShardID)
	// apply via raft with timeout
	timeout := 5 * time.Second
	if err := c.SetAssignment(key, val, timeout); err != nil {
		http.Error(w, fmt.Sprintf("apply failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleGetShard returns nodes for shard_id query parameter.
func (c *ControlNode) handleGetShard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query().Get("shard_id")
	if q == "" {
		http.Error(w, "shard_id query required", http.StatusBadRequest)
		return
	}
	key := fmt.Sprintf("shard:%s", q)
	b, err := c.GetAssignment(key)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// handleListAssignments lists all keys/values.
func (c *ControlNode) handleListAssignments(w http.ResponseWriter, r *http.Request) {
	// list all assignments
	m, err := c.ListAssignments()
	if err != nil {
		http.Error(w, fmt.Sprintf("list failed: %v", err), http.StatusInternalServerError)
		return
	}
	// convert values from JSON bytes to []string where possible
	out := map[string][]string{}
	for k, v := range m {
		var nodes []string
		if err := json.Unmarshal(v, &nodes); err != nil {
			// if unmarshal fails, skip or include empty
			nodes = []string{}
		}
		out[k] = nodes
	}
	b, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// handleGetShardMembers returns the Raft membership list for a shard
func (c *ControlNode) handleGetShardMembers(w http.ResponseWriter, r *http.Request, shardID string) {
	key := fmt.Sprintf("members:%s", shardID)
	b, err := c.GetAssignment(key)
	if err != nil {
		// No members registered yet - return empty list
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"members":[]}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// handleSetShardMembers updates the Raft membership list for a shard
func (c *ControlNode) handleSetShardMembers(w http.ResponseWriter, r *http.Request, shardID string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	var req struct {
		Members []struct {
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
		} `json:"members"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	// Store the membership info
	key := fmt.Sprintf("members:%s", shardID)

	// Re-marshal to store
	data, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "marshal failed", http.StatusInternalServerError)
		return
	}

	if err := c.SetAssignment(key, data, 5*time.Second); err != nil {
		http.Error(w, fmt.Sprintf("set failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// NodeMetadata represents information about a registered node
type NodeMetadata struct {
	NodeID   string `json:"node_id"`
	HTTPAddr string `json:"http_addr"`
	RaftAddr string `json:"raft_addr"`
}

// handleRegisterNode registers a new node in the control plane
func (c *ControlNode) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	var node NodeMetadata
	if err := json.Unmarshal(body, &node); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if node.NodeID == "" || node.HTTPAddr == "" || node.RaftAddr == "" {
		http.Error(w, "node_id, http_addr, and raft_addr required", http.StatusBadRequest)
		return
	}

	// Store node metadata with key "node:<id>"
	key := fmt.Sprintf("node:%s", node.NodeID)
	data, err := json.Marshal(node)
	if err != nil {
		http.Error(w, "marshal failed", http.StatusInternalServerError)
		return
	}

	if err := c.SetAssignment(key, data, 5*time.Second); err != nil {
		http.Error(w, fmt.Sprintf("registration failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status":"registered"}`))
}

// handleListNodes returns all registered nodes
func (c *ControlNode) handleListNodes(w http.ResponseWriter, r *http.Request) {
	m, err := c.ListAssignments()
	if err != nil {
		http.Error(w, fmt.Sprintf("list failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Filter for node: prefixed keys
	nodes := []NodeMetadata{}
	for k, v := range m {
		if len(k) > 5 && k[:5] == "node:" {
			var node NodeMetadata
			if err := json.Unmarshal(v, &node); err == nil {
				nodes = append(nodes, node)
			}
		}
	}

	response := map[string][]NodeMetadata{
		"nodes": nodes,
	}

	b, _ := json.Marshal(response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}
