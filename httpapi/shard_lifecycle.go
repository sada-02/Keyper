package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	raft "github.com/hashicorp/raft"
)

// RegisterShardLifecycleRoutes registers shard management endpoints
func (h *Handler) RegisterShardLifecycleRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/shards/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/shards/")
		parts := strings.Split(path, "/")

		if len(parts) < 2 {
			http.Error(w, "invalid shard path", http.StatusBadRequest)
			return
		}

		shardID := parts[0]
		action := parts[1]

		switch action {
		case "pause":
			h.pauseShardHandler(w, r, shardID)
		case "resume":
			h.resumeShardHandler(w, r, shardID)
		case "state":
			h.shardStateHandler(w, r, shardID)
		case "join":
			h.shardJoinHandler(w, r, shardID)
		case "leave":
			h.shardLeaveHandler(w, r, shardID)
		case "_export", "_snapshot": // Snapshot/export migration endpoints
			h.handleShardExport(w, r, shardID)
		case "_import": // Import migration endpoint
			h.handleShardImport(w, r, shardID)
		default:
			http.Error(w, "unknown action: "+action, http.StatusNotFound)
		}
	})
}

// pauseShardHandler sets a shard to read-only mode
func (h *Handler) pauseShardHandler(w http.ResponseWriter, r *http.Request, shardID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.ShardRafts == nil {
		http.Error(w, "sharding not enabled", http.StatusBadRequest)
		return
	}

	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil {
		http.Error(w, "shard not found: "+shardID, http.StatusNotFound)
		return
	}

	sr.State.SetReadOnly(true)
	w.WriteHeader(http.StatusNoContent)
}

// resumeShardHandler resumes normal read-write operation
func (h *Handler) resumeShardHandler(w http.ResponseWriter, r *http.Request, shardID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.ShardRafts == nil {
		http.Error(w, "sharding not enabled", http.StatusBadRequest)
		return
	}

	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil {
		http.Error(w, "shard not found: "+shardID, http.StatusNotFound)
		return
	}

	sr.State.SetReadOnly(false)
	sr.State.SetPaused(false)
	w.WriteHeader(http.StatusNoContent)
}

// shardStateHandler returns the current operational state of a shard
func (h *Handler) shardStateHandler(w http.ResponseWriter, r *http.Request, shardID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.ShardRafts == nil {
		http.Error(w, "sharding not enabled", http.StatusBadRequest)
		return
	}

	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil {
		http.Error(w, "shard not found: "+shardID, http.StatusNotFound)
		return
	}

	state := map[string]interface{}{
		"shard_id":  shardID,
		"read_only": sr.State.IsReadOnly(),
		"paused":    sr.State.IsPaused(),
		"can_write": sr.State.CanWrite(),
		"can_read":  sr.State.CanRead(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

// shardJoinHandler adds a node to a specific shard's Raft cluster
func (h *Handler) shardJoinHandler(w http.ResponseWriter, r *http.Request, shardID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.ShardRafts == nil {
		http.Error(w, "sharding not enabled", http.StatusBadRequest)
		return
	}

	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil || sr.Node == nil {
		http.Error(w, "shard not found: "+shardID, http.StatusNotFound)
		return
	}

	if sr.Node.Raft.State() != raft.Leader {
		w.Header().Set("X-Raft-Leader", sr.Node.Leader())
		http.Error(w, "not leader for shard "+shardID, http.StatusTemporaryRedirect)
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.NodeID == "" || req.RaftAddr == "" {
		http.Error(w, "node_id and raft_addr required", http.StatusBadRequest)
		return
	}

	if err := sr.Node.AddVoter(req.NodeID, req.RaftAddr, 10*time.Second); err != nil {
		http.Error(w, "add voter failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// shardLeaveHandler removes a node from a specific shard's Raft cluster
func (h *Handler) shardLeaveHandler(w http.ResponseWriter, r *http.Request, shardID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.ShardRafts == nil {
		http.Error(w, "sharding not enabled", http.StatusBadRequest)
		return
	}

	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil || sr.Node == nil {
		http.Error(w, "shard not found: "+shardID, http.StatusNotFound)
		return
	}

	if sr.Node.Raft.State() != raft.Leader {
		w.Header().Set("X-Raft-Leader", sr.Node.Leader())
		http.Error(w, "not leader for shard "+shardID, http.StatusTemporaryRedirect)
		return
	}

	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.NodeID == "" {
		http.Error(w, "node_id required", http.StatusBadRequest)
		return
	}

	if err := sr.Node.RemoveServer(req.NodeID, 10*time.Second); err != nil {
		http.Error(w, "remove server failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
