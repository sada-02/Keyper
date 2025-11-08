package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	raft "github.com/hashicorp/raft"
)

// RegisterShardRoutes registers admin shard endpoints.
// Requires Handler.ShardMgr to be non-nil.
func (h *Handler) RegisterShardRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/shards", h.shardsListHandler)          // GET list
	mux.HandleFunc("/v1/shards/assign", h.shardsAssignHandler) // POST assign
	mux.HandleFunc("/v1/shards/status", h.shardsStatusHandler) // GET status for all local shard rafts
	mux.HandleFunc("/v1/shards/join", h.shardsJoinHandler)     // POST join a shard raft cluster
}

func (h *Handler) shardsListHandler(w http.ResponseWriter, r *http.Request) {
	if h.ShardMgr == nil {
		http.Error(w, "shard manager not enabled", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	list := h.ShardMgr.List()
	b, _ := json.Marshal(list)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (h *Handler) shardsAssignHandler(w http.ResponseWriter, r *http.Request) {
	if h.ShardMgr == nil {
		http.Error(w, "shard manager not enabled", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var req struct {
		ShardID  string `json:"shard_id"`
		RaftAddr string `json:"raft_addr"` // optional
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.ShardID == "" {
		http.Error(w, "shard_id required", http.StatusBadRequest)
		return
	}
	h.ShardMgr.AddShard(req.ShardID)
	w.WriteHeader(http.StatusNoContent)
}

// shardsStatusHandler returns per-shard raft info we are running locally.
func (h *Handler) shardsStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	type ShardInfo struct {
		ShardID  string `json:"shard_id"`
		NodeID   string `json:"node_id,omitempty"`
		RaftAddr string `json:"raft_addr,omitempty"`
		IsLeader bool   `json:"is_leader"`
	}
	out := []ShardInfo{}
	if h.ShardRafts != nil {
		for id, sr := range h.ShardRafts {
			info := ShardInfo{ShardID: id}
			if sr != nil && sr.Node != nil {
				info.NodeID = sr.Node.ID
				info.RaftAddr = sr.Node.Addr
				if sr.Node.Raft != nil {
					info.IsLeader = sr.Node.Raft.State() == raft.Leader
				}
			}
			out = append(out, info)
		}
	}
	b, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// shardsJoinHandler adds a node to a specific shard's Raft cluster
func (h *Handler) shardsJoinHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
		ShardID  string `json:"shard_id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.NodeID == "" || req.RaftAddr == "" || req.ShardID == "" {
		http.Error(w, "node_id, raft_addr, and shard_id required", http.StatusBadRequest)
		return
	}

	// Get the shard Raft instance
	if h.ShardRafts == nil {
		http.Error(w, "shard rafts not initialized", http.StatusBadRequest)
		return
	}

	sr, exists := h.ShardRafts[req.ShardID]
	if !exists {
		http.Error(w, fmt.Sprintf("shard %s not found on this node", req.ShardID), http.StatusNotFound)
		return
	}

	// Only the leader can add voters
	if !sr.IsLeader() {
		// Return the leader's Raft address so the client can redirect
		if sr.Node != nil && sr.Node.Raft != nil {
			leaderAddr := string(sr.Node.Raft.Leader())
			w.Header().Set("X-Raft-Leader", leaderAddr)
		}
		http.Error(w, "not the leader for this shard", http.StatusTemporaryRedirect)
		return
	}

	// Add the voter to the Raft cluster
	if err := sr.AddVoter(req.NodeID, req.RaftAddr, 10*time.Second); err != nil {
		http.Error(w, fmt.Sprintf("failed to add voter: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
