package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
)

// RegisterShardRoutes registers admin shard endpoints.
// Requires Handler.ShardMgr to be non-nil.
func (h *Handler) RegisterShardRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/shards", h.shardsListHandler)          // GET list
	mux.HandleFunc("/v1/shards/assign", h.shardsAssignHandler) // POST assign
	mux.HandleFunc("/v1/shards/status", h.shardsStatusHandler) // GET status for all local shard rafts
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
					info.IsLeader = sr.Node.Raft.State() == 3 // raft.Leader == 3 (avoid import cycle)
				}
			}
			out = append(out, info)
		}
	}
	b, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
