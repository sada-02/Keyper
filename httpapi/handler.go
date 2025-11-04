package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	raft "github.com/hashicorp/raft"
	raftnode "github.com/sada-02/keyper/raft"
	"github.com/sada-02/keyper/shard"
	shardraft "github.com/sada-02/keyper/shardraft"
	"github.com/sada-02/keyper/store"
)

// Handler holds dependencies for HTTP endpoints.
type Handler struct {
	Store      *store.BadgerStore
	NodeID     string
	RaftNode   *raftnode.Node // nil if Raft disabled
	ShardMgr   *shard.ShardManager
	ShardRafts map[string]*shardraft.ShardRaft
	ShardCount int // number of shards; if > 0, use per-shard Raft instead of main Raft
}

// NewHandler builds a Handler.
func NewHandler(s *store.BadgerStore, nodeID string) *Handler {
	return &Handler{
		Store:  s,
		NodeID: nodeID,
	}
}

// Register attaches HTTP handlers to the provided mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/v1/status", h.statusHandler)
	mux.HandleFunc("/v1/keys/", h.keyHandler)
	mux.HandleFunc("/v1/join", h.joinHandler)
	mux.HandleFunc("/v1/leave", h.leaveHandler)

	// Health and observability endpoints
	mux.HandleFunc("/v1/health", h.healthHandler)
	mux.HandleFunc("/v1/health/ready", h.readinessHandler)
	mux.HandleFunc("/v1/health/live", h.livenessHandler)

	// Election management endpoints
	mux.HandleFunc("/v1/election/trigger", h.TriggerElection)
	mux.HandleFunc("/v1/election/status", h.GetElectionStatus)
	mux.HandleFunc("/v1/election/quorum", h.GetQuorumInfo)

	// Register shard lifecycle routes if sharding is enabled
	if h.ShardCount > 0 {
		h.RegisterShardLifecycleRoutes(mux)
	}
}

func (h *Handler) keyHandler(w http.ResponseWriter, r *http.Request) {
	// path: /v1/keys/<key>
	key := strings.TrimPrefix(r.URL.Path, "/v1/keys/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	// If sharding enabled, route to per-shard handler
	if h.ShardCount > 0 && h.ShardRafts != nil {
		h.shardedKeyHandler(w, r, key)
		return
	}

	// Otherwise use legacy single-raft handler
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		// If Raft enabled, apply via raft; else write directly.
		if h.RaftNode != nil {
			// If not leader, redirect client to leader
			if h.RaftNode.Raft.State() != raft.Leader {
				leader := h.RaftNode.Leader()
				if leader != "" {
					w.Header().Set("X-Raft-Leader", leader)
				}
				http.Error(w, "not leader", http.StatusTemporaryRedirect)
				return
			}
			cmd := &raftnode.Command{
				Op:    "set",
				Key:   key,
				Value: body,
			}
			if err := h.RaftNode.ApplyCommand(cmd, 5*time.Second); err != nil {
				http.Error(w, "raft apply failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// No raft -> direct write
		if err := h.Store.Set([]byte(key), body); err != nil {
			http.Error(w, "set failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		// Check for stale-read query parameter (allows reads from followers)
		allowStale := r.URL.Query().Get("stale") == "true"

		if h.RaftNode != nil {
			// If follower and client wants linearizable read -> redirect to leader
			if h.RaftNode.Raft.State() != raft.Leader && !allowStale {
				leader := h.RaftNode.Leader()
				if leader != "" {
					w.Header().Set("X-Raft-Leader", leader)
				}
				// ask client to retry at leader (307 Temporary Redirect)
				http.Error(w, "not leader — read must go to leader (or use ?stale=true)", http.StatusTemporaryRedirect)
				return
			}

			// Linearizable read (leader only): use Barrier
			if h.RaftNode.Raft.State() == raft.Leader {
				barrierFut := h.RaftNode.Raft.Barrier(5 * time.Second)
				if err := barrierFut.Error(); err != nil {
					http.Error(w, "raft barrier failed: "+err.Error(), http.StatusInternalServerError)
					return
				}
			} else {
				// Stale read from follower - warn client
				w.Header().Set("X-Raft-Stale-Read", "true")
				w.Header().Set("X-Raft-Leader", h.RaftNode.Leader())
			}

			// Read from local store
			val, err := h.Store.Get([]byte(key))
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				http.Error(w, "get failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(val)
			return
		}

		// Raft not enabled -> direct read (best-effort)
		val, err := h.Store.Get([]byte(key))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "get failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(val)
	case http.MethodDelete:
		if h.RaftNode != nil {
			if h.RaftNode.Raft.State() != raft.Leader {
				leader := h.RaftNode.Leader()
				if leader != "" {
					w.Header().Set("X-Raft-Leader", leader)
				}
				http.Error(w, "not leader", http.StatusTemporaryRedirect)
				return
			}
			cmd := &raftnode.Command{
				Op:  "delete",
				Key: key,
			}
			if err := h.RaftNode.ApplyCommand(cmd, 5*time.Second); err != nil {
				http.Error(w, "raft apply failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// No raft -> direct delete
		err := h.Store.Delete([]byte(key))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT, GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) statusHandler(w http.ResponseWriter, r *http.Request) {
	leader := ""
	isLeader := false
	if h.RaftNode != nil {
		leader = h.RaftNode.Leader()
		if h.RaftNode.Raft.State() == raft.Leader {
			isLeader = true
		}
	}

	// Get key count from store
	keyCount := 0
	if h.Store != nil {
		it := h.Store.NewIterator()
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keyCount++
		}
	}

	resp := map[string]interface{}{
		"node_id":     h.NodeID,
		"status":      "ok",
		"is_leader":   isLeader,
		"leader_addr": leader,
		"num_keys":    keyCount,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// joinHandler implements a simple join API:
// POST /v1/join with JSON {"node_id":"id","raft_addr":"host:port"}
// Only leader should accept join requests and call AddVoter on raft.
func (h *Handler) joinHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.RaftNode == nil {
		http.Error(w, "raft not enabled", http.StatusBadRequest)
		return
	}
	// only leader should authorize join
	if h.RaftNode.Raft.State() != raft.Leader {
		w.Header().Set("X-Raft-Leader", h.RaftNode.Leader())
		http.Error(w, "not leader", http.StatusTemporaryRedirect)
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.NodeID == "" || req.RaftAddr == "" {
		http.Error(w, "node_id and raft_addr required", http.StatusBadRequest)
		return
	}

	// Add voter
	if err := h.RaftNode.AddVoter(req.NodeID, req.RaftAddr, 10*time.Second); err != nil {
		http.Error(w, "add voter failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// leaveHandler implements node removal from Raft cluster:
// POST /v1/leave with JSON {"node_id":"id"}
// Only leader should accept leave requests and call RemoveServer on raft.
func (h *Handler) leaveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.RaftNode == nil {
		http.Error(w, "raft not enabled", http.StatusBadRequest)
		return
	}
	// only leader should authorize removal
	if h.RaftNode.Raft.State() != raft.Leader {
		w.Header().Set("X-Raft-Leader", h.RaftNode.Leader())
		http.Error(w, "not leader", http.StatusTemporaryRedirect)
		return
	}

	var req struct {
		NodeID string `json:"node_id"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.NodeID == "" {
		http.Error(w, "node_id required", http.StatusBadRequest)
		return
	}

	// Remove server
	if err := h.RaftNode.RemoveServer(req.NodeID, 10*time.Second); err != nil {
		http.Error(w, "remove server failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// shardedKeyHandler handles key operations when sharding is enabled.
// It routes the request to the appropriate shard based on the key.
func (h *Handler) shardedKeyHandler(w http.ResponseWriter, r *http.Request, key string) {
	// Determine which shard this key belongs to
	shardID := shard.KeyToShard(key, h.ShardCount)
	if shardID == "" {
		http.Error(w, "invalid shard configuration", http.StatusInternalServerError)
		return
	}

	// Check if this node owns the shard
	if h.ShardMgr != nil && !h.ShardMgr.HasShard(shardID) {
		// This node does not own this shard - return 307 redirect
		w.Header().Set("X-Shard-ID", shardID)
		w.Header().Set("X-Shard-Owner", "unknown") // TODO: query control plane for actual owner
		http.Error(w, "shard "+shardID+" not owned by this node", http.StatusTemporaryRedirect)
		return
	}

	// Get the shard Raft instance
	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil || sr.Node == nil || sr.Store == nil {
		http.Error(w, "shard not available: "+shardID, http.StatusServiceUnavailable)
		return
	}

	// Check shard operational state
	if r.Method == http.MethodPut || r.Method == http.MethodDelete {
		// Write operations - check if writes are allowed
		if !sr.State.CanWrite() {
			if sr.State.IsPaused() {
				http.Error(w, "shard paused: "+shardID, http.StatusServiceUnavailable)
			} else if sr.State.IsReadOnly() {
				http.Error(w, "shard read-only (migration in progress): "+shardID, http.StatusForbidden)
			}
			return
		}
	} else if r.Method == http.MethodGet {
		// Read operations - check if reads are allowed
		if !sr.State.CanRead() {
			http.Error(w, "shard paused: "+shardID, http.StatusServiceUnavailable)
			return
		}
	}

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		// Check if this shard's Raft node is the leader
		if sr.Node.Raft.State() != raft.Leader {
			leader := sr.Node.Leader()
			if leader != "" {
				w.Header().Set("X-Raft-Leader", leader)
				w.Header().Set("X-Shard-ID", shardID)
			}
			http.Error(w, "not leader for shard "+shardID, http.StatusTemporaryRedirect)
			return
		}

		// Apply command to shard's Raft
		cmd := &raftnode.Command{
			Op:    "set",
			Key:   key,
			Value: body,
		}
		if err := sr.Node.ApplyCommand(cmd, 5*time.Second); err != nil {
			http.Error(w, "shard raft apply failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	case http.MethodGet:
		// Linearizable read from shard leader
		if sr.Node.Raft.State() != raft.Leader {
			leader := sr.Node.Leader()
			if leader != "" {
				w.Header().Set("X-Raft-Leader", leader)
				w.Header().Set("X-Shard-ID", shardID)
			}
			http.Error(w, "not leader for shard "+shardID, http.StatusTemporaryRedirect)
			return
		}

		// Barrier for linearizable read
		barrierFut := sr.Node.Raft.Barrier(5 * time.Second)
		if err := barrierFut.Error(); err != nil {
			http.Error(w, "shard raft barrier failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Read from shard's store
		val, err := sr.Store.Get([]byte(key))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "shard get failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(val)

	case http.MethodDelete:
		// Check if this shard's Raft node is the leader
		if sr.Node.Raft.State() != raft.Leader {
			leader := sr.Node.Leader()
			if leader != "" {
				w.Header().Set("X-Raft-Leader", leader)
				w.Header().Set("X-Shard-ID", shardID)
			}
			http.Error(w, "not leader for shard "+shardID, http.StatusTemporaryRedirect)
			return
		}

		// Apply delete to shard's Raft
		cmd := &raftnode.Command{
			Op:  "delete",
			Key: key,
		}
		if err := sr.Node.ApplyCommand(cmd, 5*time.Second); err != nil {
			http.Error(w, "shard raft apply failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.Header().Set("Allow", "PUT, GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
