package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

// HealthStatus represents the health status of the server
type HealthStatus struct {
	OK          bool          `json:"ok"`
	NodeID      string        `json:"node_id,omitempty"`
	Uptime      float64       `json:"uptime_seconds"`
	Timestamp   string        `json:"timestamp"`
	Shards      []ShardHealth `json:"shards,omitempty"`
	RaftEnabled bool          `json:"raft_enabled"`
	Issues      []string      `json:"issues,omitempty"`
}

// ShardHealth represents the health of a single shard
type ShardHealth struct {
	ShardID   string `json:"shard_id"`
	RaftState string `json:"raft_state"`
	Leader    string `json:"leader,omitempty"`
	PeerCount int    `json:"peer_count"`
	ReadOnly  bool   `json:"read_only"`
	Healthy   bool   `json:"healthy"`
}

var serverStartTime = time.Now()

// healthHandler returns server health status
func (h *Handler) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := HealthStatus{
		OK:          true,
		Uptime:      time.Since(serverStartTime).Seconds(),
		Timestamp:   time.Now().Format(time.RFC3339),
		RaftEnabled: h.ShardRafts != nil,
		Issues:      []string{},
	}

	// Check shard health if sharding is enabled
	if h.ShardRafts != nil {
		status.Shards = make([]ShardHealth, 0, len(h.ShardRafts))

		for shardID, sr := range h.ShardRafts {
			if sr == nil || sr.Node == nil {
				status.Issues = append(status.Issues, "shard "+shardID+": nil node")
				status.OK = false
				continue
			}

			// Get peer count from Raft configuration
			configFuture := sr.Node.Raft.GetConfiguration()
			peerCount := 0
			if err := configFuture.Error(); err == nil {
				peerCount = len(configFuture.Configuration().Servers)
			}

			shardHealth := ShardHealth{
				ShardID:   shardID,
				RaftState: sr.Node.Raft.State().String(),
				Leader:    sr.Node.Leader(),
				PeerCount: peerCount,
				ReadOnly:  sr.State.IsReadOnly(),
				Healthy:   true,
			}

			// Check if Raft is healthy
			raftState := sr.Node.Raft.State()
			if raftState == raft.Shutdown {
				shardHealth.Healthy = false
				status.Issues = append(status.Issues, "shard "+shardID+": raft shutdown")
				status.OK = false
			}

			// Check if we have a leader (unless we are bootstrapping)
			if raftState != raft.Leader && shardHealth.Leader == "" && shardHealth.PeerCount > 1 {
				// No leader and we're not alone - potential issue
				shardHealth.Healthy = false
				status.Issues = append(status.Issues, "shard "+shardID+": no leader")
				// Don't mark overall health as bad - this can be temporary during elections
			}

			status.Shards = append(status.Shards, shardHealth)
		}
	}

	// Set HTTP status based on health
	statusCode := http.StatusOK
	if !status.OK {
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(status)
}

// readinessHandler returns readiness status (can accept traffic)
func (h *Handler) readinessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ready := true
	issues := []string{}

	// Check if shards are ready
	if h.ShardRafts != nil {
		for shardID, sr := range h.ShardRafts {
			if sr == nil || sr.Node == nil {
				ready = false
				issues = append(issues, "shard "+shardID+": not initialized")
				continue
			}

			// Check if Raft is shutdown
			if sr.Node.Raft.State() == raft.Shutdown {
				ready = false
				issues = append(issues, "shard "+shardID+": raft shutdown")
			}
		}
	}

	status := map[string]interface{}{
		"ready":     ready,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	if len(issues) > 0 {
		status["issues"] = issues
	}

	statusCode := http.StatusOK
	if !ready {
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(status)
}

// livenessHandler returns liveness status (process is alive)
func (h *Handler) livenessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Simple liveness check - if we can respond, we're alive
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"alive":     true,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}
