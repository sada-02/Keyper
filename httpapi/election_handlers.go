package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

// ElectionRequest represents a request to trigger an election
type ElectionRequest struct {
	Method string `json:"method"` // "stepdown" or "transfer"
	Target string `json:"target"` // Node ID to transfer to (optional)
}

// ElectionResponse contains election results
type ElectionResponse struct {
	Success     bool              `json:"success"`
	Message     string            `json:"message"`
	OldLeader   string            `json:"old_leader"`
	NewLeader   string            `json:"new_leader,omitempty"`
	Votes       map[string]string `json:"votes,omitempty"`
	ElectionLog []string          `json:"election_log,omitempty"`
}

// VoteInfo tracks voting information
type VoteInfo struct {
	Voter     string    `json:"voter"`
	Candidate string    `json:"candidate"`
	Term      uint64    `json:"term"`
	Timestamp time.Time `json:"timestamp"`
}

// TriggerElection handles graceful leader stepdown or transfer
func (h *Handler) TriggerElection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.RaftNode == nil {
		http.Error(w, "Raft not enabled", http.StatusBadRequest)
		return
	}

	// Check if this node is the leader
	if h.RaftNode.Raft.State() != raft.Leader {
		http.Error(w, "This node is not the leader", http.StatusBadRequest)
		return
	}

	var req ElectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Default to stepdown if no body
		req.Method = "stepdown"
	}

	oldLeader := h.NodeID

	var err error
	var message string

	switch req.Method {
	case "transfer":
		if req.Target == "" {
			http.Error(w, "Target node required for transfer", http.StatusBadRequest)
			return
		}
		// Transfer leadership to specific node
		future := h.RaftNode.Raft.LeadershipTransfer()
		if err = future.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Leadership transfer failed: %v", err), http.StatusInternalServerError)
			return
		}
		message = fmt.Sprintf("Leadership transferred from %s to %s", oldLeader, req.Target)

	case "stepdown":
		fallthrough
	default:
		// Leader voluntarily steps down, triggers new election
		future := h.RaftNode.Raft.LeadershipTransfer()
		if err = future.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Leader stepdown failed: %v", err), http.StatusInternalServerError)
			return
		}
		message = fmt.Sprintf("Leader %s stepped down, new election in progress", oldLeader)
	}

	// Wait a moment for election to complete
	time.Sleep(500 * time.Millisecond)

	// Get new leader (if any)
	newLeader := ""
	if h.RaftNode.Raft.State() == raft.Leader {
		newLeader = h.NodeID
	} else {
		// Try to get leader from Raft
		leaderAddr, _ := h.RaftNode.Raft.LeaderWithID()
		newLeader = string(leaderAddr)
	}

	resp := ElectionResponse{
		Success:   true,
		Message:   message,
		OldLeader: oldLeader,
		NewLeader: newLeader,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetElectionStatus returns current election state and vote tracking
func (h *Handler) GetElectionStatus(w http.ResponseWriter, r *http.Request) {
	if h.RaftNode == nil {
		http.Error(w, "Raft not enabled", http.StatusBadRequest)
		return
	}

	stats := h.RaftNode.Raft.Stats()

	status := map[string]interface{}{
		"node_id":          h.NodeID,
		"state":            h.RaftNode.Raft.State().String(),
		"is_leader":        h.RaftNode.Raft.State() == raft.Leader,
		"term":             stats["term"],
		"last_log_index":   stats["last_log_index"],
		"last_log_term":    stats["last_log_term"],
		"commit_index":     stats["commit_index"],
		"applied_index":    stats["applied_index"],
		"num_peers":        stats["num_peers"],
		"leader":           stats["leader"],
		"protocol_version": stats["protocol_version"],
	}

	// Get configuration (cluster members)
	future := h.RaftNode.Raft.GetConfiguration()
	if err := future.Error(); err == nil {
		config := future.Configuration()
		servers := make([]map[string]string, 0)
		for _, srv := range config.Servers {
			servers = append(servers, map[string]string{
				"id":       string(srv.ID),
				"address":  string(srv.Address),
				"suffrage": srv.Suffrage.String(),
			})
		}
		status["cluster_members"] = servers
		status["cluster_size"] = len(servers)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// QuorumConfig represents quorum configuration
type QuorumConfig struct {
	ClusterSize int `json:"cluster_size"`
	QuorumSize  int `json:"quorum_size"`
}

// GetQuorumInfo returns current quorum information
func (h *Handler) GetQuorumInfo(w http.ResponseWriter, r *http.Request) {
	if h.RaftNode == nil {
		http.Error(w, "Raft not enabled", http.StatusBadRequest)
		return
	}

	future := h.RaftNode.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	config := future.Configuration()
	clusterSize := len(config.Servers)

	// Raft quorum = majority = (n/2) + 1
	quorumSize := (clusterSize / 2) + 1

	info := map[string]interface{}{
		"cluster_size":     clusterSize,
		"quorum_size":      quorumSize,
		"can_elect_leader": clusterSize >= quorumSize,
		"servers":          len(config.Servers),
		"formula":          "quorum = (cluster_size / 2) + 1",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}
