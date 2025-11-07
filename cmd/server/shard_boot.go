package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sada-02/keyper/config"
	"github.com/sada-02/keyper/httpapi"
	"github.com/sada-02/keyper/shard"
	"github.com/sada-02/keyper/shardraft"
)

// startShards starts per-shard raft instances for each shard that this node should host.
// It populates handler.ShardRafts with running shard servers.
// Returns a membership manager if control plane is configured.
func startShards(cfg *config.Config, h *httpapi.Handler) *shard.MembershipManager {
	// ensure shard manager present
	if h.ShardMgr == nil {
		h.ShardMgr = shard.NewManager()
	}
	if h.ShardRafts == nil {
		h.ShardRafts = make(map[string]*shardraft.ShardRaft)
	}

	// Determine which shards this node should host
	shardsToHost := parseAssignedShards(cfg.AssignedShards, cfg.ShardCount)

	log.Printf("Node %s will host %d shards: %v", cfg.NodeID, len(shardsToHost), shardsToHost)

	for _, i := range shardsToHost {
		shardID := strconv.Itoa(i)

		// Add this shard to the manager (marks it as hosted locally)
		h.ShardMgr.AddShard(shardID)

		// Each node needs a unique port for each shard's Raft instance
		// Calculate: base_port + (shard_id * 100) + node_offset
		// This allows up to 100 nodes per shard
		// Extract node number from nodeID (e.g., "node2" -> 2)
		nodeNum := 0
		fmt.Sscanf(cfg.NodeID, "node%d", &nodeNum)
		raftPort := cfg.RaftBasePort + (i * 100) + nodeNum
		raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)

		// Don't use joinAddr for shard Raft - let membership manager handle joins
		sr, err := shardraft.StartShardRaft(cfg.NodeID, shardID, raftAddr, cfg.DataDir, "")
		if err != nil {
			log.Printf("warning: unable to start shard raft %s at %s: %v", shardID, raftAddr, err)
			continue
		}
		h.ShardRafts[shardID] = sr
		log.Printf("started shard %s raft at %s (node id %s)", shardID, raftAddr, sr.Node.ID)

		// Register this shard with the control plane for membership coordination
		if cfg.ControlPlaneAddr != "" {
			if err := registerShardWithControlPlane(cfg.ControlPlaneAddr, shardID, sr.Node.ID, raftAddr); err != nil {
				log.Printf("warning: failed to register shard %s with control plane: %v", shardID, err)
			} else {
				log.Printf("registered shard %s (node %s, addr %s) with control plane", shardID, sr.Node.ID, raftAddr)
			}
		}
	}

	// Initialize membership manager if control plane is configured
	var membershipMgr *shard.MembershipManager
	if cfg.ControlPlaneAddr != "" {
		membershipMgr = shard.NewMembershipManager(cfg.NodeID, cfg.ControlPlaneAddr, 30*time.Second)

		// Register all shards with the membership manager
		for shardID, sr := range h.ShardRafts {
			membershipMgr.RegisterShard(shardID, sr)
		}

		// Start the reconciliation loop
		membershipMgr.Start()
		log.Printf("Started automatic Raft membership management (control plane: %s)", cfg.ControlPlaneAddr)

		// Trigger immediate reconciliation instead of waiting 30 seconds
		go func() {
			time.Sleep(2 * time.Second) // Give nodes time to register
			membershipMgr.ReconcileNow()
			log.Printf("Triggered immediate membership reconciliation")
		}()
	}

	return membershipMgr
}

// parseAssignedShards converts the AssignedShards config string into a list of shard IDs.
// If assignedStr is empty, returns all shards (0 to shardCount-1) for backward compatibility.
// Format: comma-separated integers, e.g., "0,2,3"
func parseAssignedShards(assignedStr string, shardCount int) []int {
	if assignedStr == "" {
		// Default behavior: host all shards (backward compatible)
		result := make([]int, shardCount)
		for i := 0; i < shardCount; i++ {
			result[i] = i
		}
		return result
	}

	// Parse comma-separated list
	parts := strings.Split(assignedStr, ",")
	result := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		id, err := strconv.Atoi(part)
		if err != nil {
			log.Printf("warning: invalid shard ID '%s' in assigned-shards, skipping", part)
			continue
		}

		if id < 0 || id >= shardCount {
			log.Printf("warning: shard ID %d out of range [0,%d), skipping", id, shardCount)
			continue
		}

		result = append(result, id)
	}

	return result
}

// registerShardWithControlPlane notifies the control plane about this shard's membership
func registerShardWithControlPlane(controlPlaneAddr, shardID, nodeID, raftAddr string) error {
	// Get current members for this shard
	getURL := fmt.Sprintf("http://%s/v1/control/shards/%s/members", controlPlaneAddr, shardID)
	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get(getURL)
	if err != nil {
		return fmt.Errorf("get members: %w", err)
	}
	defer resp.Body.Close()

	var existingData struct {
		Members []struct {
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
		} `json:"members"`
	}

	// Parse existing members (or empty if not found)
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&existingData); err != nil {
			return fmt.Errorf("decode members: %w", err)
		}
	}

	// Check if we're already registered
	alreadyExists := false
	for _, member := range existingData.Members {
		if member.NodeID == nodeID {
			alreadyExists = true
			break
		}
	}

	if alreadyExists {
		return nil // Already registered
	}

	// Add ourselves to the member list
	existingData.Members = append(existingData.Members, struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}{
		NodeID:   nodeID,
		RaftAddr: raftAddr,
	})

	// POST updated member list back
	postURL := fmt.Sprintf("http://%s/v1/control/shards/%s/members", controlPlaneAddr, shardID)
	payload, err := json.Marshal(existingData)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, postURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp2, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post members: %w", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusNoContent && resp2.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp2.StatusCode)
	}

	return nil
}
