package main

import (
	"fmt"
	"log"
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

		raftPort := cfg.RaftBasePort + i
		raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)

		sr, err := shardraft.StartShardRaft(cfg.NodeID, shardID, raftAddr, cfg.DataDir, cfg.JoinAddr)
		if err != nil {
			log.Printf("warning: unable to start shard raft %s at %s: %v", shardID, raftAddr, err)
			continue
		}
		h.ShardRafts[shardID] = sr
		log.Printf("started shard %s raft at %s (node id %s)", shardID, raftAddr, sr.Node.ID)
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
