package shardraft

import (
	"time"

	raft "github.com/hashicorp/raft"
)

// Ensure ShardRaft implements the ShardRaftNode interface
// This adapter provides the methods needed by shard.MembershipManager

// AddVoter adds a voting member to this shard's Raft cluster
func (sr *ShardRaft) AddVoter(nodeID, raftAddr string, timeout time.Duration) error {
	return sr.Node.AddVoter(nodeID, raftAddr, timeout)
}

// RemoveServer removes a member from this shard's Raft cluster
func (sr *ShardRaft) RemoveServer(nodeID string, timeout time.Duration) error {
	return sr.Node.RemoveServer(nodeID, timeout)
}

// IsLeader returns true if this node is the Raft leader for this shard
func (sr *ShardRaft) IsLeader() bool {
	return sr.Node.Raft.State() == raft.Leader
}

// GetShardID returns the shard ID this Raft instance manages
func (sr *ShardRaft) GetShardID() string {
	return sr.ShardID
}
