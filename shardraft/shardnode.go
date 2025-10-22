package shardraft

import (
	"fmt"
	"path/filepath"

	"github.com/sada-02/keyper/raft"
	"github.com/sada-02/keyper/store"
)

// ShardRaft wraps a raftnode.Node for a specific shard.
type ShardRaft struct {
	ShardID string
	Node    *raftnode.Node
	Store   *store.BadgerStore
}

// StartShardRaft starts a raft instance for shardID on this node.
// - nodeBaseID: the node's base ID (e.g. "node1").
// - raftAddr: the raft listen address for the shard (host:port).
// - dataDir: base data dir; shard data will live in dataDir/shards/<shardID>
// - joinAddr: optional join HTTP address to add this raft server (leader); can be empty to bootstrap single-node.
func StartShardRaft(nodeBaseID, shardID, raftAddr, dataDir, joinAddr string) (*ShardRaft, error) {
	shardDataDir := filepath.Join(dataDir, "shards", shardID)

	// open per-shard Badger store
	st, err := store.NewBadgerStore(shardDataDir)
	if err != nil {
		return nil, fmt.Errorf("open shard store %s: %w", shardID, err)
	}

	// Build a unique node id per shard: "<nodeBaseID>-shard-<shardID>"
	nodeID := fmt.Sprintf("%s-shard-%s", nodeBaseID, shardID)

	raftCfg := &raftnode.RaftConfig{
		NodeID:   nodeID,
		RaftAddr: raftAddr,
		DataDir:  shardDataDir,
		Store:    st,
		JoinAddr: joinAddr,
	}

	node, err := raftnode.NewNode(raftCfg)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("start raft for shard %s: %w", shardID, err)
	}

	return &ShardRaft{
		ShardID: shardID,
		Node:    node,
		Store:   st,
	}, nil
}

// Shutdown shuts down the underlying raft node and closes store.
func (sr *ShardRaft) Shutdown() {
	if sr.Node != nil && sr.Node.Raft != nil {
		_ = sr.Node.Raft.Shutdown()
	}
	if sr.Store != nil {
		_ = sr.Store.Close()
	}
}
