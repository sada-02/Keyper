package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/sada-02/keyper/config"
	"github.com/sada-02/keyper/httpapi"
	"github.com/sada-02/keyper/shard"
	"github.com/sada-02/keyper/shardraft"
)

// startShards starts per-shard raft instances for each shard that this node should host.
// It populates handler.ShardRafts with running shard servers.
func startShards(cfg *config.Config, h *httpapi.Handler) {
	// ensure shard manager present
	if h.ShardMgr == nil {
		h.ShardMgr = shard.NewManager()
	}
	if h.ShardRafts == nil {
		h.ShardRafts = make(map[string]*shardraft.ShardRaft)
	}

	for i := 0; i < cfg.ShardCount; i++ {
		shardID := strconv.Itoa(i)

		// For this MVP we host all shards on every node if ShardCount > 0.
		// In a later step, you'll host only assigned shards.
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
}
