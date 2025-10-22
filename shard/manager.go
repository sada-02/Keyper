package shard

import (
	"sync"
)

// ShardManager keeps track of shard IDs hosted on this node.
type ShardManager struct {
	mu     sync.RWMutex
	shards map[string]struct{} // map[shardID]present
}

// NewManager creates an empty manager.
func NewManager() *ShardManager {
	return &ShardManager{
		shards: make(map[string]struct{}),
	}
}

// AddShard marks this node as host for shardID.
func (m *ShardManager) AddShard(shardID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[shardID] = struct{}{}
}

// RemoveShard unmarks shardID.
func (m *ShardManager) RemoveShard(shardID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.shards, shardID)
}

// HasShard returns true if this node hosts the shard.
func (m *ShardManager) HasShard(shardID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.shards[shardID]
	return ok
}

// List returns all shard IDs hosted locally.
func (m *ShardManager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.shards))
	for s := range m.shards {
		out = append(out, s)
	}
	return out
}
