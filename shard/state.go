package shard

import (
	"sync"
)

// ShardState tracks the operational state of a shard
type ShardState struct {
	mu       sync.RWMutex
	ReadOnly bool // true = reject writes during migration
	Paused   bool // true = reject all operations
}

// NewShardState creates a new shard state (read-write by default)
func NewShardState() *ShardState {
	return &ShardState{
		ReadOnly: false,
		Paused:   false,
	}
}

// SetReadOnly sets the shard to read-only mode
func (s *ShardState) SetReadOnly(readonly bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ReadOnly = readonly
}

// IsReadOnly returns true if shard is in read-only mode
func (s *ShardState) IsReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ReadOnly
}

// SetPaused sets the shard to paused mode (no operations allowed)
func (s *ShardState) SetPaused(paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Paused = paused
}

// IsPaused returns true if shard is paused
func (s *ShardState) IsPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Paused
}

// CanWrite returns true if writes are allowed
func (s *ShardState) CanWrite() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.ReadOnly && !s.Paused
}

// CanRead returns true if reads are allowed
func (s *ShardState) CanRead() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.Paused
}
