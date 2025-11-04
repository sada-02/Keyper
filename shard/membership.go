package shard

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// MembershipManager handles automatic per-shard Raft membership coordination
type MembershipManager struct {
	mu sync.RWMutex

	// nodeID is this node's identifier
	nodeID string

	// controlPlaneAddr is the HTTP address of the control plane
	controlPlaneAddr string

	// shardRafts maps shard ID to Raft node interface
	shardRafts map[string]ShardRaftNode

	// currentMembers tracks known members per shard
	// map[shardID]map[nodeID]raftAddr
	currentMembers map[string]map[string]string

	// pollInterval for checking control plane
	pollInterval time.Duration

	// stopCh for graceful shutdown
	stopCh chan struct{}
}

// ShardRaftNode interface abstracts the Raft operations we need
type ShardRaftNode interface {
	// AddVoter adds a voting member to the Raft cluster
	AddVoter(nodeID, raftAddr string, timeout time.Duration) error

	// RemoveServer removes a member from the Raft cluster
	RemoveServer(nodeID string, timeout time.Duration) error

	// IsLeader returns true if this node is the Raft leader
	IsLeader() bool

	// GetShardID returns the shard ID this Raft manages
	GetShardID() string
}

// NewMembershipManager creates a new membership coordinator
func NewMembershipManager(nodeID, controlPlaneAddr string, pollInterval time.Duration) *MembershipManager {
	return &MembershipManager{
		nodeID:           nodeID,
		controlPlaneAddr: controlPlaneAddr,
		shardRafts:       make(map[string]ShardRaftNode),
		currentMembers:   make(map[string]map[string]string),
		pollInterval:     pollInterval,
		stopCh:           make(chan struct{}),
	}
}

// RegisterShard registers a shard's Raft instance for management
func (m *MembershipManager) RegisterShard(shardID string, raftNode ShardRaftNode) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.shardRafts[shardID] = raftNode
	if m.currentMembers[shardID] == nil {
		m.currentMembers[shardID] = make(map[string]string)
	}

	log.Printf("[MembershipMgr] Registered shard %s for membership management", shardID)
}

// Start begins the membership reconciliation loop
func (m *MembershipManager) Start() {
	if m.controlPlaneAddr == "" {
		log.Println("[MembershipMgr] No control plane configured, skipping automatic membership")
		return
	}

	go m.reconciliationLoop()
	log.Printf("[MembershipMgr] Started membership reconciliation (poll interval: %v)", m.pollInterval)
}

// Stop gracefully stops the membership manager
func (m *MembershipManager) Stop() {
	close(m.stopCh)
	log.Println("[MembershipMgr] Stopped membership reconciliation")
}

// reconciliationLoop periodically syncs membership with control plane
func (m *MembershipManager) reconciliationLoop() {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.reconcileAllShards()
		case <-m.stopCh:
			return
		}
	}
}

// reconcileAllShards syncs membership for all registered shards
func (m *MembershipManager) reconcileAllShards() {
	m.mu.RLock()
	shards := make([]string, 0, len(m.shardRafts))
	for shardID := range m.shardRafts {
		shards = append(shards, shardID)
	}
	m.mu.RUnlock()

	for _, shardID := range shards {
		if err := m.reconcileShard(shardID); err != nil {
			log.Printf("[MembershipMgr] Failed to reconcile shard %s: %v", shardID, err)
		}
	}
}

// reconcileShard syncs a single shard's membership with control plane
func (m *MembershipManager) reconcileShard(shardID string) error {
	m.mu.RLock()
	raftNode, exists := m.shardRafts[shardID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("shard %s not registered", shardID)
	}

	// Only the Raft leader should modify membership
	if !raftNode.IsLeader() {
		return nil // Not an error, just skip
	}

	// Query control plane for desired membership
	desiredMembers, err := m.queryControlPlane(shardID)
	if err != nil {
		return fmt.Errorf("query control plane: %w", err)
	}

	// Get current known members
	m.mu.RLock()
	currentMembers := make(map[string]string)
	for k, v := range m.currentMembers[shardID] {
		currentMembers[k] = v
	}
	m.mu.RUnlock()

	// Calculate changes
	toAdd := make(map[string]string)    // nodeID -> raftAddr
	toRemove := make(map[string]string) // nodeID -> raftAddr

	// Find members to add (in desired but not in current)
	for nodeID, raftAddr := range desiredMembers {
		if nodeID == m.nodeID {
			continue // Don't add ourselves
		}
		if _, exists := currentMembers[nodeID]; !exists {
			toAdd[nodeID] = raftAddr
		}
	}

	// Find members to remove (in current but not in desired)
	for nodeID, raftAddr := range currentMembers {
		if nodeID == m.nodeID {
			continue // Don't remove ourselves
		}
		if _, exists := desiredMembers[nodeID]; !exists {
			toRemove[nodeID] = raftAddr
		}
	}

	// Apply changes
	if len(toAdd) == 0 && len(toRemove) == 0 {
		return nil // No changes needed
	}

	log.Printf("[MembershipMgr] Reconciling shard %s: +%d members, -%d members",
		shardID, len(toAdd), len(toRemove))

	// Add new members first (safer - increases redundancy)
	for nodeID, raftAddr := range toAdd {
		if err := raftNode.AddVoter(nodeID, raftAddr, 10*time.Second); err != nil {
			log.Printf("[MembershipMgr] Failed to add %s to shard %s: %v", nodeID, shardID, err)
			continue
		}

		m.mu.Lock()
		m.currentMembers[shardID][nodeID] = raftAddr
		m.mu.Unlock()

		log.Printf("[MembershipMgr] Added %s (%s) to shard %s Raft cluster", nodeID, raftAddr, shardID)
	}

	// Remove old members (after adds succeed)
	for nodeID := range toRemove {
		if err := raftNode.RemoveServer(nodeID, 10*time.Second); err != nil {
			log.Printf("[MembershipMgr] Failed to remove %s from shard %s: %v", nodeID, shardID, err)
			continue
		}

		m.mu.Lock()
		delete(m.currentMembers[shardID], nodeID)
		m.mu.Unlock()

		log.Printf("[MembershipMgr] Removed %s from shard %s Raft cluster", nodeID, shardID)
	}

	return nil
}

// queryControlPlane fetches desired shard membership from control plane
// Returns map[nodeID]raftAddr
func (m *MembershipManager) queryControlPlane(shardID string) (map[string]string, error) {
	// Query control plane for shard membership
	url := fmt.Sprintf("http://%s/v1/control/shards/%s/members", m.controlPlaneAddr, shardID)

	resp, err := httpGet(url, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}

		members, err := ParseMembershipResponse(body)
		if err != nil {
			return nil, fmt.Errorf("parse response: %w", err)
		}

		return members, nil
	}

	return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// httpGet performs an HTTP GET with timeout
func httpGet(url string, timeout time.Duration) (*http.Response, error) {
	client := &http.Client{Timeout: timeout}
	return client.Get(url)
}

// ForceReconcile immediately triggers reconciliation for all shards (for testing)
func (m *MembershipManager) ForceReconcile() {
	m.reconcileAllShards()
}

// GetCurrentMembers returns the current known members for a shard (for debugging)
func (m *MembershipManager) GetCurrentMembers(shardID string) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string)
	for k, v := range m.currentMembers[shardID] {
		result[k] = v
	}
	return result
}

// MembershipInfo represents the membership state for a shard
type MembershipInfo struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
}

// ParseMembershipResponse parses control plane membership response
func ParseMembershipResponse(data []byte) (map[string]string, error) {
	var response struct {
		Members []MembershipInfo `json:"members"`
	}

	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, member := range response.Members {
		result[member.NodeID] = member.RaftAddr
	}

	return result, nil
}
