package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// DiscoveryClient provides control-plane based service discovery
// Clients use this to find which nodes host which shards
type DiscoveryClient struct {
	controlPlaneURL string
	httpClient      *http.Client

	// Caches
	mu              sync.RWMutex
	nodeCache       map[string]*NodeInfo // nodeID -> NodeInfo
	shardCache      map[string][]string  // shardID -> list of node HTTP addresses
	raftToHTTPCache map[string]string    // raft address -> HTTP address
	cacheExpiry     time.Time
	cacheTTL        time.Duration
}

// NodeInfo represents a node's addresses
type NodeInfo struct {
	NodeID   string `json:"node_id"`
	HTTPAddr string `json:"http_addr"`
	RaftAddr string `json:"raft_addr"`
}

// NewDiscoveryClient creates a discovery client for the control plane
func NewDiscoveryClient(controlPlaneURL string) *DiscoveryClient {
	if !strings.HasPrefix(controlPlaneURL, "http://") && !strings.HasPrefix(controlPlaneURL, "https://") {
		controlPlaneURL = "http://" + controlPlaneURL
	}
	controlPlaneURL = strings.TrimRight(controlPlaneURL, "/")

	return &DiscoveryClient{
		controlPlaneURL: controlPlaneURL,
		httpClient:      &http.Client{Timeout: 5 * time.Second},
		nodeCache:       make(map[string]*NodeInfo),
		shardCache:      make(map[string][]string),
		raftToHTTPCache: make(map[string]string),
		cacheTTL:        30 * time.Second, // Cache for 30 seconds
	}
}

// DiscoverNodesForShard queries the control plane to find which nodes host a shard
// Returns list of HTTP addresses that host the shard
func (dc *DiscoveryClient) DiscoverNodesForShard(shardID string) ([]string, error) {
	// Check cache first
	dc.mu.RLock()
	if time.Now().Before(dc.cacheExpiry) {
		if nodes, ok := dc.shardCache[shardID]; ok {
			dc.mu.RUnlock()
			return nodes, nil
		}
	}
	dc.mu.RUnlock()

	// Query control plane: GET /v1/control/shards/get?shard_id={shardID}
	reqURL := fmt.Sprintf("%s/v1/control/shards/get?shard_id=%s",
		dc.controlPlaneURL, url.QueryEscape(shardID))

	resp, err := dc.httpClient.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("query control plane: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("shard %s not found in control plane", shardID)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("control plane error %d: %s", resp.StatusCode, string(body))
	}

	// Response is JSON array of node addresses
	var nodes []string
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Update cache
	dc.mu.Lock()
	dc.shardCache[shardID] = nodes
	dc.cacheExpiry = time.Now().Add(dc.cacheTTL)
	dc.mu.Unlock()

	return nodes, nil
}

// DiscoverAllNodes queries the control plane for all registered nodes
// Updates the node cache with their metadata
func (dc *DiscoveryClient) DiscoverAllNodes() ([]*NodeInfo, error) {
	reqURL := fmt.Sprintf("%s/v1/control/nodes", dc.controlPlaneURL)

	resp, err := dc.httpClient.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("query nodes: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("control plane error %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Nodes []*NodeInfo `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Update caches
	dc.mu.Lock()
	dc.nodeCache = make(map[string]*NodeInfo)
	dc.raftToHTTPCache = make(map[string]string)
	for _, node := range response.Nodes {
		dc.nodeCache[node.NodeID] = node
		dc.raftToHTTPCache[node.RaftAddr] = node.HTTPAddr
	}
	dc.cacheExpiry = time.Now().Add(dc.cacheTTL)
	dc.mu.Unlock()

	return response.Nodes, nil
}

// ResolveRaftToHTTP converts a Raft address to HTTP address using node metadata
// This is used when a node returns X-Raft-Leader header
func (dc *DiscoveryClient) ResolveRaftToHTTP(raftAddr string) (string, error) {
	// Check cache first
	dc.mu.RLock()
	if httpAddr, ok := dc.raftToHTTPCache[raftAddr]; ok {
		dc.mu.RUnlock()
		return httpAddr, nil
	}
	dc.mu.RUnlock()

	// Cache miss - refresh node list
	if _, err := dc.DiscoverAllNodes(); err != nil {
		return "", fmt.Errorf("refresh nodes: %w", err)
	}

	// Try cache again
	dc.mu.RLock()
	httpAddr, ok := dc.raftToHTTPCache[raftAddr]
	dc.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("no HTTP address found for raft address %s", raftAddr)
	}

	return httpAddr, nil
}

// GetNodeByID returns node metadata for a specific node ID
func (dc *DiscoveryClient) GetNodeByID(nodeID string) (*NodeInfo, error) {
	dc.mu.RLock()
	if time.Now().Before(dc.cacheExpiry) {
		if node, ok := dc.nodeCache[nodeID]; ok {
			dc.mu.RUnlock()
			return node, nil
		}
	}
	dc.mu.RUnlock()

	// Refresh cache
	if _, err := dc.DiscoverAllNodes(); err != nil {
		return nil, err
	}

	dc.mu.RLock()
	node, ok := dc.nodeCache[nodeID]
	dc.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	return node, nil
}

// InvalidateCache clears all cached data, forcing fresh queries
func (dc *DiscoveryClient) InvalidateCache() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.shardCache = make(map[string][]string)
	dc.nodeCache = make(map[string]*NodeInfo)
	dc.raftToHTTPCache = make(map[string]string)
	dc.cacheExpiry = time.Time{}
}

// SetCacheTTL changes the cache time-to-live duration
func (dc *DiscoveryClient) SetCacheTTL(ttl time.Duration) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.cacheTTL = ttl
}
