package client

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/sada-02/keyper/shard"
)

// ShardedClient chooses a node by the key using consistent hashing or explicit shard mapping.
// It delegates to Client for actual HTTP calls.
// Supports control-plane based discovery for dynamic node routing.
type ShardedClient struct {
	baseClient   *Client
	ring         *shard.Ring       // optional: old ring-based routing
	shardCount   int               // if >0 use KeyToShard mapping
	nodesByShard map[string]string // map shardID -> nodeURL (e.g. "0"->"http://127.0.0.1:8080")
	discovery    *DiscoveryClient  // optional: control plane discovery
}

// NewShardedClientUsingRing (existing) - unchanged constructor for ring approach
func NewShardedClient(nodes []string, replicas int) *ShardedClient {
	c := New(nodes)
	r := shard.NewRing(replicas)
	for _, n := range nodes {
		u := n
		// normalize
		if _, err := url.ParseRequestURI(n); err != nil || (len(n) >= 4 && (n[:4] != "http") && (n[:5] != "https")) {
			u = "http://" + n
		}
		candidate := u
		if candidate[len(candidate)-1] == '/' {
			candidate = candidate[:len(candidate)-1]
		}
		r.AddNode(candidate)
	}
	return &ShardedClient{baseClient: c, ring: r}
}

// NewShardedClientByShardCount constructs a ShardedClient that maps key -> shard using shard.KeyToShard.
// nodes is a map from shardID (string) to node base URL (e.g. "0"->"http://127.0.0.1:8080").
// shardCount is required to compute key->shard.
func NewShardedClientByShardCount(nodesByShard map[string]string, shardCount int) *ShardedClient {
	// collect unique node addresses for internal base client
	unique := map[string]struct{}{}
	addrs := []string{}
	for _, u := range nodesByShard {
		if _, ok := unique[u]; !ok {
			unique[u] = struct{}{}
			addrs = append(addrs, u)
		}
	}
	c := New(addrs)
	return &ShardedClient{
		baseClient:   c,
		shardCount:   shardCount,
		nodesByShard: nodesByShard,
	}
}

// NewShardedClientWithDiscovery creates a client that uses control plane for discovery
// This is the recommended approach for production - clients don't need to know node addresses
func NewShardedClientWithDiscovery(controlPlaneURL string, shardCount int) *ShardedClient {
	discovery := NewDiscoveryClient(controlPlaneURL)

	// Create base client with empty address list (will use discovery)
	baseClient := New([]string{})

	return &ShardedClient{
		baseClient:   baseClient,
		shardCount:   shardCount,
		discovery:    discovery,
		nodesByShard: make(map[string]string),
	}
}

// SetDiscoveryClient allows updating the discovery client (for testing)
func (sc *ShardedClient) SetDiscoveryClient(dc *DiscoveryClient) {
	sc.discovery = dc
}

// helper: pick node for a key
func (sc *ShardedClient) nodeForKey(key string) (string, error) {
	// 1) if shardCount set, use KeyToShard
	if sc.shardCount > 0 {
		shardID := shard.KeyToShard(key, sc.shardCount)
		if shardID == "" {
			return "", fmt.Errorf("invalid shardCount")
		}

		// Try discovery first if available
		if sc.discovery != nil {
			nodes, err := sc.discovery.DiscoverNodesForShard(shardID)
			if err == nil && len(nodes) > 0 {
				// Return first node (could add leader selection logic here)
				return nodes[0], nil
			}
			// Fall through to static mapping if discovery fails
		}

		// Try static mapping
		node, ok := sc.nodesByShard[shardID]
		if !ok {
			return "", fmt.Errorf("no node for shard %s", shardID)
		}
		return node, nil
	}
	// 2) fallback to ring (existing behavior)
	if sc.ring != nil {
		n, ok := sc.ring.GetNode(key)
		if ok {
			return n, nil
		}
	}
	return "", fmt.Errorf("no nodes available")
}

// Put stores a key by routing to the node responsible for key.
// Automatically handles redirects and leader discovery.
func (sc *ShardedClient) Put(key string, value []byte) error {
	node, err := sc.nodeForKey(key)
	if err != nil {
		return err
	}
	resp, err := sc.baseClient.DoRequestTo(node, "PUT", "/v1/keys/"+url.PathEscape(key), value, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Handle 307 redirect (wrong node for shard)
	if resp.StatusCode == http.StatusTemporaryRedirect {
		if shardID := resp.Header.Get("X-Shard-ID"); shardID != "" && sc.discovery != nil {
			// Re-discover nodes for this shard
			sc.discovery.InvalidateCache()
			nodes, err := sc.discovery.DiscoverNodesForShard(shardID)
			if err == nil && len(nodes) > 0 {
				// Retry with discovered node
				resp2, err := sc.baseClient.DoRequestTo(nodes[0], "PUT", "/v1/keys/"+url.PathEscape(key), value, nil)
				if err != nil {
					return err
				}
				defer resp2.Body.Close()
				if resp2.StatusCode >= 200 && resp2.StatusCode < 300 {
					return nil
				}
				b, _ := io.ReadAll(resp2.Body)
				return fmt.Errorf("put failed after redirect status=%d body=%s", resp2.StatusCode, string(b))
			}
		}
		return fmt.Errorf("redirect to different shard but discovery failed")
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("put failed status=%d body=%s", resp.StatusCode, string(b))
}

// Get fetches from the node responsible for key.
// Automatically handles redirects and leader discovery.
func (sc *ShardedClient) Get(key string) ([]byte, error) {
	node, err := sc.nodeForKey(key)
	if err != nil {
		return nil, err
	}
	resp, err := sc.baseClient.DoRequestTo(node, "GET", "/v1/keys/"+url.PathEscape(key), nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Handle 307 redirect (wrong node for shard)
	if resp.StatusCode == http.StatusTemporaryRedirect {
		if shardID := resp.Header.Get("X-Shard-ID"); shardID != "" && sc.discovery != nil {
			// Re-discover nodes for this shard
			sc.discovery.InvalidateCache()
			nodes, err := sc.discovery.DiscoverNodesForShard(shardID)
			if err == nil && len(nodes) > 0 {
				// Retry with discovered node
				resp2, err := sc.baseClient.DoRequestTo(nodes[0], "GET", "/v1/keys/"+url.PathEscape(key), nil, nil)
				if err != nil {
					return nil, err
				}
				defer resp2.Body.Close()
				if resp2.StatusCode == 404 {
					return nil, fmt.Errorf("not found")
				}
				if resp2.StatusCode >= 200 && resp2.StatusCode < 300 {
					return io.ReadAll(resp2.Body)
				}
				b, _ := io.ReadAll(resp2.Body)
				return nil, fmt.Errorf("get failed after redirect status=%d body=%s", resp2.StatusCode, string(b))
			}
		}
		return nil, fmt.Errorf("redirect to different shard but discovery failed")
	}

	if resp.StatusCode == 404 || resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("not found")
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return io.ReadAll(resp.Body)
	}
	b, _ := io.ReadAll(resp.Body)
	return nil, fmt.Errorf("get failed status=%d body=%s", resp.StatusCode, string(b))
}

// Delete deletes key on its node.
func (sc *ShardedClient) Delete(key string) error {
	node, err := sc.nodeForKey(key)
	if err != nil {
		return err
	}
	resp, err := sc.baseClient.DoRequestTo(node, "DELETE", "/v1/keys/"+url.PathEscape(key), nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("delete failed status=%d body=%s", resp.StatusCode, string(b))
}
