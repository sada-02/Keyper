package client

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/sada-02/keyper/shard"
)

// ShardedClient chooses a node by the key using consistent hashing, then delegates to Client.
type ShardedClient struct {
	baseClient *Client
	ring       *shard.Ring
}

// NewShardedClient creates a sharded client. Pass node HTTP addresses (e.g. "http://127.0.0.1:8080").
func NewShardedClient(nodes []string, replicas int) *ShardedClient {
	c := New(nodes)
	r := shard.NewRing(replicas)
	for _, n := range nodes {
		// ensure normalized URL (client.New also normalizes); keep it trimmed
		u := n
		// If user passed "127.0.0.1:8080" convert to http://...
		if _, err := url.ParseRequestURI(n); err != nil || (!startsWithHTTP(n) && !startsWithHTTPS(n)) {
			u = "http://" + n
		}
		candidate := u
		// ensure trailing slash removed
		if candidate[len(candidate)-1] == '/' {
			candidate = candidate[:len(candidate)-1]
		}
		r.AddNode(candidate)
	}
	return &ShardedClient{baseClient: c, ring: r}
}

func startsWithHTTP(s string) bool {
	return len(s) >= 4 && s[:4] == "http"
}
func startsWithHTTPS(s string) bool {
	return len(s) >= 5 && s[:5] == "https"
}

// Put stores a key by routing to the node responsible for key.
func (sc *ShardedClient) Put(key string, value []byte) error {
	path := "/v1/keys/" + url.PathEscape(key)

	node, ok := sc.ring.GetNode(key)
	if !ok {
		return fmt.Errorf("no nodes in ring")
	}

	// Try the selected node first (single-target).
	resp, err := sc.baseClient.DoRequestTo(node, "PUT", path, value, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// If follower redirected us, let cluster-aware DoRequest follow leader and retry.
	if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently {
		_ = resp.Body.Close()
		resp2, err := sc.baseClient.DoRequest("PUT", path, value, nil)
		if err != nil {
			return err
		}
		defer resp2.Body.Close()
		if resp2.StatusCode >= 200 && resp2.StatusCode < 300 {
			return nil
		}
		b, _ := io.ReadAll(resp2.Body)
		return fmt.Errorf("put failed status=%d body=%s", resp2.StatusCode, string(b))
	}

	// Normal handling for direct success/errors
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("put failed status=%d body=%s", resp.StatusCode, string(b))
}

// Get fetches from the node responsible for key.
func (sc *ShardedClient) Get(key string) ([]byte, error) {
	path := "/v1/keys/" + url.PathEscape(key)

	node, ok := sc.ring.GetNode(key)
	if !ok {
		return nil, fmt.Errorf("no nodes in ring")
	}

	resp, err := sc.baseClient.DoRequestTo(node, "GET", path, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// If redirected, let cluster-aware client follow the leader and retry.
	if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently {
		_ = resp.Body.Close()
		resp2, err := sc.baseClient.DoRequest("GET", path, nil, nil)
		if err != nil {
			return nil, err
		}
		defer resp2.Body.Close()
		if resp2.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("not found")
		}
		if resp2.StatusCode >= 200 && resp2.StatusCode < 300 {
			return io.ReadAll(resp2.Body)
		}
		b, _ := io.ReadAll(resp2.Body)
		return nil, fmt.Errorf("get failed status=%d body=%s", resp2.StatusCode, string(b))
	}

	if resp.StatusCode == http.StatusNotFound {
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
	path := "/v1/keys/" + url.PathEscape(key)

	node, ok := sc.ring.GetNode(key)
	if !ok {
		return fmt.Errorf("no nodes in ring")
	}

	resp, err := sc.baseClient.DoRequestTo(node, "DELETE", path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// If redirected, follow with cluster-aware client
	if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently {
		_ = resp.Body.Close()
		resp2, err := sc.baseClient.DoRequest("DELETE", path, nil, nil)
		if err != nil {
			return err
		}
		defer resp2.Body.Close()
		if resp2.StatusCode >= 200 && resp2.StatusCode < 300 {
			return nil
		}
		b, _ := io.ReadAll(resp2.Body)
		return fmt.Errorf("delete failed status=%d body=%s", resp2.StatusCode, string(b))
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("delete failed status=%d body=%s", resp.StatusCode, string(b))
}
