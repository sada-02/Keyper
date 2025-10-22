package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Client is a simple cluster-aware HTTP client for the Keyper API.
type Client struct {
	addrs     []string      // initial candidate addresses
	http      *http.Client  // underlying HTTP client
	mu        sync.RWMutex  // protects leader
	leader    string        // cached leader base URL (e.g. "http://127.0.0.1:8080")
	tryLimit  int           // number of nodes to try before giving up
	retryWait time.Duration // wait between retries
}

// New creates a Client. Provide at least one node HTTP address.
func New(addrs []string) *Client {
	unique := make([]string, 0, len(addrs))
	seen := map[string]struct{}{}
	for _, a := range addrs {
		if a == "" {
			continue
		}
		// ensure each address has a scheme
		if !strings.HasPrefix(a, "http://") && !strings.HasPrefix(a, "https://") {
			a = "http://" + a
		}
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		unique = append(unique, strings.TrimRight(a, "/"))
	}

	c := &Client{
		addrs: unique,
		http: &http.Client{
			Timeout: 6 * time.Second,
		},
		tryLimit:  len(unique),
		retryWait: 300 * time.Millisecond,
	}
	if c.tryLimit == 0 {
		c.tryLimit = 1
	}
	return c
}

// SetHTTPClient lets you provide a custom http.Client (optional).
func (c *Client) SetHTTPClient(h *http.Client) {
	c.http = h
}

// helper: get currently cached leader (may be empty)
func (c *Client) getLeader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

func (c *Client) setLeader(u string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = strings.TrimRight(u, "/")
}

// DoRequest tries to perform the request on the cluster nodes and follows leader redirects.
// method: "GET", "PUT", "DELETE"
// path: path starting with / (eg "/v1/keys/foo")
// body: may be nil
// returns resp *http.Response (caller must close) or error
func (c *Client) DoRequest(method, path string, body []byte, headers map[string]string) (*http.Response, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// If we have a cached leader, try it first.
	leader := c.getLeader()
	if leader != "" {
		if resp, err := c.doOnce(leader, method, path, body, headers); err == nil {
			return resp, nil
		} else if isTemporaryRedirect(resp, err) {
			// handle redirect below (resp may be non-nil)
			if resp != nil {
				if newLeader := resp.Header.Get("X-Raft-Leader"); newLeader != "" {
					// normalize leader to http URL if necessary
					newURL := normalizeLeaderAddr(newLeader)
					c.setLeader(newURL)
					_ = resp.Body.Close()
					return c.doOnce(newURL, method, path, body, headers)
				}
			}
		}
		// otherwise, fallthrough to trying full list
	}

	// Try each address up to tryLimit, following leader header when returned.
	try := 0
	for _, base := range c.addrs {
		try++
		if try > c.tryLimit {
			break
		}
		resp, err := c.doOnce(base, method, path, body, headers)
		if err != nil {
			// try next node
			time.Sleep(c.retryWait)
			continue
		}
		// If server redirected us to leader, update cached leader and retry once
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently {
			newLeader := resp.Header.Get("X-Raft-Leader")
			_ = resp.Body.Close()
			if newLeader == "" {
				// no header: return redirect as-is
				return resp, nil
			}
			newURL := normalizeLeaderAddr(newLeader)
			c.setLeader(newURL)
			return c.doOnce(newURL, method, path, body, headers)
		}
		// success or other status; caller will inspect status
		return resp, nil
	}

	return nil, errors.New("all nodes failed or unreachable")
}

// DoRequestTo performs a single request against the specified base URL (e.g. "http://127.0.0.1:8080")
// It does not try the entire cluster — just a single base. Caller must close the returned response.
func (c *Client) DoRequestTo(base, method, path string, body []byte, headers map[string]string) (*http.Response, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	base = strings.TrimRight(base, "/")
	return c.doOnce(base, method, path, body, headers)
}

// doOnce performs a single HTTP request against base + path. It returns response or error.
// Caller must close response body.
func (c *Client) doOnce(base, method, path string, body []byte, headers map[string]string) (*http.Response, error) {
	urlStr := strings.TrimRight(base, "/") + path
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, urlStr, bodyReader)
	if err != nil {
		return nil, err
	}
	// default headers
	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}
	// Make request
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func isTemporaryRedirect(resp *http.Response, err error) bool {
	if err != nil {
		return false
	}
	if resp == nil {
		return false
	}
	if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently {
		return true
	}
	return false
}

// normalizeLeaderAddr converts a leader identifier into an HTTP base URL.
// If leader looks like "http://..." or "https://..." it is returned trimmed.
// If leader looks like "host:port", this function returns "http://host:8080"
// (default HTTP port 8080). If leader contains only host (no port) returns "http://host".
func normalizeLeaderAddr(leader string) string {
	leader = strings.TrimSpace(leader)
	if leader == "" {
		return ""
	}
	if strings.HasPrefix(leader, "http://") || strings.HasPrefix(leader, "https://") {
		return strings.TrimRight(leader, "/")
	}
	// try to parse host:port
	if strings.Contains(leader, ":") {
		// assume http on default port 8080
		parts := strings.Split(leader, ":")
		host := parts[0]
		return "http://" + host + ":8080"
	}
	// only host
	return "http://" + leader
}

// High-level helper APIs

// Put stores a key. Returns nil on success (2xx or 204).
func (c *Client) Put(key string, value []byte) error {
	path := "/v1/keys/" + url.PathEscape(key)
	resp, err := c.DoRequest(http.MethodPut, path, value, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("put failed: status=%d body=%s", resp.StatusCode, string(b))
}

// Get fetches a key value. Returns the raw bytes or error.
func (c *Client) Get(key string) ([]byte, error) {
	path := "/v1/keys/" + url.PathEscape(key)
	resp, err := c.DoRequest(http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("not found")
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return io.ReadAll(resp.Body)
	}
	b, _ := io.ReadAll(resp.Body)
	return nil, fmt.Errorf("get failed status=%d body=%s", resp.StatusCode, string(b))
}

// Delete removes a key.
func (c *Client) Delete(key string) error {
	path := "/v1/keys/" + url.PathEscape(key)
	resp, err := c.DoRequest(http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("delete failed: status=%d body=%s", resp.StatusCode, string(b))
}

// Status queries a single node's /v1/status (tries leader cached first).
func (c *Client) Status() (string, error) {
	resp, err := c.DoRequest(http.MethodGet, "/v1/status", nil, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), nil
}
