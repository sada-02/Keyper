package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sada-02/keyper/config"
	"github.com/sada-02/keyper/httpapi"
	"github.com/sada-02/keyper/metrics"
	raftnode "github.com/sada-02/keyper/raft"
	"github.com/sada-02/keyper/shard"
	shardraft "github.com/sada-02/keyper/shardraft"
	"github.com/sada-02/keyper/store"
)

func main() {
	cfg := config.Load()

	// Create data dir if not exists
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	st, err := store.NewBadgerStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer func() {
		_ = st.Close()
	}()

	h := httpapi.NewHandler(st, cfg.NodeID)
	h.ShardMgr = shard.NewManager()
	h.ShardRafts = make(map[string]*shardraft.ShardRaft)
	h.ShardCount = cfg.ShardCount // Set shard count so handler knows to use per-shard Raft

	// If Raft enabled, initialize node and attach to handler
	var rn *raftnode.Node
	if cfg.EnableRaft {
		raftCfg := &raftnode.RaftConfig{
			NodeID:      cfg.NodeID,
			RaftAddr:    cfg.RaftAddr,
			DataDir:     cfg.DataDir,
			Store:       st,
			JoinAddr:    cfg.JoinAddr,
			TLSCertFile: cfg.RaftTLSCert,
			TLSKeyFile:  cfg.RaftTLSKey,
			TLSCAFile:   cfg.RaftTLSCA,
		}
		nnode, err := raftnode.NewNode(raftCfg)
		if err != nil {
			log.Fatalf("failed to start raft node: %v", err)
		}
		rn = nnode
		h.RaftNode = rn

		tlsStatus := "disabled"
		if cfg.UseRaftTLS() {
			tlsStatus = "enabled"
		}
		fmt.Printf("Started raft node: id=%s raft_addr=%s leader=%s tls=%s\n", rn.ID, rn.Addr, rn.Leader(), tlsStatus)

		// If join flag provided, attempt auto-join to the cluster leader.
		if cfg.JoinAddr != "" {
			// joinLeader will retry for a bit until it succeeds or times out.
			if err := joinLeader(cfg.JoinAddr, cfg.NodeID, cfg.RaftAddr, 30*time.Second); err != nil {
				log.Fatalf("failed to join leader at %s: %v", cfg.JoinAddr, err)
			}
			fmt.Printf("Successfully joined cluster via %s\n", cfg.JoinAddr)
		}
	}

	// Register with control plane if configured
	if cfg.ControlPlaneAddr != "" {
		if err := registerWithControlPlane(cfg.ControlPlaneAddr, cfg.NodeID, cfg.HTTPAddr, cfg.RaftAddr, 30*time.Second); err != nil {
			log.Printf("Warning: failed to register with control plane at %s: %v", cfg.ControlPlaneAddr, err)
		} else {
			fmt.Printf("Successfully registered with control plane at %s\n", cfg.ControlPlaneAddr)
		}
	}

	var membershipMgr *shard.MembershipManager
	if cfg.ShardCount > 0 {
		membershipMgr = startShards(cfg, h)
	}

	// Initialize metrics
	reg := metrics.DefaultRegistry()
	reg.SetStartTime()

	mux := http.NewServeMux()
	h.Register(mux)
	// register shard admin endpoints (includes migration endpoints)
	h.RegisterShardRoutes(mux)

	// Add Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Wrap with metrics middleware
	var handler http.Handler = httpapi.MetricsMiddleware(mux)

	// Wrap with auth middleware if token is configured
	if cfg.AuthToken != "" {
		handler = authMiddleware(cfg.AuthToken, handler)
		fmt.Printf("Authentication enabled for admin endpoints\n")
	}

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Run server in goroutine
	go func() {
		protocol := "HTTP"
		if cfg.UseTLS() {
			protocol = "HTTPS"
		}
		fmt.Printf("%s server listening on %s (data-dir=%s node=%s)\n", protocol, cfg.HTTPAddr, cfg.DataDir, cfg.NodeID)

		var err error
		if cfg.UseTLS() {
			err = srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	// Graceful shutdown handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down gracefully...")

	// Stop membership manager
	if membershipMgr != nil {
		membershipMgr.Stop()
	}

	// Shutdown HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// Close Raft
	if rn != nil {
		if err := rn.Raft.Shutdown().Error(); err != nil {
			log.Printf("Raft shutdown error: %v", err)
		}
	}

	// Close store
	if err := st.Close(); err != nil {
		log.Printf("Store close error: %v", err)
	}

	fmt.Println("Shutdown complete")
}

// joinLeader tries to POST to leaderAddr + "/v1/join" the JSON
// {"node_id": "<nodeID>", "raft_addr":"<raftAddr>"} and follows
// leader redirects returned via X-Raft-Leader header. It will retry
// until timeout.
func joinLeader(leaderHTTP string, nodeID string, raftAddr string, timeout time.Duration) error {
	type joinReq struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
		// do not auto-follow redirects because leader may reply 307 and include X-Raft-Leader header
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	reqBody := joinReq{
		NodeID:   nodeID,
		RaftAddr: raftAddr,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	deadline := time.Now().Add(timeout)
	try := 0
	target := leaderHTTP

	for time.Now().Before(deadline) {
		try++
		url := fmt.Sprintf("%s/v1/join", target)
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			// network errors: try again after short sleep
			fmt.Printf("[join] attempt %d: error contacting %s: %v\n", try, target, err)
			time.Sleep(1 * time.Second)
			continue
		}

		// read and close body
		_ = resp.Body.Close()

		// If leader accepted the join, 204 No Content expected
		if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
			return nil
		}

		// If redirected or follower returns TemporaryRedirect, check X-Raft-Leader header and retry to that leader.
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusMovedPermanently || resp.StatusCode == http.StatusFound {
			if leader := resp.Header.Get("X-Raft-Leader"); leader != "" {
				// leader may include raft addr; convert raft addr to http if needed
				// assume leader header contains raft address (host:port) or http://host:port
				tgt := leader
				// if leader looks like "host:port" convert to http://host:8080 default?
				if _, _, err := net.SplitHostPort(leader); err == nil {
					// Convert to http address on default http port 8080
					tgt = "http://" + leader
				}
				target = tgt
				fmt.Printf("[join] redirect to leader %s (resp status %d)\n", target, resp.StatusCode)
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}

		// For other status codes, log and retry
		fmt.Printf("[join] attempt %d: unexpected status %d from %s\n", try, resp.StatusCode, target)
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("join timed out after %s", timeout.String())
}

// registerWithControlPlane registers this node with the control plane
func registerWithControlPlane(controlPlaneAddr, nodeID, httpAddr, raftAddr string, timeout time.Duration) error {
	type nodeReg struct {
		NodeID   string `json:"node_id"`
		HTTPAddr string `json:"http_addr"`
		RaftAddr string `json:"raft_addr"`
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	reqBody := nodeReg{
		NodeID:   nodeID,
		HTTPAddr: httpAddr,
		RaftAddr: raftAddr,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	deadline := time.Now().Add(timeout)
	try := 0

	for time.Now().Before(deadline) {
		try++
		url := fmt.Sprintf("http://%s/v1/control/nodes", controlPlaneAddr)
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("[control-reg] attempt %d: error contacting %s: %v\n", try, controlPlaneAddr, err)
			time.Sleep(1 * time.Second)
			continue
		}

		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
			return nil
		}

		fmt.Printf("[control-reg] attempt %d: unexpected status %d from %s\n", try, resp.StatusCode, controlPlaneAddr)
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("control plane registration timed out after %s", timeout.String())
}

// authMiddleware wraps an HTTP handler to require Bearer token authentication
// for admin endpoints (join, shard operations, migration, etc.)
func authMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// List of admin endpoints that require authentication
		adminEndpoints := []string{
			"/v1/join",
			"/v1/shards/",
			"/v1/control/",
		}

		// Check if this is an admin endpoint
		requiresAuth := false
		for _, prefix := range adminEndpoints {
			if len(r.URL.Path) >= len(prefix) && r.URL.Path[:len(prefix)] == prefix {
				requiresAuth = true
				break
			}
		}

		// Allow public endpoints without auth
		if !requiresAuth {
			next.ServeHTTP(w, r)
			return
		}

		// Check Authorization header
		authHeader := r.Header.Get("Authorization")
		expectedAuth := "Bearer " + token

		if authHeader != expectedAuth {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Authentication successful
		next.ServeHTTP(w, r)
	})
}
