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

	"github.com/sada-02/keyper/config"
	"github.com/sada-02/keyper/httpapi"
	raftnode "github.com/sada-02/keyper/raft"
	"github.com/sada-02/keyper/store"
	"github.com/sada-02/keyper/shard"
	shardraft "github.com/sada-02/keyper/shardraft"
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

	// If Raft enabled, initialize node and attach to handler
	var rn *raftnode.Node
	if cfg.EnableRaft {
		raftCfg := &raftnode.RaftConfig{
			NodeID:   cfg.NodeID,
			RaftAddr: cfg.RaftAddr,
			DataDir:  cfg.DataDir,
			Store:    st,
			JoinAddr: cfg.JoinAddr,
		}
		nnode, err := raftnode.NewNode(raftCfg)
		if err != nil {
			log.Fatalf("failed to start raft node: %v", err)
		}
		rn = nnode
		h.RaftNode = rn

		fmt.Printf("Started raft node: id=%s raft_addr=%s leader=%s\n", rn.ID, rn.Addr, rn.Leader())

		// If join flag provided, attempt auto-join to the cluster leader.
		if cfg.JoinAddr != "" {
			// joinLeader will retry for a bit until it succeeds or times out.
			if err := joinLeader(cfg.JoinAddr, cfg.NodeID, cfg.RaftAddr, 30*time.Second); err != nil {
				log.Fatalf("failed to join leader at %s: %v", cfg.JoinAddr, err)
			}
			fmt.Printf("Successfully joined cluster via %s\n", cfg.JoinAddr)
		}
	}

	if cfg.ShardCount > 0 {
		startShards(cfg, h)
	}

	mux := http.NewServeMux()
	h.Register(mux)
	// register shard admin endpoints
	h.RegisterShardRoutes(mux)

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Run server in goroutine
	go func() {
		fmt.Printf("HTTP server listening on %s (data-dir=%s node=%s)\n", cfg.HTTPAddr, cfg.DataDir, cfg.NodeID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	fmt.Println("\nshutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("server shutdown: %v", err)
	}

	// Shutdown raft if needed
	if rn != nil && rn.Raft != nil {
		f := rn.Raft.Shutdown()
		_ = f.Error()
	}

	// Shutdown per-shard raft instances (if any)
	if h.ShardRafts != nil {
		for id, sr := range h.ShardRafts {
			if sr == nil {
				continue
			}
 			// shut down raft instance
 			if sr.Node != nil && sr.Node.Raft != nil {
 				_ = sr.Node.Raft.Shutdown()
 			}
 			// close per-shard store
 			if sr.Store != nil {
 				_ = sr.Store.Close()
 			}
 			fmt.Printf("shard %s shut down\n", id)
 		}
 	}
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
