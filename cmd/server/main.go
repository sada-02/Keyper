package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sada-02/keyper/config"
	"github.com/sada-02/keyper/httpapi"
	raftnode "github.com/sada-02/keyper/raft"
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
	}

	mux := http.NewServeMux()
	h.Register(mux)

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
}
