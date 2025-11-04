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
	"github.com/sada-02/keyper/control"
)

func main() {
	cfg := config.Load()

	ctrlCfg := &control.ControlConfig{
		NodeID:       cfg.NodeID + "-control",
		RaftAddr:     cfg.RaftAddr, // pass explicit raft-addr flag when starting control
		DataDir:      cfg.DataDir + "/control",
		JoinAddr:     cfg.JoinAddr,
		ApplyTimeout: 5 * time.Second,
		TLSCertFile:  cfg.RaftTLSCert,
		TLSKeyFile:   cfg.RaftTLSKey,
		TLSCAFile:    cfg.RaftTLSCA,
	}

	cn, err := control.StartControlNode(ctrlCfg)
	if err != nil {
		log.Fatalf("start control node: %v", err)
	}
	defer cn.Shutdown()

	mux := http.NewServeMux()
	cn.RegisterControlRoutes(mux)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		protocol := "HTTP"
		if cfg.UseTLS() {
			protocol = "HTTPS"
		}
		fmt.Printf("Control %s listening on %s\n", protocol, cfg.HTTPAddr)

		var err error
		if cfg.UseTLS() {
			err = srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("control http serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
