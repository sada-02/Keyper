package raftnode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	raft "github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/sada-02/keyper/store"
)

// Node wraps the raft instance and provides helpers.
type Node struct {
	Raft *raft.Raft
	// ID and Addr are for info
	ID   string
	Addr string // raft bind address
}

// RaftConfig contains parameters for starting a node.
type RaftConfig struct {
	NodeID   string
	RaftAddr string // host:port for Raft transport
	DataDir  string // base data dir - we will create DataDir/raft
	Store    *store.BadgerStore
	JoinAddr string // if non-empty, perform join flow (call via HTTP to leader)
}

// NewNode starts and returns a configured Raft node. If joinAddr is empty,
// this node will attempt to bootstrap a single-node cluster.
func NewNode(cfg *RaftConfig) (*Node, error) {
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, err
	}

	// Raft config
	rconf := raft.DefaultConfig()
	rconf.LocalID = raft.ServerID(cfg.NodeID)

	// Create snapshot store (files)
	snapshots, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %w", err)
	}

	// Create the BoltDB-backed stores for stable store and log store
	boltDBPath := filepath.Join(raftDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}

	// Use boltStore for both stable and log store
	logStore := boltStore
	stableStore := boltStore

	// Transport
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	// FSM
	f := NewFSM(cfg.Store)

	// Instantiate Raft
	r, err := raft.NewRaft(rconf, f, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	node := &Node{
		Raft: r,
		ID:   cfg.NodeID,
		Addr: cfg.RaftAddr,
	}

	// Bootstrap single-node if join address not provided and no existing state
	hasState := false
	// If there are any servers in configuration, treat as existing state
	cfgs := r.GetConfiguration()
	if cfgs != nil {
		future := r.GetConfiguration()
		if future != nil {
			_ = future // ignored, but we check below by reading config servers
		}
	}

	// Check whether there are any known servers in the configuration
	fut := r.GetConfiguration()
	if err := fut.Error(); err == nil {
		for _, srv := range fut.Configuration().Servers {
			if srv.ID != "" {
				hasState = true
				break
			}
		}
	}

	if !hasState && cfg.JoinAddr == "" {
		// bootstrap single-node cluster
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(cfg.RaftAddr),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if f.Error() != nil && f.Error() != raft.ErrCantBootstrap { // ErrCantBootstrap on existing
			return nil, fmt.Errorf("bootstrap cluster: %w", f.Error())
		}
	}

	// If join address present, caller is expected to call join endpoint on existing cluster.
	return node, nil
}

// Leader returns the current leader address (raft.ServerAddress -> string), or empty string.
func (n *Node) Leader() string {
	return string(n.Raft.Leader())
}

// ApplyCommand marshals command and applies via Raft, returning error or nil.
// It waits up to timeout for apply to complete.
func (n *Node) ApplyCommand(cmd *Command, timeout time.Duration) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := n.Raft.Apply(b, timeout)
	if err := f.Error(); err != nil {
		return err
	}
	// result may be error returned by FSM.Apply
	if res := f.Response(); res != nil {
		// if FSM.Apply returned an error, it will be available here
		if ferr, ok := res.(error); ok {
			return ferr
		}
	}
	return nil
}

// Join logic (helper) — for simplicity, we perform an HTTP POST to /v1/join on the joinAddr
// The existing leader should implement /v1/join to call AddVoter on the raft cluster.
// We don't implement HTTP join here; main will call the join endpoint if needed.
// (This function kept for completeness.)
func (n *Node) AddVoter(nodeID, addr string, timeout time.Duration) error {
	f := n.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, timeout)
	return f.Error()
}

// Snapshot reader helper — not used externally
func readAll(r io.Reader) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
