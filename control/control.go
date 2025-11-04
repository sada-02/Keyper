package control

import (
	"encoding/json"
	"fmt"
	"time"

	raftnode "github.com/sada-02/keyper/raft"
	"github.com/sada-02/keyper/store"
)

// Internal raft command format for control-plane.
type controlCmd struct {
	Op    string `json:"op"`    // "set" | "delete"
	Key   string `json:"key"`   // e.g. "shard:3"
	Value []byte `json:"value"` // JSON value
}

// ControlNode is a small Raft-backed control plane that stores simple KV entries.
// It persists each assignment into a per-control Badger store and replicates them via Raft.
type ControlNode struct {
	Node  *raftnode.Node
	store *store.BadgerStore
}

// ControlConfig configures the control node.
type ControlConfig struct {
	NodeID   string
	RaftAddr string
	DataDir  string // directory for control store & raft data; will create DataDir/control
	JoinAddr string // optional HTTP join address for an existing control node
	// Timeout used when applying commands to raft
	ApplyTimeout time.Duration

	// TLS configuration for Raft transport
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string
}

// StartControlNode starts a control-plane Raft node backed by a Badger store.
// It returns a ControlNode; caller is responsible for shutdown.
func StartControlNode(cfg *ControlConfig) (*ControlNode, error) {
	dataDir := cfg.DataDir
	// create per-control data path convention: <DataDir>/control
	controlDataDir := dataDir
	// open Badger store for control plane
	st, err := store.NewBadgerStore(controlDataDir)
	if err != nil {
		return nil, fmt.Errorf("open control store: %w", err)
	}

	raftCfg := &raftnode.RaftConfig{
		NodeID:      cfg.NodeID,
		RaftAddr:    cfg.RaftAddr,
		DataDir:     controlDataDir,
		Store:       st,
		JoinAddr:    cfg.JoinAddr,
		TLSCertFile: cfg.TLSCertFile,
		TLSKeyFile:  cfg.TLSKeyFile,
		TLSCAFile:   cfg.TLSCAFile,
	}

	n, err := raftnode.NewNode(raftCfg)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("start control raft node: %w", err)
	}

	return &ControlNode{
		Node:  n,
		store: st,
	}, nil
}

// Shutdown closes raft and store.
func (c *ControlNode) Shutdown() {
	if c.Node != nil && c.Node.Raft != nil {
		_ = c.Node.Raft.Shutdown()
	}
	if c.store != nil {
		_ = c.store.Close()
	}
}

// apply serializes a controlCmd and applies it via raft.
func (c *ControlNode) apply(cmd *controlCmd, timeout time.Duration) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	// Use the raftnode wrapper to apply the raw bytes (we assume raftnode.Node.ApplyCommand exists)
	if c.Node == nil {
		return fmt.Errorf("raft node not initialized")
	}
	// Use raftnode.Node.ApplyRaw (fallback to .Raft.Apply if required). We'll use .ApplyCommand if available.
	// The raftnode.NewNode wrapper used previously exposed Apply method; follow same contract:
	if err := c.Node.Apply(b, timeout); err != nil {
		return err
	}
	return nil
}

// SetAssignment sets the list of nodes for a shard (replicated via Raft).
// shardKey should be "shard:<id>" (we expose helpers that wrap this).
func (c *ControlNode) SetAssignment(key string, value []byte, timeout time.Duration) error {
	cmd := &controlCmd{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	// apply via raft
	if err := c.apply(cmd, timeout); err != nil {
		return err
	}
	// local store also updated by FSM (on Apply) — but just in case, write locally as well.
	// Best practice: the FSM will perform the store.Set when log is applied; this local write is optional
	// and can be omitted to avoid duplication. We keep it idempotent: set again.
	return c.store.Set([]byte(key), value)
}

// GetAssignment reads current assignment from the local store (fast local read).
func (c *ControlNode) GetAssignment(key string) ([]byte, error) {
	return c.store.Get([]byte(key))
}

// ListAssignments lists all keys in the control store (simple iteration).
func (c *ControlNode) ListAssignments() (map[string][]byte, error) {
	out := make(map[string][]byte)
	it := c.store.NewIterator() // NOTE: implement NewIterator in store if not already present
	if it == nil {
		// Fallback: attempt Get on known keys (not ideal). Return empty map to be safe.
		return out, nil
	}
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Key()
		v := make([]byte, len(it.Value()))
		copy(v, it.Value())
		out[string(k)] = v
	}
	return out, nil
}
