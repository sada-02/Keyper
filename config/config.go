package config

import (
	"flag"
)

// Config holds runtime configuration from flags.
type Config struct {
	DataDir    string
	HTTPAddr   string
	NodeID     string
	EnableRaft bool
	RaftAddr   string
	JoinAddr   string

	// Phase 6: per-shard options
	ShardCount     int    // number of shards to start on this node (0 = disabled)
	RaftBasePort   int    // base port for per-shard raft instances; shard i uses base + i
	AssignedShards string // comma-separated shard IDs to host (e.g., "0,2,3"); if empty, hosts all

	// Control plane for membership coordination
	ControlPlaneAddr string // HTTP address of control plane (e.g., "localhost:7000")

	// Feature F: TLS & Auth
	TLSCertFile string // Path to TLS certificate file for HTTPS
	TLSKeyFile  string // Path to TLS private key file for HTTPS
	RaftTLSCert string // Path to TLS cert for Raft transport
	RaftTLSKey  string // Path to TLS key for Raft transport
	RaftTLSCA   string // Path to CA cert for verifying Raft peers
	AuthToken   string // Bearer token for admin endpoints (optional)

	// For bootstrapping clusters (control plane or shard nodes)
	Bootstrap bool
}

// Load parses command-line flags into Config.
func Load() *Config {
	c := &Config{}

	flag.StringVar(&c.DataDir, "data-dir", "./data", "data directory for Badger")
	flag.StringVar(&c.HTTPAddr, "http-addr", ":8080", "http listen address")
	flag.StringVar(&c.NodeID, "node-id", "node-1", "node identifier")
	flag.BoolVar(&c.EnableRaft, "enable-raft", false, "enable raft replication")
	flag.StringVar(&c.RaftAddr, "raft-addr", "127.0.0.1:12000", "raft bind address (host:port)")
	flag.StringVar(&c.JoinAddr, "join", "", "HTTP address of existing node to join (e.g. http://host:8080)")

	// Phase 6 flags:
	flag.IntVar(&c.ShardCount, "shard-count", 0, "number of shards (0 = no per-shard raft instances started automatically)")
	flag.IntVar(&c.RaftBasePort, "raft-base-port", 12000, "base port for per-shard raft instances; shard i uses base+ i")
	flag.StringVar(&c.AssignedShards, "assigned-shards", "", "comma-separated shard IDs to host (e.g., '0,2,3'); if empty, hosts all shards")
	flag.StringVar(&c.ControlPlaneAddr, "control-plane", "", "HTTP address of control plane for membership coordination (e.g., 'localhost:7000')")

	// Feature F: Security flags
	flag.StringVar(&c.TLSCertFile, "tls-cert", "", "Path to TLS certificate file for HTTPS (enables TLS when set)")
	flag.StringVar(&c.TLSKeyFile, "tls-key", "", "Path to TLS private key file for HTTPS")
	flag.StringVar(&c.RaftTLSCert, "raft-tls-cert", "", "Path to TLS certificate for Raft transport")
	flag.StringVar(&c.RaftTLSKey, "raft-tls-key", "", "Path to TLS private key for Raft transport")
	flag.StringVar(&c.RaftTLSCA, "raft-tls-ca", "", "Path to CA certificate for verifying Raft peers")
	flag.StringVar(&c.AuthToken, "auth-token", "", "Bearer token for authenticating admin endpoints (optional)")

	// Allow an explicit --bootstrap flag so scripts can request single-node bootstrap.
	flag.BoolVar(&c.Bootstrap, "bootstrap", false, "bootstrap a single-node Raft cluster (useful for first node)")

	flag.Parse()
	return c
}

// UseTLS returns true if TLS is configured for HTTP
func (c *Config) UseTLS() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}

// UseRaftTLS returns true if TLS is configured for Raft transport
func (c *Config) UseRaftTLS() bool {
	return c.RaftTLSCert != "" && c.RaftTLSKey != ""
}
