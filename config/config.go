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
	ShardCount   int // number of shards to start on this node (0 = disabled)
	RaftBasePort int // base port for per-shard raft instances; shard i uses base + i
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

	flag.Parse()
	return c
}
