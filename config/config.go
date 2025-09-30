package config

import (
	"flag"
)

// Config holds runtime settings for the server.
type Config struct {
	DataDir    string
	HTTPAddr   string
	NodeID     string
	EnableRaft bool
	RaftAddr   string
	JoinAddr   string // admin http addr of a node to join, empty => bootstrap single-node
}

// Load parses flags and returns a Config.
func Load() *Config {
	var c Config
	flag.StringVar(&c.DataDir, "data-dir", "./data", "data directory for Badger")
	flag.StringVar(&c.HTTPAddr, "http-addr", ":8080", "http listen address")
	flag.StringVar(&c.NodeID, "node-id", "node-1", "node identifier (optional)")
	flag.BoolVar(&c.EnableRaft, "enable-raft", false, "enable raft replication")
	flag.StringVar(&c.RaftAddr, "raft-addr", "127.0.0.1:12000", "raft bind address (host:port)")
	flag.StringVar(&c.JoinAddr, "join", "", "HTTP address of existing node to join (e.g. http://host:8080); empty = bootstrap single-node")
	flag.Parse()
	return &c
}
