package logging

import (
	"os"

	"github.com/hashicorp/go-hclog"
)

// Logger wraps hclog.Logger with contextual fields
type Logger struct {
	hclog.Logger
	nodeID   string
	shardID  string
	raftAddr string
}

// Config holds logger configuration
type Config struct {
	Level    string // debug, info, warn, error
	JSON     bool   // Output JSON format
	NodeID   string
	ShardID  string
	RaftAddr string
}

// NewLogger creates a new structured logger with context
func NewLogger(cfg Config) *Logger {
	level := hclog.Info
	switch cfg.Level {
	case "debug":
		level = hclog.Debug
	case "warn":
		level = hclog.Warn
	case "error":
		level = hclog.Error
	}

	opts := &hclog.LoggerOptions{
		Name:       "keyper",
		Level:      level,
		Output:     os.Stdout,
		JSONFormat: cfg.JSON,
	}

	baseLogger := hclog.New(opts)

	// Add contextual fields
	if cfg.NodeID != "" {
		baseLogger = baseLogger.With("node_id", cfg.NodeID)
	}
	if cfg.ShardID != "" {
		baseLogger = baseLogger.With("shard_id", cfg.ShardID)
	}
	if cfg.RaftAddr != "" {
		baseLogger = baseLogger.With("raft_addr", cfg.RaftAddr)
	}

	return &Logger{
		Logger:   baseLogger,
		nodeID:   cfg.NodeID,
		shardID:  cfg.ShardID,
		raftAddr: cfg.RaftAddr,
	}
}

// WithShard returns a new logger with shard context
func (l *Logger) WithShard(shardID string) *Logger {
	return &Logger{
		Logger:   l.Logger.With("shard_id", shardID),
		nodeID:   l.nodeID,
		shardID:  shardID,
		raftAddr: l.raftAddr,
	}
}

// WithNode returns a new logger with node context
func (l *Logger) WithNode(nodeID string) *Logger {
	return &Logger{
		Logger:   l.Logger.With("node_id", nodeID),
		nodeID:   nodeID,
		shardID:  l.shardID,
		raftAddr: l.raftAddr,
	}
}

// WithRaftAddr returns a new logger with Raft address context
func (l *Logger) WithRaftAddr(raftAddr string) *Logger {
	return &Logger{
		Logger:   l.Logger.With("raft_addr", raftAddr),
		nodeID:   l.nodeID,
		shardID:  l.shardID,
		raftAddr: raftAddr,
	}
}

// WithFields returns a new logger with additional fields
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return &Logger{
		Logger:   l.Logger.With(args...),
		nodeID:   l.nodeID,
		shardID:  l.shardID,
		raftAddr: l.raftAddr,
	}
}

// GetNodeID returns the node ID context
func (l *Logger) GetNodeID() string {
	return l.nodeID
}

// GetShardID returns the shard ID context
func (l *Logger) GetShardID() string {
	return l.shardID
}

// GetRaftAddr returns the Raft address context
func (l *Logger) GetRaftAddr() string {
	return l.raftAddr
}

// DefaultLogger returns a default logger for components without specific context
func DefaultLogger(jsonOutput bool) *Logger {
	return NewLogger(Config{
		Level: "info",
		JSON:  jsonOutput,
	})
}
