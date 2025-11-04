package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Registry holds all Prometheus metrics for Keyper
type Registry struct {
	// Raft metrics
	RaftLeaderChanges prometheus.Counter
	RaftCommitLatency prometheus.Histogram
	RaftAppliedOps    *prometheus.CounterVec
	RaftLogEntries    prometheus.Gauge
	RaftPeers         *prometheus.GaugeVec
	RaftState         *prometheus.GaugeVec
	RaftLastContact   *prometheus.GaugeVec

	// BadgerDB metrics
	BadgerReads        *prometheus.CounterVec
	BadgerWrites       *prometheus.CounterVec
	BadgerBytesRead    *prometheus.CounterVec
	BadgerBytesWritten *prometheus.CounterVec
	BadgerLSMSize      *prometheus.GaugeVec
	BadgerVLogSize     *prometheus.GaugeVec

	// HTTP API metrics
	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
	HTTPInFlight *prometheus.GaugeVec

	// Shard metrics
	ShardOperations *prometheus.CounterVec
	ShardMigrations *prometheus.CounterVec
	ShardReadOnly   *prometheus.GaugeVec
	ShardKeys       *prometheus.GaugeVec

	// Control plane metrics
	ControlPlaneNodes  prometheus.Gauge
	ControlPlaneShards prometheus.Gauge

	// System metrics
	StartTime prometheus.Gauge
}

var (
	// Global registry instance
	globalRegistry *Registry
	once           sync.Once
)

// DefaultRegistry returns the global metrics registry
func DefaultRegistry() *Registry {
	once.Do(func() {
		globalRegistry = newRegistry()
	})
	return globalRegistry
}

// newRegistry creates and registers all Prometheus metrics
func newRegistry() *Registry {
	return &Registry{
		// Raft metrics
		RaftLeaderChanges: promauto.NewCounter(prometheus.CounterOpts{
			Name: "keyper_raft_leader_changes_total",
			Help: "Total number of Raft leader changes",
		}),

		RaftCommitLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "keyper_raft_commit_duration_seconds",
			Help:    "Raft commit latency in seconds",
			Buckets: prometheus.DefBuckets, // [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
		}),

		RaftAppliedOps: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_raft_applied_ops_total",
				Help: "Total number of Raft operations applied",
			},
			[]string{"shard_id", "op_type"}, // op_type: set, delete, etc.
		),

		RaftLogEntries: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "keyper_raft_log_entries",
			Help: "Current number of entries in Raft log",
		}),

		RaftPeers: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_raft_peers",
				Help: "Number of Raft peers in cluster",
			},
			[]string{"shard_id"},
		),

		RaftState: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_raft_state",
				Help: "Raft state (0=follower, 1=candidate, 2=leader)",
			},
			[]string{"shard_id", "node_id"},
		),

		RaftLastContact: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_raft_last_contact_seconds",
				Help: "Time since last contact from leader in seconds",
			},
			[]string{"shard_id"},
		),

		// BadgerDB metrics
		BadgerReads: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_badger_reads_total",
				Help: "Total number of BadgerDB reads",
			},
			[]string{"shard_id"},
		),

		BadgerWrites: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_badger_writes_total",
				Help: "Total number of BadgerDB writes",
			},
			[]string{"shard_id"},
		),

		BadgerBytesRead: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_badger_bytes_read_total",
				Help: "Total bytes read from BadgerDB",
			},
			[]string{"shard_id"},
		),

		BadgerBytesWritten: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_badger_bytes_written_total",
				Help: "Total bytes written to BadgerDB",
			},
			[]string{"shard_id"},
		),

		BadgerLSMSize: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_badger_lsm_size_bytes",
				Help: "BadgerDB LSM tree size in bytes",
			},
			[]string{"shard_id"},
		),

		BadgerVLogSize: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_badger_vlog_size_bytes",
				Help: "BadgerDB value log size in bytes",
			},
			[]string{"shard_id"},
		),

		// HTTP API metrics
		HTTPRequests: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "path", "status"},
		),

		HTTPDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "keyper_http_request_duration_seconds",
				Help:    "HTTP request duration in seconds",
				Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
			},
			[]string{"method", "path"},
		),

		HTTPInFlight: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_http_in_flight_requests",
				Help: "Current number of in-flight HTTP requests",
			},
			[]string{"method", "path"},
		),

		// Shard metrics
		ShardOperations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_shard_operations_total",
				Help: "Total number of shard operations",
			},
			[]string{"shard_id", "operation"}, // operation: get, set, delete, export, import
		),

		ShardMigrations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "keyper_shard_migrations_total",
				Help: "Total number of shard migrations",
			},
			[]string{"shard_id", "status"}, // status: started, completed, failed
		),

		ShardReadOnly: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_shard_read_only",
				Help: "Shard read-only status (0=writable, 1=read-only)",
			},
			[]string{"shard_id"},
		),

		ShardKeys: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "keyper_shard_keys",
				Help: "Approximate number of keys in shard",
			},
			[]string{"shard_id"},
		),

		// Control plane metrics
		ControlPlaneNodes: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "keyper_control_plane_nodes",
			Help: "Number of registered nodes in control plane",
		}),

		ControlPlaneShards: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "keyper_control_plane_shards",
			Help: "Number of shards tracked by control plane",
		}),

		// System metrics
		StartTime: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "keyper_start_time_seconds",
			Help: "Unix timestamp when the server started",
		}),
	}
}

// RecordRaftCommit records a Raft commit operation with latency
func (r *Registry) RecordRaftCommit(duration time.Duration) {
	r.RaftCommitLatency.Observe(duration.Seconds())
}

// RecordRaftApply records a Raft apply operation
func (r *Registry) RecordRaftApply(shardID, opType string) {
	r.RaftAppliedOps.WithLabelValues(shardID, opType).Inc()
}

// RecordHTTPRequest records an HTTP request with status and duration
func (r *Registry) RecordHTTPRequest(method, path, status string, duration time.Duration) {
	r.HTTPRequests.WithLabelValues(method, path, status).Inc()
	r.HTTPDuration.WithLabelValues(method, path).Observe(duration.Seconds())
}

// RecordShardOperation records a shard-level operation
func (r *Registry) RecordShardOperation(shardID, operation string) {
	r.ShardOperations.WithLabelValues(shardID, operation).Inc()
}

// RecordMigration records a shard migration event
func (r *Registry) RecordMigration(shardID, status string) {
	r.ShardMigrations.WithLabelValues(shardID, status).Inc()
}

// SetRaftState sets the current Raft state (0=follower, 1=candidate, 2=leader)
func (r *Registry) SetRaftState(shardID, nodeID string, state int) {
	r.RaftState.WithLabelValues(shardID, nodeID).Set(float64(state))
}

// SetShardReadOnly sets the read-only status of a shard
func (r *Registry) SetShardReadOnly(shardID string, readOnly bool) {
	val := 0.0
	if readOnly {
		val = 1.0
	}
	r.ShardReadOnly.WithLabelValues(shardID).Set(val)
}

// UpdateBadgerStats updates BadgerDB statistics
func (r *Registry) UpdateBadgerStats(shardID string, lsmSize, vlogSize int64) {
	r.BadgerLSMSize.WithLabelValues(shardID).Set(float64(lsmSize))
	r.BadgerVLogSize.WithLabelValues(shardID).Set(float64(vlogSize))
}

// RecordBadgerRead records a BadgerDB read operation
func (r *Registry) RecordBadgerRead(shardID string, bytes int) {
	r.BadgerReads.WithLabelValues(shardID).Inc()
	r.BadgerBytesRead.WithLabelValues(shardID).Add(float64(bytes))
}

// RecordBadgerWrite records a BadgerDB write operation
func (r *Registry) RecordBadgerWrite(shardID string, bytes int) {
	r.BadgerWrites.WithLabelValues(shardID).Inc()
	r.BadgerBytesWritten.WithLabelValues(shardID).Add(float64(bytes))
}

// SetStartTime records the server start time
func (r *Registry) SetStartTime() {
	r.StartTime.Set(float64(time.Now().Unix()))
}

// IncrementLeaderChanges increments the leader change counter
func (r *Registry) IncrementLeaderChanges() {
	r.RaftLeaderChanges.Inc()
}

// SetRaftPeers sets the number of Raft peers for a shard
func (r *Registry) SetRaftPeers(shardID string, count int) {
	r.RaftPeers.WithLabelValues(shardID).Set(float64(count))
}

// SetLastContact sets the time since last contact from leader
func (r *Registry) SetLastContact(shardID string, seconds float64) {
	r.RaftLastContact.WithLabelValues(shardID).Set(seconds)
}

// SetControlPlaneStats sets control plane statistics
func (r *Registry) SetControlPlaneStats(nodes, shards int) {
	r.ControlPlaneNodes.Set(float64(nodes))
	r.ControlPlaneShards.Set(float64(shards))
}
