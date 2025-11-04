package raftnode

import (
	"fmt"
	"time"

	"github.com/sada-02/keyper/metrics"
)

// Apply applies the given command bytes to the underlying Hashicorp Raft instance,
// waiting up to timeout for it to be committed. Returns error from the future.
func (n *Node) Apply(b []byte, timeout time.Duration) error {
	if n == nil {
		return fmt.Errorf("raft node is nil")
	}
	if n.Raft == nil {
		return fmt.Errorf("raft not initialized on node %s", n.ID)
	}

	// Record commit latency
	start := time.Now()
	f := n.Raft.Apply(b, timeout)
	err := f.Error()

	if err == nil {
		// Only record successful commits
		metrics.DefaultRegistry().RecordRaftCommit(time.Since(start))
	}

	return err
}
