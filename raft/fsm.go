package raftnode

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	raft "github.com/hashicorp/raft"
	"github.com/sada-02/keyper/store"
)

// Command is the structure we store in the Raft log.
type Command struct {
	Op    string `json:"op"`              // "set" or "delete"
	Key   string `json:"key"`             // key
	Value []byte `json:"value,omitempty"` // value for set
}

// fsm implements raft.FSM using the Badger-backed store.
type fsm struct {
	store *store.BadgerStore
}

func NewFSM(s *store.BadgerStore) raft.FSM {
	return &fsm{store: s}
}

// Apply applies a Raft log entry to the underlying store.
func (f *fsm) Apply(logEntry *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		return fmt.Errorf("failed unmarshal command: %w", err)
	}

	switch cmd.Op {
	case "set":
		if err := f.store.Set([]byte(cmd.Key), cmd.Value); err != nil {
			return fmt.Errorf("set failed: %w", err)
		}
		return nil
	case "delete":
		if err := f.store.Delete([]byte(cmd.Key)); err != nil {
			return fmt.Errorf("delete failed: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown op: %s", cmd.Op)
	}
}

// Snapshot returns a snapshot of the current state.
// We use store.Export to write newline JSON KV pairs into a temp file and return a fileSnapshot that streams it.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	tmpFile, err := os.CreateTemp("", "fsm-snap-*.tmp")
	if err != nil {
		return nil, err
	}

	// Use store.Export to write DB to tmpFile
	if err := f.store.Export(tmpFile); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return nil, err
	}

	// flush & rewind
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return nil, err
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return nil, err
	}

	return &fileSnapshot{file: tmpFile}, nil
}

// Restore reads a snapshot (stream of KVPair JSON lines) and restores them into the store.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	// Use store.Import to write key/value pairs into DB
	if err := f.store.Import(rc); err != nil {
		return err
	}
	// small delay to ensure writes persisted
	time.Sleep(10 * time.Millisecond)
	return nil
}

// fileSnapshot implements raft.FSMSnapshot by streaming a file.
type fileSnapshot struct {
	file *os.File
}

func (s *fileSnapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		_ = s.file.Close()
	}()
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		_ = sink.Cancel()
		return err
	}

	buf := make([]byte, 64*1024)
	for {
		n, rerr := s.file.Read(buf)
		if n > 0 {
			if _, werr := sink.Write(buf[:n]); werr != nil {
				_ = sink.Cancel()
				return werr
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			_ = sink.Cancel()
			return rerr
		}
	}
	if err := sink.Close(); err != nil {
		return err
	}
	_ = os.Remove(s.file.Name())
	return nil
}

func (s *fileSnapshot) Release() {}
