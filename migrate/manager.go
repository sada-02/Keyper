package migrate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Manager orchestrates a simple snapshot-transfer-import migration.
type Manager struct {
	Client *http.Client
	// optional timeouts
	Timeout time.Duration
}

// NewManager returns a Manager with a sensible default HTTP client.
func NewManager() *Manager {
	return &Manager{
		Client: &http.Client{
			Timeout: 0, // we handle timeouts via contexts if needed
		},
		Timeout: 0,
	}
}

// MigrateShard streams an exported snapshot from srcBase -> dstBase for the shardID.
// srcBase and dstBase are HTTP base URLs like "http://127.0.0.1:8080".
func (m *Manager) MigrateShard(srcBase, dstBase, shardID string, timeout time.Duration) error {
	srcBase = strings.TrimSuffix(srcBase, "/")
	dstBase = strings.TrimSuffix(dstBase, "/")
	srcURL := fmt.Sprintf("%s/v1/shards/%s/_export", srcBase, shardID)
	dstURL := fmt.Sprintf("%s/v1/shards/%s/_import", dstBase, shardID)

	// GET export from source
	srcResp, err := m.Client.Get(srcURL)
	if err != nil {
		return fmt.Errorf("get export from source: %w", err)
	}
	defer srcResp.Body.Close()
	if srcResp.StatusCode < 200 || srcResp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(srcResp.Body, 4096))
		return fmt.Errorf("source export failed status=%d body=%s", srcResp.StatusCode, string(body))
	}

	// Read all data from source (buffered approach for testing)
	data, err := io.ReadAll(srcResp.Body)
	if err != nil {
		return fmt.Errorf("read export data: %w", err)
	}

	// POST to destination
	req, err := http.NewRequest(http.MethodPost, dstURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create POST req: %w", err)
	}
	req.Header.Set("Content-Type", "application/gzip")

	dstResp, err := m.Client.Do(req)
	if err != nil {
		return fmt.Errorf("post to destination: %w", err)
	}
	defer dstResp.Body.Close()

	if dstResp.StatusCode < 200 || dstResp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(dstResp.Body, 4096))
		return fmt.Errorf("destination import failed status=%d body=%s", dstResp.StatusCode, string(body))
	}

	return nil
}

// Helper: MigrateWithRetry attempts migration with simple retry/backoff
func (m *Manager) MigrateWithRetry(src, dst, shardID string, attempts int, backoff time.Duration) error {
	var last error
	for i := 0; i < attempts; i++ {
		if err := m.MigrateShard(src, dst, shardID, m.Timeout); err != nil {
			last = err
			time.Sleep(backoff)
			continue
		}
		return nil
	}
	if last == nil {
		return errors.New("migration failed")
	}
	return last
}
