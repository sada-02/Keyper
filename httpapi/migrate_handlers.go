package httpapi

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/sada-02/keyper/store"
)

// RegisterShardRoutes previously existed. We also register migrate endpoints
// by handling the "/v1/shards/" prefix and dispatching to _export/_import.
func (h *Handler) RegisterShardMigrateRoutes(mux *http.ServeMux) {

	// prefix handler for per-shard actions (e.g., /v1/shards/{id}/_export)
	mux.HandleFunc("/v1/shards/", h.shardPrefixHandler)
}

// shardPrefixHandler dispatches to export/import handlers.
func (h *Handler) shardPrefixHandler(w http.ResponseWriter, r *http.Request) {
	// path after /v1/shards/
	rem := strings.TrimPrefix(r.URL.Path, "/v1/shards/")
	// expect "<shardID>/_export" or "<shardID>/_import"
	parts := strings.SplitN(rem, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	shardID := parts[0]
	action := parts[1]

	switch action {
	case "_export", "_snapshot": // _snapshot is an alias for _export
		h.handleShardExport(w, r, shardID)
	case "_import":
		h.handleShardImport(w, r, shardID)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// handleShardExport streams gzipped ndjson snapshot of the shard store.
func (h *Handler) handleShardExport(w http.ResponseWriter, r *http.Request, shardID string) {
	// locate per-shard store
	if h.ShardRafts == nil {
		http.Error(w, "shard support not enabled", http.StatusBadRequest)
		return
	}
	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil || sr.Store == nil {
		http.Error(w, "shard not hosted here", http.StatusNotFound)
		return
	}

	// prepare response headers
	w.Header().Set("Content-Type", "application/gzip")
	// Don't set Content-Encoding - that causes auto-decompression by HTTP clients
	w.WriteHeader(http.StatusOK)

	// stream export directly to response writer
	if err := store.ExportToWriter(sr.Store, w); err != nil {
		// writing has already started; log and abort
		// in production you may want to panic or return structured error earlier
		fmt.Printf("shard export failed for %s: %v\n", shardID, err)
	}
}

// handleShardImport receives gzipped ndjson and imports into local shard store
func (h *Handler) handleShardImport(w http.ResponseWriter, r *http.Request, shardID string) {
	if h.ShardRafts == nil {
		http.Error(w, "shard support not enabled", http.StatusBadRequest)
		return
	}
	sr, ok := h.ShardRafts[shardID]
	if !ok || sr == nil || sr.Store == nil {
		http.Error(w, "shard not hosted here", http.StatusNotFound)
		return
	}

	// Expect gzipped ndjson in request body
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	// Import stream into the per-shard store
	if err := store.ImportFromReader(sr.Store, r.Body); err != nil {
		http.Error(w, fmt.Sprintf("import failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
