package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	raft "github.com/hashicorp/raft"
	raftnode "github.com/sada-02/keyper/raft"
	"github.com/sada-02/keyper/store"
)

// Handler holds dependencies for HTTP endpoints.
type Handler struct {
	Store    *store.BadgerStore
	NodeID   string
	RaftNode *raftnode.Node // nil if Raft disabled
}

// NewHandler builds a Handler.
func NewHandler(s *store.BadgerStore, nodeID string) *Handler {
	return &Handler{
		Store:  s,
		NodeID: nodeID,
	}
}

// Register registers HTTP routes on mux.
func (h *Handler) Register(mux *http.ServeMux) {
	// Key API: PUT/GET/DELETE /v1/keys/{key}
	mux.HandleFunc("/v1/keys/", h.keyHandler)

	// Status endpoint
	mux.HandleFunc("/v1/status", h.statusHandler)

	// Join endpoint for adding voters (leader must implement).
	mux.HandleFunc("/v1/join", h.joinHandler)
}

func (h *Handler) keyHandler(w http.ResponseWriter, r *http.Request) {
	// path: /v1/keys/<key>
	key := strings.TrimPrefix(r.URL.Path, "/v1/keys/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		// If Raft enabled, apply via raft; else write directly.
		if h.RaftNode != nil {
			// If not leader, redirect client to leader
			if h.RaftNode.Raft.State() != raft.Leader {
				leader := h.RaftNode.Leader()
				if leader != "" {
					w.Header().Set("X-Raft-Leader", leader)
				}
				http.Error(w, "not leader", http.StatusTemporaryRedirect)
				return
			}
			cmd := &raftnode.Command{
				Op:    "set",
				Key:   key,
				Value: body,
			}
			if err := h.RaftNode.ApplyCommand(cmd, 5*time.Second); err != nil {
				http.Error(w, "raft apply failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// No raft -> direct write
		if err := h.Store.Set([]byte(key), body); err != nil {
			http.Error(w, "set failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		// For simplicity: direct read from store.
		val, err := h.Store.Get([]byte(key))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "get failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(val)
	case http.MethodDelete:
		if h.RaftNode != nil {
			if h.RaftNode.Raft.State() != raft.Leader {
				leader := h.RaftNode.Leader()
				if leader != "" {
					w.Header().Set("X-Raft-Leader", leader)
				}
				http.Error(w, "not leader", http.StatusTemporaryRedirect)
				return
			}
			cmd := &raftnode.Command{
				Op:  "delete",
				Key: key,
			}
			if err := h.RaftNode.ApplyCommand(cmd, 5*time.Second); err != nil {
				http.Error(w, "raft apply failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// No raft -> direct delete
		err := h.Store.Delete([]byte(key))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT, GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) statusHandler(w http.ResponseWriter, r *http.Request) {
	leader := ""
	isLeader := false
	if h.RaftNode != nil {
		leader = h.RaftNode.Leader()
		if h.RaftNode.Raft.State() == raft.Leader {
			isLeader = true
		}
	}
	resp := `{"node_id":"` + h.NodeID + `","status":"ok","is_leader":` + strconv.FormatBool(isLeader) + `,"leader_addr":"` + leader + `"}`

	_, _ = w.Write([]byte(resp))
}

// joinHandler implements a simple join API:
// POST /v1/join with JSON {"node_id":"id","raft_addr":"host:port"}
// Only leader should accept join requests and call AddVoter on raft.
func (h *Handler) joinHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.RaftNode == nil {
		http.Error(w, "raft not enabled", http.StatusBadRequest)
		return
	}
	// only leader should authorize join
	if h.RaftNode.Raft.State() != raft.Leader {
		w.Header().Set("X-Raft-Leader", h.RaftNode.Leader())
		http.Error(w, "not leader", http.StatusTemporaryRedirect)
		return
	}

	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.NodeID == "" || req.RaftAddr == "" {
		http.Error(w, "node_id and raft_addr required", http.StatusBadRequest)
		return
	}

	// Add voter
	if err := h.RaftNode.AddVoter(req.NodeID, req.RaftAddr, 10*time.Second); err != nil {
		http.Error(w, "add voter failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
