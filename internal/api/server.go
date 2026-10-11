// Package api exposes the store over HTTP: GET/PUT/DELETE on /kv/{key}.
// In single-node mode (no Raft configured) writes go straight to a local
// WAL. In cluster mode, writes are proposed to Raft and only acknowledged
// once a majority of the cluster has durably replicated them.
package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/bryanphanny/raftkv-store/internal/raft"
	"github.com/bryanphanny/raftkv-store/internal/store"
	"github.com/bryanphanny/raftkv-store/internal/wal"
)

type Server struct {
	store *store.Store
	wal   *wal.WAL
	raft  *raft.Raft // nil in single-node mode
}

func NewServer(s *store.Store, w *wal.WAL, r *raft.Raft) *Server {
	return &Server{store: s, wal: w, raft: r}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	return mux
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Served from local state without consulting Raft: fast, but reads
		// a follower's copy of the committed log, which can briefly lag the
		// leader. Making reads linearizable (e.g. via a leader read-index
		// round) is a known gap, not fixed at this stage.
		value, err := s.store.Get(key)
		if errors.Is(err, store.ErrKeyNotFound) {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.Write([]byte(value))

	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		s.write(w, raft.OpPut, key, string(body))

	case http.MethodDelete:
		s.write(w, raft.OpDelete, key, "")

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// write commits an operation before it takes effect: in cluster mode that
// means a majority of the Raft log; in single-node mode, the local WAL.
// Either way, the caller only sees 200 OK once the write cannot be lost by
// a single crash.
func (s *Server) write(w http.ResponseWriter, op, key, value string) {
	if s.raft != nil {
		index, term, ok := s.raft.Propose(op, key, value)
		if !ok {
			s.writeNotLeader(w)
			return
		}
		if !s.raft.WaitCommitted(index, term) {
			http.Error(w, "lost leadership before write committed; retry", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	if _, err := s.wal.Append(op, key, value); err != nil {
		http.Error(w, "failed to persist write", http.StatusInternalServerError)
		return
	}
	switch op {
	case raft.OpPut:
		s.store.Put(key, value)
	case raft.OpDelete:
		s.store.Delete(key)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) writeNotLeader(w http.ResponseWriter) {
	_, _, _, leaderID := s.raft.State()
	w.Header().Set("X-Raft-Leader-Id", leaderID)
	http.Error(w, fmt.Sprintf("not leader (current leader: %q)", leaderID), http.StatusServiceUnavailable)
}
