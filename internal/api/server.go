// Package api exposes the store over HTTP: GET/PUT/DELETE on /kv/{key}.
package api

import (
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/bryanphanny/raftkv-store/internal/store"
)

type Server struct {
	store *store.Store
}

func NewServer(s *store.Store) *Server {
	return &Server{store: s}
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
		s.store.Put(key, string(body))
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		s.store.Delete(key)
		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) ListenAndServe(addr string) error {
	log.Printf("kvserver listening on %s", addr)
	return http.ListenAndServe(addr, s.Handler())
}
