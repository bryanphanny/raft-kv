// Package store implements the in-memory key-value store that Stage 2 (WAL)
// and Stage 3 (Raft) will wrap without changing this interface.
package store

import (
	"errors"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return value, nil
}

func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}
