// Package wal implements a write-ahead log: an append-only, durable record
// of every operation applied to the store, so a crash between "operation
// accepted" and "operation applied" is recoverable by replaying the log
// instead of silently losing or corrupting state.
package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
)

type Entry struct {
	Seq   uint64 `json:"seq"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	seq  uint64
}

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{file: f}, nil
}

// Replay reads every entry currently in the log and invokes apply for each,
// in order. Call it once at startup, before any Append, so the sequence
// counter picks up where the log left off instead of restarting at zero.
func (w *WAL) Replay(apply func(Entry)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek wal: %w", err)
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		var e Entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return fmt.Errorf("decode wal entry: %w", err)
		}
		apply(e)
		if e.Seq > w.seq {
			w.seq = e.Seq
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan wal: %w", err)
	}

	if _, err := w.file.Seek(0, 2); err != nil {
		return fmt.Errorf("seek wal end: %w", err)
	}
	return nil
}

// Append durably writes an entry to the log. Callers must only apply the
// operation to in-memory state after this returns successfully, so a crash
// can never leave an operation applied without being logged, or logged
// without eventually being applied.
func (w *WAL) Append(op, key, value string) (Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	e := Entry{Seq: w.seq, Op: op, Key: key, Value: value}

	data, err := json.Marshal(e)
	if err != nil {
		return Entry{}, fmt.Errorf("encode wal entry: %w", err)
	}
	data = append(data, '\n')

	if _, err := w.file.Write(data); err != nil {
		return Entry{}, fmt.Errorf("write wal entry: %w", err)
	}
	// fsync forces the write out of the OS page cache and onto disk. Without
	// it, a power loss (not just a process crash) could lose entries the
	// kernel was still buffering, even though Write returned successfully.
	if err := w.file.Sync(); err != nil {
		return Entry{}, fmt.Errorf("sync wal entry: %w", err)
	}
	return e, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
