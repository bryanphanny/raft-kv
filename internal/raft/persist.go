package raft

import (
	"encoding/json"
	"fmt"
	"os"
)

// persistentState is exactly the "persistent state on all servers" from
// Figure 2 of the Raft paper: currentTerm, votedFor, and log. Everything
// else (commitIndex, lastApplied, leader-only nextIndex/matchIndex) is
// volatile and gets reconstructed after a restart instead of reloaded —
// a restarted node doesn't need to remember what was committed, only what
// it has in its log and who it voted for, which is enough for the rest of
// the protocol to safely rediscover the rest.
type persistentState struct {
	CurrentTerm uint64     `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

// Storage durably saves a Raft node's persistent state to a single file on
// disk, using write-to-temp-then-rename so a crash mid-save can never leave
// behind a half-written, corrupt state file — the rename only replaces the
// old file once the new one is completely and durably written.
type Storage struct {
	path string
}

func NewStorage(path string) *Storage {
	return &Storage{path: path}
}

func (st *Storage) Save(s persistentState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}

	tmp := st.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open temp raft state file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync raft state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close raft state: %w", err)
	}
	if err := os.Rename(tmp, st.path); err != nil {
		return fmt.Errorf("rename raft state into place: %w", err)
	}
	return nil
}

func (st *Storage) Load() (persistentState, bool, error) {
	data, err := os.ReadFile(st.path)
	if os.IsNotExist(err) {
		return persistentState{}, false, nil
	}
	if err != nil {
		return persistentState{}, false, fmt.Errorf("read raft state: %w", err)
	}
	var s persistentState
	if err := json.Unmarshal(data, &s); err != nil {
		return persistentState{}, false, fmt.Errorf("decode raft state: %w", err)
	}
	return s, true, nil
}
