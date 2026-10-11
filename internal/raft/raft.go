// Package raft implements the leader-election and log-replication core of
// the Raft consensus algorithm (Ongaro & Ousterhout, "In Search of an
// Understandable Consensus Algorithm"). A Raft node's job is to keep an
// identical, ordered log in sync across every node in a cluster, with
// exactly one leader at a time, so that a replicated state machine (here,
// the KV store) stays consistent even if a minority of nodes crash.
package raft

import (
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	rpcTimeout         = 100 * time.Millisecond
)

// Op values for LogEntry.Op — the only two mutations the KV state machine
// understands.
const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
	OpNoop   = "NOOP"
)

// LogEntry is one operation in the replicated log. Index is 1-based; log[0]
// (see Raft.log) is a dummy sentinel so prevLogIndex=0 is always valid.
type LogEntry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Config describes a node's identity and the rest of the cluster.
type Config struct {
	ID    string
	Peers map[string]string // peer id -> base URL, e.g. "http://127.0.0.1:8082"
}

// Raft is one node's consensus state. All access goes through methods that
// take the lock; "Locked" suffixed helpers assume the caller already holds it.
type Raft struct {
	mu    sync.Mutex
	id    string
	peers map[string]string

	state    State
	leaderID string

	// Persistent state (Stage 3c will make this durable across restarts).
	currentTerm uint64
	votedFor    string
	log         []LogEntry // log[0] = {Term: 0, Index: 0} sentinel

	// Volatile state, all nodes.
	commitIndex uint64
	lastApplied uint64

	// Volatile state, leader only (reinitialized on election).
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionResetAt time.Time

	// cond is broadcast whenever commitIndex advances or this node's
	// term/state changes, so WaitCommitted callers blocked on a specific
	// index wake up to re-check rather than poll.
	cond *sync.Cond

	storage *Storage // nil disables persistence (used by tests)

	apply  func(LogEntry)
	client *http.Client
}

// New constructs a node and, if storagePath is non-empty, restores
// currentTerm/votedFor/log from disk — this is the crash-recovery path: a
// node that restarts after a crash must rejoin with the same view of what
// it had already voted for and logged, or it could vote twice in one term
// or forget an entry it had promised to a leader.
func New(cfg Config, storagePath string, apply func(LogEntry)) *Raft {
	r := &Raft{
		id:              cfg.ID,
		peers:           cfg.Peers,
		state:           Follower,
		log:             []LogEntry{{Term: 0, Index: 0}},
		electionResetAt: time.Now(),
		apply:           apply,
		client:          &http.Client{Timeout: rpcTimeout},
	}
	r.cond = sync.NewCond(&r.mu)

	if storagePath != "" {
		r.storage = NewStorage(storagePath)
		saved, ok, err := r.storage.Load()
		if err != nil {
			log.Fatalf("[%s] failed to load persisted raft state: %v", cfg.ID, err)
		}
		if ok {
			r.currentTerm = saved.CurrentTerm
			r.votedFor = saved.VotedFor
			r.log = saved.Log
			log.Printf("[%s] restored persisted state: term=%d votedFor=%q log entries=%d",
				cfg.ID, r.currentTerm, r.votedFor, len(r.log)-1)
		}
	}
	return r
}

// persistLocked durably saves currentTerm/votedFor/log. Per the Raft paper,
// this must happen before replying to any RPC that depended on the change
// (granting a vote, acknowledging appended entries) — otherwise a crash
// right after replying, but before the write hit disk, would let this node
// come back up having forgotten a promise it already made to the cluster.
func (r *Raft) persistLocked() {
	if r.storage == nil {
		return
	}
	err := r.storage.Save(persistentState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log,
	})
	if err != nil {
		// A failed persist means we cannot safely keep any promise this
		// state change implies, so it's safer to crash loudly than
		// continue and risk a silent safety violation.
		log.Fatalf("[%s] FATAL: failed to persist raft state: %v", r.id, err)
	}
}

// Start begins the node as a follower waiting for a leader. Call once.
func (r *Raft) Start() {
	go r.runElectionTimer()
}

func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// runElectionTimer watches for the election timeout elapsing with no
// heartbeat or vote reset, and starts an election when it does. A fresh
// timer is spun up after every election attempt (win, lose, or split vote)
// so the node always has exactly one timer racing against the next leader.
func (r *Raft) runElectionTimer() {
	timeout := randomElectionTimeout()

	r.mu.Lock()
	termStarted := r.currentTerm
	r.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		if r.state == Leader {
			r.mu.Unlock()
			return
		}
		if termStarted != r.currentTerm {
			r.mu.Unlock()
			return
		}
		if elapsed := time.Since(r.electionResetAt); elapsed >= timeout {
			r.mu.Unlock()
			r.startElection()
			return
		}
		r.mu.Unlock()
	}
}

func (r *Raft) resetElectionTimerLocked() {
	r.electionResetAt = time.Now()
}

func (r *Raft) lastLogInfoLocked() (index, term uint64) {
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

// becomeFollowerLocked steps down (or stays down) because we observed a
// higher term. Seeing a higher term anywhere is Raft's signal that our view
// of who's in charge is stale. Advancing currentTerm retires whatever
// election timer goroutine is running (it checks termStarted == currentTerm
// and exits once that's false), so we must start a replacement here — a
// leader's heartbeats mask a dead timer, so if nothing restarts it, this
// node would simply never notice a future leader going silent.
func (r *Raft) becomeFollowerLocked(term uint64) {
	log.Printf("[%s] seeing higher term %d (was %d); becoming follower", r.id, term, r.currentTerm)
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.leaderID = ""
	r.resetElectionTimerLocked()
	r.persistLocked()
	go r.runElectionTimer()
	// Wake anyone blocked in WaitCommitted: if they were waiting on us as
	// leader, we no longer are, and they need to stop waiting and retry
	// against whoever the new leader turns out to be.
	r.cond.Broadcast()
}

func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	savedTerm := r.currentTerm
	r.votedFor = r.id
	r.resetElectionTimerLocked()
	r.persistLocked()
	lastIndex, lastTerm := r.lastLogInfoLocked()
	r.mu.Unlock()

	log.Printf("[%s] starting election for term %d", r.id, savedTerm)

	votes := 1 // vote for self; every further mutation happens under r.mu

	for _, addr := range r.peers {
		go func(addr string) {
			args := RequestVoteArgs{
				Term:         savedTerm,
				CandidateID:  r.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply, err := r.sendRequestVote(addr, args)
			if err != nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.state != Candidate || savedTerm != r.currentTerm {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > (len(r.peers)+1)/2 {
					r.becomeLeaderLocked()
				}
			}
		}(addr)
	}

	// In case this election doesn't resolve (split vote, or we lose),
	// make sure a timer is still running to try again.
	go r.runElectionTimer()
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.id
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)
	lastIndex, _ := r.lastLogInfoLocked()
	for peerID := range r.peers {
		r.nextIndex[peerID] = lastIndex + 1
		r.matchIndex[peerID] = 0
	}

	// Append a no-op entry for this term right away. Figure 8 of the Raft
	// paper forbids a leader from advancing commitIndex over an entry from
	// an earlier term purely because a majority now holds it — only a
	// majority-replicated entry from the leader's OWN term can be committed
	// directly. Without this no-op, data a previous leader already
	// replicated to a majority (and may have even told a client "committed")
	// would sit un-applied on every node until the next real client write,
	// since nothing from the new term would exist yet to pull commitIndex
	// forward over it.
	noop := LogEntry{Term: r.currentTerm, Index: lastIndex + 1, Op: OpNoop}
	r.log = append(r.log, noop)
	r.persistLocked()

	log.Printf("[%s] elected leader for term %d", r.id, r.currentTerm)
	r.cond.Broadcast()

	go r.leaderHeartbeatLoop(r.currentTerm)
}

func (r *Raft) leaderHeartbeatLoop(term uint64) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		r.replicate(term)

		<-ticker.C
		r.mu.Lock()
		if r.state != Leader || r.currentTerm != term {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

// Propose appends a client operation to the leader's log and kicks off
// replication immediately (rather than waiting for the next heartbeat
// tick), returning the index/term to pass to WaitCommitted. ok is false if
// this node isn't currently the leader — the caller must not treat the
// write as accepted and should retry against whoever State() says the
// leader is.
func (r *Raft) Propose(op, key, value string) (index, term uint64, ok bool) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, 0, false
	}
	lastIndex, _ := r.lastLogInfoLocked()
	entry := LogEntry{Term: r.currentTerm, Index: lastIndex + 1, Op: op, Key: key, Value: value}
	r.log = append(r.log, entry)
	index, term = entry.Index, r.currentTerm
	r.persistLocked()
	r.mu.Unlock()

	r.replicate(term)
	return index, term, true
}

// WaitCommitted blocks until the entry at index (proposed during term) has
// been committed (replicated to a majority), or returns false if this node
// stops being leader for that term first — e.g. it crashed, was
// partitioned, or lost an election — meaning the write's fate is unknown
// and the caller should retry rather than assume success or failure.
func (r *Raft) WaitCommitted(index, term uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if r.state != Leader || r.currentTerm != term {
			return false
		}
		if r.commitIndex >= index {
			return true
		}
		r.cond.Wait()
	}
}

// advanceCommitIndexLocked recomputes commitIndex from the leader's view of
// matchIndex. Figure 8 of the Raft paper is the reason for the Term check:
// a leader may only commit an entry by counting replicas directly if that
// entry was created during its OWN current term — committing an older
// entry just because a majority now happens to hold it is unsafe (it could
// be silently overwritten later). Older entries become committed
// indirectly, as a side effect of a later current-term entry committing.
func (r *Raft) advanceCommitIndexLocked() {
	for n := uint64(len(r.log) - 1); n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			continue
		}
		count := 1 // the leader itself
		for peerID := range r.peers {
			if r.matchIndex[peerID] >= n {
				count++
			}
		}
		if count > (len(r.peers)+1)/2 {
			r.commitIndex = n
			r.applyCommittedLocked()
			r.cond.Broadcast()
			return
		}
	}
}

// applyCommittedLocked pushes every entry between lastApplied and
// commitIndex into the state machine, in order. apply must not call back
// into the Raft node (it runs while r.mu is held).
func (r *Raft) applyCommittedLocked() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		if r.apply != nil {
			r.apply(r.log[r.lastApplied])
		}
	}
}

// State returns a snapshot for status reporting.
func (r *Raft) State() (id string, state State, term uint64, leaderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.id, r.state, r.currentTerm, r.leaderID
}
