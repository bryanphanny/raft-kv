package raft

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// RequestVoteArgs/Reply and AppendEntriesArgs/Reply mirror the RPCs from the
// Raft paper, Figure 2 / Section 5.

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// RequestVote decides whether to grant a vote to a candidate. A vote is
// granted only if the candidate's term is current, we haven't already voted
// for someone else this term, and the candidate's log is at least as
// up-to-date as ours (the Raft "election restriction" — this is what
// guarantees a new leader can never be missing a committed entry).
func (r *Raft) RequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}

	reply := RequestVoteReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		return reply
	}

	lastIndex, lastTerm := r.lastLogInfoLocked()
	logUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (r.votedFor == "" || r.votedFor == args.CandidateID) && logUpToDate {
		reply.VoteGranted = true
		r.votedFor = args.CandidateID
		r.resetElectionTimerLocked()
		r.persistLocked()
	}
	return reply
}

// AppendEntries is both the heartbeat and the log-replication RPC: a leader
// sends it (with Entries possibly empty, for a plain heartbeat) to assert
// "I'm still leader" and to push new log entries.
func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}

	reply := AppendEntriesReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.Success = false
		return reply
	}

	// A legitimate AppendEntries at our current (or higher) term means its
	// sender is the leader; any contact from the leader resets our timer so
	// we don't start a needless election.
	r.state = Follower
	r.leaderID = args.LeaderID
	r.resetElectionTimerLocked()

	// Log consistency check (Raft's "AppendEntries consistency check"): we
	// only accept these entries if we already agree with the leader on
	// everything up to PrevLogIndex. If we don't have an entry there, or it
	// was written in a different term, reject — the leader will back up
	// nextIndex and retry with earlier entries until it finds the point
	// where our logs agree.
	if args.PrevLogIndex >= uint64(len(r.log)) {
		reply.Success = false
		return reply
	}
	if r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return reply
	}

	// Splice in the new entries: on the first divergence from our existing
	// log, truncate everything from there on (it belongs to an old, failed
	// leader's term and must not survive) and append the leader's version.
	insertAt := args.PrevLogIndex + 1
	for i, e := range args.Entries {
		idx := insertAt + uint64(i)
		if idx < uint64(len(r.log)) {
			if r.log[idx].Term == e.Term {
				continue // already have this exact entry
			}
			r.log = append(r.log[:idx], args.Entries[i:]...)
			break
		}
		r.log = append(r.log, args.Entries[i:]...)
		break
	}
	if len(args.Entries) > 0 {
		r.persistLocked()
	}

	if args.LeaderCommit > r.commitIndex {
		lastNewIndex, _ := r.lastLogInfoLocked()
		if args.LeaderCommit < lastNewIndex {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastNewIndex
		}
		r.applyCommittedLocked()
		r.cond.Broadcast()
	}

	reply.Success = true
	return reply
}

// replicate sends every peer whatever log entries they're missing (or, if
// they're fully caught up, an empty AppendEntries as a heartbeat). Called
// both periodically (leaderHeartbeatLoop) and immediately after Propose, so
// a fresh write doesn't have to wait for the next heartbeat tick to start
// replicating.
func (r *Raft) replicate(term uint64) {
	r.mu.Lock()
	if r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	peers := r.peers
	r.mu.Unlock()

	for peerID, addr := range peers {
		go r.replicateToPeer(term, peerID, addr)
	}
}

func (r *Raft) replicateToPeer(term uint64, peerID, addr string) {
	r.mu.Lock()
	if r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	nextIdx := r.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := r.log[prevLogIndex].Term
	var entries []LogEntry
	if nextIdx < uint64(len(r.log)) {
		entries = append(entries, r.log[nextIdx:]...)
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	reply, err := r.sendAppendEntries(addr, args)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return
	}

	if reply.Success {
		r.matchIndex[peerID] = prevLogIndex + uint64(len(entries))
		r.nextIndex[peerID] = r.matchIndex[peerID] + 1
		r.advanceCommitIndexLocked()
		return
	}

	// Log mismatch: back up one entry and retry on the next replication
	// round. Backing up by exactly one is the simplest correct strategy;
	// a real system would have the follower report how far back to jump to
	// avoid an O(log length) retry chain after a long partition.
	if r.nextIndex[peerID] > 1 {
		r.nextIndex[peerID]--
	}
}

// --- HTTP client side (sending RPCs to peers) ---

func doRPC[Args any, Reply any](client *http.Client, url string, args Args) (Reply, error) {
	var reply Reply

	body, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func (r *Raft) sendRequestVote(addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	return doRPC[RequestVoteArgs, RequestVoteReply](r.client, addr+"/raft/requestvote", args)
}

func (r *Raft) sendAppendEntries(addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	return doRPC[AppendEntriesArgs, AppendEntriesReply](r.client, addr+"/raft/appendentries", args)
}

// --- HTTP server side (handling RPCs from peers, and status for curl/demo) ---

func (r *Raft) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/raft/requestvote", func(w http.ResponseWriter, req *http.Request) {
		var args RequestVoteArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		reply := r.RequestVote(args)
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/appendentries", func(w http.ResponseWriter, req *http.Request) {
		var args AppendEntriesArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		reply := r.AppendEntries(args)
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/status", func(w http.ResponseWriter, req *http.Request) {
		id, state, term, leaderID := r.State()
		json.NewEncoder(w).Encode(map[string]any{
			"id":       id,
			"state":    state.String(),
			"term":     term,
			"leaderId": leaderID,
		})
	})

	return mux
}
