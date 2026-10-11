# RaftKV — a Raft-backed distributed key-value store

## Why this project exists

Bryan is targeting software engineering internships in cloud infrastructure /
networking / databases — systems-level roles, not general full-stack. He was
building a Python personal finance risk-analytics platform (separate repo:
`risk-analytics-platform`) and correctly recognized it teaches finance and
data engineering, not the sockets/consensus/storage-engine fundamentals that
infra roles actually probe for.

Research into what real infra companies and courses treat as legitimate
signal (see decision log below) converged on: **language choice isn't the
gate, and "used by many" isn't the gate — depth of what's actually
implemented from scratch is.** A Raft-backed KV store is the single highest-
leverage project for this goal because it's *literally* what etcd is (the
thing that stores all of Kubernetes' cluster state), and it's the capstone
project of MIT's own graduate distributed-systems course (6.5840, formerly
6.824) — meaning there's an external, well-calibrated difficulty benchmark
for it, not a made-up one.

**Decision log (from the prior research conversation):**
- Full pivot away from the finance project was *not* recommended — keep it as
  a shipped secondary piece; the domain (finance) and language (Python)
  aren't disqualifying, per multiple sources on what actually reads as
  "systems-relevant" to reviewers.
- This project (RaftKV) is the new primary/flagship piece.
- Chosen over alternatives (DNS server, storage engine, container runtime)
  because it has the strongest external validation and most directly mirrors
  a real, load-bearing piece of cloud infrastructure (etcd/Consul).
- Language: Go. Not because language is the point, but because Go is
  literally what etcd, Kubernetes, and most of the real coordination-layer
  ecosystem is written in — learning Go's concurrency primitives here is
  learning the same primitives the real systems use, not translating from
  something else later.

## The build plan (staged, matching MIT 6.5840's own lab structure)

Each stage is a fully working, demonstrable state on its own — don't wait
until Stage 3 to have something to show.

### Stage 1 — single-node KV store over HTTP ✅ DONE
In-memory map behind a `sync.RWMutex`, exposed over a plain HTTP API. No
persistence, no replication yet — this stage exists purely to get the
storage interface and API shape right before anything gets harder.

```
PUT    /kv/foo   (body: "bar")   -> 200
GET    /kv/foo                   -> 200, body "bar"
GET    /kv/nope                  -> 404
DELETE /kv/foo                   -> 200
```

Verified working (see `internal/store/store.go`, `internal/api/server.go`,
`cmd/kvserver/main.go`). Run it:

```
go build -o bin/kvserver ./cmd/kvserver
./bin/kvserver -addr :8080
curl -X PUT --data "bar" http://localhost:8080/kv/foo
curl http://localhost:8080/kv/foo
```

### Stage 2 — write-ahead log (WAL) persistence ✅ DONE (single-node mode)
Every write appends an entry to an on-disk log *before* it's applied to the
in-memory map. On startup, the log is replayed from the beginning to rebuild
state. This is the same reason every real database (Postgres, SQLite,
RocksDB) has a WAL: it's what makes a crash mid-write recoverable instead of
silently corrupting.

Log format (one JSON line per operation, append-only file, see
`internal/wal/wal.go`):
```json
{"seq": 42, "op": "PUT", "key": "foo", "value": "bar"}
{"seq": 43, "op": "DELETE", "key": "foo"}
```

This path is only used when `kvserver` is run without `-id` (single-node
mode). In cluster mode (Stage 3), the replicated Raft log supersedes it —
see Stage 3's persistence piece below.

### Stage 3 — Raft consensus ✅ DONE (leader election, log replication, persistence/crash recovery)
Replicates the KV store across a 3-node cluster so it survives any one node
crashing. Lives in `internal/raft/` (`raft.go` core state machine,
`transport.go` HTTP RPC layer + handlers, `persist.go` durable state).

1. **Leader election** ✅ — nodes start as followers, use randomized
   election timeouts (150-300ms), become candidates and request votes if
   they don't hear from a leader in time. Verified: exactly one leader at
   any time across 3 nodes, clean re-election (with a bumped term) when the
   leader is `kill -9`'d, no flapping once settled.
2. **Log replication** ✅ — the leader appends client operations to its own
   log, replicates via `AppendEntries` (with the real prevLogIndex/
   prevLogTerm consistency check + conflicting-suffix truncation, not just a
   heartbeat), and advances `commitIndex` once a majority of nodes have an
   entry **from the leader's own current term** (Raft paper Figure 8 — an
   older-term entry can only commit indirectly, as a side effect of a
   later current-term entry committing). Verified: a write committed before
   a leader crash survives under the newly elected leader; writes continue
   succeeding against the new leader afterward.
3. **Persistence & crash recovery** ✅ — `currentTerm`/`votedFor`/`log` are
   fsync'd to disk (atomic temp-file-then-rename) before a node replies to
   any RPC that depends on the change, and reloaded on restart
   (`raft.New`'s `storagePath` param). Verified: a killed node rejoins with
   its prior log intact, catches up on entries it missed via the normal
   `AppendEntries` consistency check, and a restarted ex-leader correctly
   rejoins as a follower under the new leader's higher term with no
   split-brain.
   - **Known subtlety hit and fixed during this build**: a brand-new leader
     cannot advance `commitIndex` over *any* entry — even ones a majority
     already has durably on disk — until something from its own term
     commits (that's the Figure-8 rule above). Without a fix, data a
     previous leader had already told a client was "committed" would sit
     un-applied on every node's state machine until the next real client
     write happened to occur. Fixed the standard way: a new leader appends
     a no-op log entry for its own term immediately upon election, which
     commits on its own and pulls `commitIndex` forward over everything
     before it.

Core RPCs (straight from the paper, Section 5), implemented in
`internal/raft/transport.go`:
```
RequestVote(term, candidateId, lastLogIndex, lastLogTerm)
  -> (term, voteGranted)

AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)
  -> (term, success)
```

**Known gaps / deliberately out of scope so far:**
- **Reads are not linearizable.** `GET /kv/{key}` is served from whichever
  node you hit, reading local state — a follower can be briefly behind the
  leader. Fixing this (lease reads or a read-index round through the
  leader) is a real, well-known Raft extension, just not built yet.
- **No log compaction.** `nextIndex` only ever backs up one entry at a time
  on a mismatch (see the stretch goal below) and the log grows unbounded.

### Stretch — snapshotting
Once the log is large, compact it into a snapshot so a lagging node can be
brought up to date with one transfer instead of replaying the entire
history. Only attempt this after Stage 3 is solid.

## Concepts glossary (for reading the docs below)

- **Term** — a logical clock in Raft; increases every time a new election
  happens. Used to detect stale leaders.
- **Quorum / majority** — an entry is only committed once more than half the
  nodes have it, so the cluster tolerates minority failures.
- **Leader / follower / candidate** — the three states a Raft node can be in.
- **Log replication** — the leader is the only node that accepts writes;
  it propagates them to followers as log entries.
- **Linearizability** — the consistency guarantee Raft provides: the
  cluster behaves as if there's a single copy of the data, even though
  there are three.
- **WAL (write-ahead log)** — durability mechanism: write intent to disk
  before mutating in-memory state, so a crash mid-write is recoverable.

## Where to actually read the docs (in this order)

1. **The Raft paper** — "In Search of an Understandable Consensus
   Algorithm" (Ongaro & Ousterhout). https://raft.github.io/raft.pdf —
   read this *before* writing any Stage 3 code. Section 5 is the whole
   algorithm; Figure 2 is the one-page reference you'll keep re-opening.
2. **The Secret Lives of Data — Raft visualization** —
   http://thesecretlivesofdata.com/raft/ — an interactive visual walkthrough
   of leader election and log replication. Read alongside the paper, not
   instead of it.
3. **MIT 6.5840 Lab 3 (Raft) spec** —
   http://nil.csail.mit.edu/6.5840/2024/labs/lab-raft.html — the official
   staged difficulty breakdown (2A moderate, 2B/2C/2D hard) this project's
   plan is modeled on. Even if not doing the course, this spec is a good
   sanity check for what "done" looks like at each stage.
4. **Go documentation**, in the order you'll need them:
   - `net/http` — https://pkg.go.dev/net/http (Stage 1, done)
   - `sync` (Mutex/RWMutex) — https://pkg.go.dev/sync (Stage 1, done)
   - `encoding/json` — https://pkg.go.dev/encoding/json (Stage 2)
   - `os` file I/O (append-mode writes, fsync) — https://pkg.go.dev/os
     (Stage 2)
   - `net/rpc` or plain HTTP for inter-node RPCs — decide when starting
     Stage 3; plain HTTP handlers are easier to debug with curl while
     building.
5. **etcd's raft library** — https://github.com/etcd-io/raft — production
   Go Raft implementation. Deliberately listed last: read this *after*
   struggling with your own Stage 3 attempt, not before, or it stops being
   your own understanding.

## Repo layout

```
cmd/kvserver/       entry point (main.go) — single-node or cluster mode via -id/-peers
internal/store/     the actual KV storage (in-memory map + RWMutex)
internal/api/       HTTP handlers wrapping the store; writes go through Raft when configured
internal/wal/       Stage 2 write-ahead log (single-node mode only)
internal/raft/      Stage 3: raft.go (core state machine), transport.go (RPCs + HTTP), persist.go (durable state)
scripts/cluster.sh  start/stop/restart/status a local 3-node demo cluster
```

## Current status

- [x] Stage 1 — single-node KV store, HTTP API, manually verified with curl
- [x] Stage 2 — WAL persistence (single-node mode)
- [x] Stage 3 — Raft replication across 3 nodes: leader election, log
      replication with commit-index advancement, persistence/crash
      recovery. All manually verified live (kill -9 the leader mid-cluster,
      confirm re-election + no data loss + restarted node rejoins
      correctly). Known gaps: reads aren't linearizable, no log compaction.
- [ ] Stretch — snapshotting (not started)

Run a demo cluster: `scripts/cluster.sh start`, then `curl`/kill/restart
nodes per the instructions at the top of that script.

Secondary project: `risk-analytics-platform` (Python/FastAPI, finance risk
analytics) — kept as a shipped portfolio piece, not being actively developed
in parallel with this one right now.
