// Stage 3b: with no -id flag, a single-node KV store durable via a local
// WAL (Stage 1/2). With -id and -peers, a cluster node whose writes go
// through Raft: proposed to the leader's log, replicated to a majority,
// and only then applied to the KV store on every node.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/bryanphanny/raftkv-store/internal/api"
	"github.com/bryanphanny/raftkv-store/internal/raft"
	"github.com/bryanphanny/raftkv-store/internal/store"
	"github.com/bryanphanny/raftkv-store/internal/wal"
)

// parsePeers turns "n2=http://127.0.0.1:8082,n3=http://127.0.0.1:8083" into
// a peer-id -> base-URL map. It must not include this node's own entry.
func parsePeers(s string) map[string]string {
	peers := make(map[string]string)
	if s == "" {
		return peers
	}
	for _, pair := range strings.Split(s, ",") {
		idAddr := strings.SplitN(pair, "=", 2)
		if len(idAddr) != 2 {
			log.Fatalf("invalid -peers entry %q, want id=http://host:port", pair)
		}
		peers[idAddr[0]] = idAddr[1]
	}
	return peers
}

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	walPath := flag.String("wal", "kvstore.wal", "path to write-ahead log file")
	id := flag.String("id", "", "this node's unique id within the cluster")
	peersFlag := flag.String("peers", "", "comma-separated peer-id=http://host:port list (excluding this node)")
	raftStatePath := flag.String("raftstate", "", "path to persist Raft term/votedFor/log (default: <id>.raftstate)")
	flag.Parse()

	if *id != "" && *raftStatePath == "" {
		*raftStatePath = *id + ".raftstate"
	}

	s := store.New()
	mux := http.NewServeMux()

	var apiServer *api.Server

	if *id == "" {
		w, err := wal.Open(*walPath)
		if err != nil {
			log.Fatalf("open wal: %v", err)
		}
		defer w.Close()

		if err := w.Replay(func(e wal.Entry) {
			switch e.Op {
			case wal.OpPut:
				s.Put(e.Key, e.Value)
			case wal.OpDelete:
				s.Delete(e.Key)
			}
		}); err != nil {
			log.Fatalf("replay wal: %v", err)
		}

		apiServer = api.NewServer(s, w, nil)
	} else {
		node := raft.New(raft.Config{
			ID:    *id,
			Peers: parsePeers(*peersFlag),
		}, *raftStatePath, func(e raft.LogEntry) {
			switch e.Op {
			case raft.OpPut:
				s.Put(e.Key, e.Value)
			case raft.OpDelete:
				s.Delete(e.Key)
			}
		})
		node.Start()
		mux.Handle("/raft/", node.Handler())

		apiServer = api.NewServer(s, nil, node)
	}

	mux.Handle("/kv/", apiServer.Handler())

	log.Printf("kvserver listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
