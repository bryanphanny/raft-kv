// Stage 1: a single-node, in-memory key-value store with an HTTP API.
// No persistence yet -- that's Stage 2 (write-ahead log).
package main

import (
	"flag"
	"log"

	"github.com/bryanphanny/raftkv-store/internal/api"
	"github.com/bryanphanny/raftkv-store/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	s := store.New()
	server := api.NewServer(s)

	if err := server.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
