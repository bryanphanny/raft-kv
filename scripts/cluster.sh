#!/usr/bin/env bash
# Run a local 3-node RaftKV cluster for demos.
#
#   scripts/cluster.sh start   # build + launch n1/n2/n3 on :9091-:9093
#   scripts/cluster.sh status  # print each node's Raft state (id/term/leader)
#   scripts/cluster.sh stop    # kill the nodes, keep their data/state on disk
#   scripts/cluster.sh clean   # stop + wipe all data/state
#
# Demo script: start, write a key through whichever node is leader (status
# tells you which), kill -9 that node's pid (see .cluster/n*.pid), watch
# status show a new leader within ~1s, confirm the key survived, restart the
# killed node and watch it rejoin as a follower and catch back up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/kvserver"
RUN_DIR="$ROOT/.cluster"

PEERS_N1="n2=http://127.0.0.1:9092,n3=http://127.0.0.1:9093"
PEERS_N2="n1=http://127.0.0.1:9091,n3=http://127.0.0.1:9093"
PEERS_N3="n1=http://127.0.0.1:9091,n2=http://127.0.0.1:9092"

start() {
  mkdir -p "$RUN_DIR"
  go build -o "$BIN" "$ROOT/cmd/kvserver"

  "$BIN" -addr :9091 -id n1 -peers "$PEERS_N1" -raftstate "$RUN_DIR/n1.raftstate" >"$RUN_DIR/n1.log" 2>&1 &
  echo $! >"$RUN_DIR/n1.pid"
  "$BIN" -addr :9092 -id n2 -peers "$PEERS_N2" -raftstate "$RUN_DIR/n2.raftstate" >"$RUN_DIR/n2.log" 2>&1 &
  echo $! >"$RUN_DIR/n2.pid"
  "$BIN" -addr :9093 -id n3 -peers "$PEERS_N3" -raftstate "$RUN_DIR/n3.raftstate" >"$RUN_DIR/n3.log" 2>&1 &
  echo $! >"$RUN_DIR/n3.pid"

  sleep 1
  echo "cluster started (pids in $RUN_DIR/*.pid, logs in $RUN_DIR/*.log)"
  status
}

restart_node() {
  id="$1"
  case "$id" in
    n1) addr=:9091; peers="$PEERS_N1" ;;
    n2) addr=:9092; peers="$PEERS_N2" ;;
    n3) addr=:9093; peers="$PEERS_N3" ;;
    *) echo "unknown node $id (want n1, n2, or n3)"; exit 1 ;;
  esac
  "$BIN" -addr "$addr" -id "$id" -peers "$peers" -raftstate "$RUN_DIR/$id.raftstate" >>"$RUN_DIR/$id.log" 2>&1 &
  echo $! >"$RUN_DIR/$id.pid"
  echo "$id restarted (pid $(cat "$RUN_DIR/$id.pid"))"
}

stop() {
  for f in "$RUN_DIR"/*.pid; do
    [ -f "$f" ] || continue
    kill -9 "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  echo "cluster stopped"
}

status() {
  for p in 9091 9092 9093; do
    curl -s "http://127.0.0.1:$p/raft/status" || echo "{\"port\":$p,\"state\":\"unreachable\"}"
    echo
  done
}

clean() {
  stop
  rm -rf "$RUN_DIR"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  clean) clean ;;
  restart) restart_node "${2:-}" ;;
  *)
    echo "usage: $0 {start|stop|status|clean|restart <n1|n2|n3>}"
    exit 1
    ;;
esac
