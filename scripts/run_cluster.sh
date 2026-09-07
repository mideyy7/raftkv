#!/usr/bin/env bash
# Start an N-node raftkv cluster (default 5) and print the client endpoints.
# Runs until you press Ctrl-C. Env: N, RAFT_BASE_PORT, NODE_BIN, DATA_DIR.
set -u
cd "$(dirname "$0")/.."
. scripts/lib.sh

N="${N:-5}"
export RAFT_BASE_PORT="${RAFT_BASE_PORT:-9001}"
KEEP_DATA="${DATA_DIR:-}"

if [[ -n "$KEEP_DATA" ]]; then
  WORKDIR="$KEEP_DATA"; mkdir -p "$WORKDIR"
  peers=$(peers_spec "$N"); NODE_PIDS=()
  for i in $(seq 1 "$N"); do
    mkdir -p "$WORKDIR/n$i"
    "$NODE_BIN" --id="$i" --listen_host=127.0.0.1 --listen_port="$(raft_port "$i")" \
      --peers="$peers" --data_dir="$WORKDIR/n$i" \
      --election_timeout_ms="${ELECTION_MS:-300}" --heartbeat_ms="${HB_MS:-75}" --tick_ms="${TICK_MS:-25}" \
      >"$WORKDIR/n$i.log" 2>&1 &
    NODE_PIDS+=("$!")
  done
else
  start_cluster "$N"
fi

trap 'stop_cluster; exit 0' INT TERM
sleep 1
EP=""
for i in $(seq 1 "$N"); do EP="${EP}${EP:+,}${i}@127.0.0.1:$(client_port "$i")"; done
L=$(wait_for_leader "$N" 20 || echo "?")
echo "cluster up: N=$N  data=$WORKDIR  leader=n$L"
echo "ENDPOINTS=$EP"
echo "(Ctrl-C to stop)"
while true; do sleep 3600; done
