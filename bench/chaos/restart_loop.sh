#!/usr/bin/env bash
# Chaos loop: every INTERVAL seconds, kill -9 a random (or --leader) node and
# restart it after DOWN seconds. Runs until stopped. Intended to run alongside
# a load generator.
#
#   INTERVAL=3 DOWN=1 MODE=random bench/chaos/restart_loop.sh <N> <workdir> <peers_spec>
set -u
cd "$(dirname "$0")/../.."
. scripts/lib.sh

N="${1:?N}"; WORKDIR="${2:?workdir}"; PEERS="${3:?peers_spec}"
INTERVAL="${INTERVAL:-3}"; DOWN="${DOWN:-1}"; MODE="${MODE:-random}"
: "${NODE_BIN:=build/release/raftkv-node}"

# NODE_PIDS must be exported by the caller as a space-separated list in PIDS.
read -r -a PIDS <<< "${PIDS:?export PIDS=\"p1 .. pN\"}"

while :; do
  sleep "$INTERVAL"
  if [[ "$MODE" == "leader" ]]; then
    v=$(find_leader "$N"); [[ -z "$v" ]] && v=$(( (RANDOM % N) + 1 ))
  else
    v=$(( (RANDOM % N) + 1 ))
  fi
  kill -9 "${PIDS[$((v-1))]}" 2>/dev/null
  echo "$(date +%T) killed n$v"
  sleep "$DOWN"
  "$NODE_BIN" --id="$v" --listen_host=127.0.0.1 --listen_port="$(raft_port "$v")" \
    --peers="$PEERS" --data_dir="$WORKDIR/n$v" \
    --election_timeout_ms=300 --heartbeat_ms=75 --tick_ms=25 \
    >"$WORKDIR/n$v.restart.$(date +%s).log" 2>&1 &
  PIDS[$((v-1))]=$!
  echo "$(date +%T) restarted n$v"
done
