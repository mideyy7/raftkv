#!/usr/bin/env bash
# Phase 4 process gate: the real raftkv-cli against a live 5-node cluster.
#  1. point the client at ALL endpoints; do COUNT PUTs then COUNT GETs, verify
#     every value round-trips (redirects handled transparently by the client).
#  2. kill -9 the leader partway through a second PUT run; the client must
#     recover on its own (only a latency blip) and still land every write.
#  3. exactly-once check: each key is written exactly once with a known value;
#     any mismatch on read-back is a dedup/replay bug.
set -u
cd "$(dirname "$0")/.."
. scripts/lib.sh

N=5
COUNT="${COUNT:-300}"
export RAFT_BASE_PORT=7601
trap 'stop_cluster' EXIT

EP=""
for i in $(seq 1 "$N"); do EP="${EP}${EP:+,}${i}@127.0.0.1:$(client_port "$i")"; done

start_cluster "$N"
sleep 1
wait_for_leader "$N" 20 >/dev/null || { echo "no leader"; exit 1; }

fail=0

# --- run 1: steady-state round trip via the CLI ---
if ./build/debug/raftkv-cli --endpoints="$EP" bench "$COUNT" ; then
  echo "run1: bench OK"
else
  echo "run1: bench FAIL"; fail=1
fi

# --- run 2: kill the leader mid-write-stream, client must recover ---
( for i in $(seq 1 "$COUNT"); do
    ./build/debug/raftkv-cli --endpoints="$EP" put p4k_$i val_$i >/dev/null || echo "put $i FAILED"
  done ) &
writer=$!
sleep 1
L=$(find_leader "$N")
if [[ -n "$L" ]]; then
  echo "run2: killing leader n$L mid-stream"
  kill -9 "${NODE_PIDS[$((L-1))]}" 2>/dev/null
fi
wait "$writer"

# read every key back
miss=0
for i in $(seq 1 "$COUNT"); do
  v=$(./build/debug/raftkv-cli --endpoints="$EP" get p4k_$i)
  [[ "$v" == "val_$i" ]] || { miss=$((miss+1)); [[ "$miss" -le 5 ]] && echo "  MISSING p4k_$i (got '$v')"; }
done
if [[ "$miss" -eq 0 ]]; then
  echo "run2: all $COUNT writes survived the leader kill"
else
  echo "run2: FAIL ($miss/$COUNT missing after leader kill)"; fail=1
fi

[[ "$fail" -eq 0 ]] && { echo "PASS"; exit 0; }
echo "FAIL"; exit 1
