#!/usr/bin/env bash
# Phase 2 process gate: real 3-node cluster elects exactly one leader; on
# `kill -9` of the leader a new distinct leader is elected. Repeat CYCLES times.
set -u
cd "$(dirname "$0")/.."
. scripts/lib.sh

N=3
CYCLES="${CYCLES:-10}"
trap 'stop_cluster' EXIT

start_cluster "$N"
sleep 0.5

fail=0
leader=$(wait_for_leader "$N" 15) || { echo "no leader elected"; exit 1; }
echo "initial leader: n$leader"

for c in $(seq 1 "$CYCLES"); do
  # verify exactly one leader right now
  nlead=0
  for i in $(seq 1 "$N"); do
    s=$(ask "$i" STATUS)
    [[ "$(field "$s" role)" == "leader" ]] && nlead=$((nlead+1))
  done
  if [[ "$nlead" -ne 1 ]]; then echo "cycle $c: $nlead leaders (want 1)"; fail=1; fi

  old=$leader
  oldpid=${NODE_PIDS[$((old-1))]}
  kill -9 "$oldpid" 2>/dev/null
  wait "$oldpid" 2>/dev/null

  # a new leader must emerge among the survivors, different from old
  newleader=""
  for i in $(seq 1 60); do
    cand=""
    for j in $(seq 1 "$N"); do
      [[ "$j" -eq "$old" ]] && continue
      s=$(ask "$j" STATUS)
      [[ "$(field "$s" role)" == "leader" ]] && cand="$j"
    done
    [[ -n "$cand" && "$cand" != "$old" ]] && { newleader="$cand"; break; }
    sleep 0.2
  done
  if [[ -z "$newleader" ]]; then
    echo "cycle $c: FAIL no re-election after killing n$old"
    fail=1
    break
  fi
  echo "cycle $c: n$old killed -> n$newleader elected"

  # restart the old node so the cluster is full again for the next cycle
  rp=$(raft_port "$old")
  "$NODE_BIN" --id="$old" --listen_host=127.0.0.1 --listen_port="$rp" \
    --peers="$(peers_spec "$N")" --data_dir="$WORKDIR/n$old" \
    --election_timeout_ms=300 --heartbeat_ms=75 --tick_ms=25 \
    >"$WORKDIR/n$old.restart$c.log" 2>&1 &
  NODE_PIDS[$((old-1))]=$!
  sleep 0.6
  leader=$newleader
done

if [[ "$fail" -eq 0 ]]; then echo "PASS: $CYCLES/$CYCLES re-elections"; exit 0; fi
echo "FAIL"; exit 1
