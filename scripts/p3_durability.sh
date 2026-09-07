#!/usr/bin/env bash
# Phase 3 process gate: 5 real node processes. Drive COUNT writes through the
# leader; every COUNT/KILLS writes, `kill -9` a random node (or the leader, with
# ALWAYS_LEADER=1) and restart it. At the end, from the leader, confirm every
# acknowledged key is present with the right value -> zero committed write loss.
# Repeat CYCLES times.
set -u
cd "$(dirname "$0")/.."
. scripts/lib.sh

N=5
COUNT="${COUNT:-300}"
KILLS="${KILLS:-4}"
CYCLES="${CYCLES:-2}"
ALWAYS_LEADER="${ALWAYS_LEADER:-0}"
trap 'stop_cluster' EXIT

# hit node $1 with "$2..." once; echo reply
hit() { local id="$1"; shift; printf '%s\nQUIT\n' "$*" | nc -w 1 127.0.0.1 "$(client_port "$id")" 2>/dev/null | head -1; }

overall=0
for cyc in $(seq 1 "$CYCLES"); do
  # distinct port range per cycle so a stray process can never collide
  export RAFT_BASE_PORT=$(( 7001 + cyc * 40 ))
  start_cluster "$N"
  sleep 0.5
  L=$(wait_for_leader "$N" 20) || { echo "cycle $cyc: no leader"; overall=1; stop_cluster; continue; }

  ACK="$WORKDIR/acked.txt"; : > "$ACK"
  kill_every=$(( COUNT / KILLS )); [[ "$kill_every" -lt 1 ]] && kill_every=1

  for i in $(seq 1 "$COUNT"); do
    key="k_${cyc}_${i}"; val="val_${cyc}_${i}"
    ok=""
    for att in $(seq 1 30); do
      resp=$(hit "$L" "PUT $key $val $cyc $i")
      case "$resp" in
        OK*)       ok=1; break ;;
        REDIRECT*) L=$(echo "$resp" | awk '{print $2}'); [[ -z "$L" || "$L" == 0 ]] && L=$(find_leader "$N") ;;
        *)         L=$(find_leader "$N") ;;
      esac
      [[ -z "$L" ]] && { L=1; sleep 0.2; }
    done
    [[ -n "$ok" ]] && echo "$key $val" >> "$ACK"

    if [[ $(( i % kill_every )) -eq 0 && "$i" -lt "$COUNT" ]]; then
      if [[ "$ALWAYS_LEADER" -eq 1 ]]; then
        victim="$L"; [[ -z "$victim" || "$victim" == 0 ]] && victim=$(( (RANDOM % N) + 1 ))
      else
        victim=$(( (RANDOM % N) + 1 ))
      fi
      vpid=${NODE_PIDS[$((victim-1))]}
      kill -9 "$vpid" 2>/dev/null; wait "$vpid" 2>/dev/null
      rp=$(raft_port "$victim")
      "$NODE_BIN" --id="$victim" --listen_host=127.0.0.1 --listen_port="$rp" \
        --peers="$(peers_spec "$N")" --data_dir="$WORKDIR/n$victim" \
        --election_timeout_ms=300 --heartbeat_ms=75 --tick_ms=25 \
        >"$WORKDIR/n$victim.r${cyc}_${i}.log" 2>&1 &
      NODE_PIDS[$((victim-1))]=$!
      L=$(wait_for_leader "$N" 15)
    fi
  done

  sleep 1
  L=$(wait_for_leader "$N" 20)
  nack=$(wc -l < "$ACK" | tr -d ' ')
  miss=0
  while read -r key val; do
    got=""
    for att in $(seq 1 20); do
      resp=$(hit "$L" "GET $key")
      case "$resp" in
        "VALUE $val") got=1; break ;;
        VALUE*|NIL)   break ;;
        REDIRECT*)    L=$(echo "$resp" | awk '{print $2}'); [[ -z "$L" || "$L" == 0 ]] && L=$(find_leader "$N") ;;
        *)            L=$(find_leader "$N") ;;
      esac
      [[ -z "$L" ]] && { L=1; sleep 0.2; }
    done
    [[ -z "$got" ]] && { miss=$((miss+1)); [[ "$miss" -le 5 ]] && echo "  MISSING $key"; }
  done < "$ACK"

  if [[ "$miss" -eq 0 ]]; then
    echo "cycle $cyc: OK  ($nack acked, all present after $KILLS kill/restarts)"
  else
    echo "cycle $cyc: FAIL ($miss of $nack acked keys missing)"; overall=1
  fi
  stop_cluster
done

[[ "$overall" -eq 0 ]] && { echo "PASS: zero committed write loss"; exit 0; }
echo "FAIL"; exit 1
