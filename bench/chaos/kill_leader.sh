#!/usr/bin/env bash
# Find the current leader of an N-node cluster and kill -9 it. Prints the
# wall-clock instant of the kill (ns) so an external harness can line it up.
#   kill_leader.sh <N>            (expects NODE_PIDS via a sourced lib.sh caller)
# Standalone: pass --pids "p1 p2 .. pN" so it can actually kill.
set -u
cd "$(dirname "$0")/../.."
. scripts/lib.sh

N="${1:-5}"
PIDS_STR="${PIDS:-}"
L=$(find_leader "$N")
[[ -z "$L" ]] && { echo "no leader"; exit 1; }
if [[ -n "$PIDS_STR" ]]; then
  read -r -a P <<< "$PIDS_STR"
  kill -9 "${P[$((L-1))]}" 2>/dev/null
fi
echo "KILLED_LEADER=n$L KILL_WALL_NS=$(python3 -c 'import time;print(time.time_ns())')"
