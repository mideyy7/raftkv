#!/usr/bin/env bash
# Cut network connectivity between a MINORITY of nodes and the rest.
#
#   partition_minority.sh <N> <m1> [m2 ...]        # isolate listed nodes
#   partition_minority.sh --heal <N>               # remove all blocks
#
# macOS: app-level NETBLOCK/NETUNBLOCK on the node's client port (no root).
# Linux: also drops `iptables -A INPUT -s <peer-ip> -j DROP` between the raft
#        ports (needs privileges); the app-level block is applied regardless so
#        the test is meaningful on both.
set -u
cd "$(dirname "$0")/../.."
. scripts/lib.sh

if [[ "${1:-}" == "--heal" ]]; then
  N="$2"
  for a in $(seq 1 "$N"); do for b in $(seq 1 "$N"); do
    [[ "$a" != "$b" ]] && ask "$a" NETUNBLOCK "$b" >/dev/null
  done; done
  [[ "$(uname)" == "Linux" ]] && sudo iptables -F 2>/dev/null || true
  echo "healed"
  exit 0
fi

N="$1"; shift
MIN=("$@")
MAJ=()
for i in $(seq 1 "$N"); do
  skip=0; for m in "${MIN[@]}"; do [[ "$i" == "$m" ]] && skip=1; done
  [[ "$skip" == 0 ]] && MAJ+=("$i")
done

for a in "${MIN[@]}"; do for b in "${MAJ[@]}"; do
  ask "$a" NETBLOCK "$b" >/dev/null
  ask "$b" NETBLOCK "$a" >/dev/null
  if [[ "$(uname)" == "Linux" ]]; then
    pa=$(raft_port "$a"); pb=$(raft_port "$b")
    sudo iptables -A INPUT -p tcp --dport "$pa" -s 127.0.0.1 -j DROP 2>/dev/null || true
    sudo iptables -A INPUT -p tcp --dport "$pb" -s 127.0.0.1 -j DROP 2>/dev/null || true
  fi
done; done
echo "partitioned minority={${MIN[*]}} | majority={${MAJ[*]}}"
