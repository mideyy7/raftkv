#!/usr/bin/env bash
# Phase 2 process gate: 5-node cluster, app-level partition (NETBLOCK) isolates
# the leader + one follower (minority of 2) from the other 3. The majority side
# must elect a new leader; the minority must NOT have a leader at a term >= the
# majority's. Healing (NETUNBLOCK) must converge back to a single leader.
set -u
cd "$(dirname "$0")/.."
. scripts/lib.sh

N=5
trap 'stop_cluster' EXIT
start_cluster "$N"
sleep 0.5

l1=$(wait_for_leader "$N" 15) || { echo "no initial leader"; exit 1; }
echo "initial leader: n$l1"

# minority = {l1, one other}; majority = the remaining 3
minority=("$l1")
for i in $(seq 1 "$N"); do
  [[ "$i" -ne "$l1" && ${#minority[@]} -lt 2 ]] && minority+=("$i")
done
majority=()
for i in $(seq 1 "$N"); do
  skip=0
  for m in "${minority[@]}"; do [[ "$i" -eq "$m" ]] && skip=1; done
  [[ "$skip" -eq 0 ]] && majority+=("$i")
done
echo "minority: ${minority[*]}   majority: ${majority[*]}"

# install the block both directions
for a in "${minority[@]}"; do
  for b in "${majority[@]}"; do
    ask "$a" NETBLOCK "$b" >/dev/null
    ask "$b" NETBLOCK "$a" >/dev/null
  done
done

# majority side elects a new leader
newleader=""
for i in $(seq 1 80); do
  for j in "${majority[@]}"; do
    s=$(ask "$j" STATUS)
    [[ "$(field "$s" role)" == "leader" ]] && newleader="$j"
  done
  [[ -n "$newleader" ]] && break
  sleep 0.2
done
[[ -z "$newleader" ]] && { echo "FAIL majority elected no leader"; exit 1; }
maj_term=$(field "$(ask "$newleader" STATUS)" term)
echo "majority elected n$newleader (term $maj_term)"

# minority must not have a leader at term >= maj_term
bad=0
for j in "${minority[@]}"; do
  s=$(ask "$j" STATUS)
  r=$(field "$s" role); t=$(field "$s" term)
  if [[ "$r" == "leader" && "${t:-0}" -ge "${maj_term:-0}" ]]; then
    echo "FAIL minority n$j is leader at term $t >= majority term $maj_term"
    bad=1
  fi
done
[[ "$bad" -ne 0 ]] && exit 1
echo "minority correctly has no competing leader"

# heal
for a in "${minority[@]}"; do
  for b in "${majority[@]}"; do
    ask "$a" NETUNBLOCK "$b" >/dev/null
    ask "$b" NETUNBLOCK "$a" >/dev/null
  done
done

# converge to exactly one leader across all 5
for i in $(seq 1 60); do
  cnt=0; lead=""
  for j in $(seq 1 "$N"); do
    s=$(ask "$j" STATUS)
    [[ "$(field "$s" role)" == "leader" ]] && { cnt=$((cnt+1)); lead="$j"; }
  done
  [[ "$cnt" -eq 1 ]] && { echo "healed: single leader n$lead"; echo "PASS"; exit 0; }
  sleep 0.2
done
echo "FAIL did not converge to a single leader after heal"
exit 1
