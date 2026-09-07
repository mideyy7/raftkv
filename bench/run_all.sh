#!/usr/bin/env bash
# Regenerate every RaftKV benchmark number into docs/RESULTS/ and print a
# summary. Scale with env vars; defaults are a real-but-quick pass on one
# laptop (5 node processes, separate data dirs + ports).
#
#   DUR=15 CURVE="10 50 100 200" RECOVERY_TRIALS=30 bench/run_all.sh
set -u
cd "$(dirname "$0")/.."
. scripts/lib.sh

BUILD="${BUILD:-build/release}"
NODE_BIN="$BUILD/raftkv-node"
LG="$BUILD/raftkv-loadgen"
CLI="$BUILD/raftkv-cli"
export NODE_BIN
N=5
DUR="${DUR:-15}"
WARMUP="${WARMUP:-5}"
CURVE="${CURVE:-10 50 100 200}"
RECOVERY_TRIALS="${RECOVERY_TRIALS:-30}"
OUT=docs/RESULTS
mkdir -p "$OUT"
STAMP=$(date +%Y%m%d_%H%M%S)
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo nogit)
MACHINE="$(uname -mrs) / $(sysctl -n hw.ncpu 2>/dev/null || nproc) cpu"

for b in "$NODE_BIN" "$LG" "$CLI"; do
  [[ -x "$b" ]] || { echo "missing $b -- build the $BUILD preset first"; exit 2; }
done

endpoints() { local e="" i; for i in $(seq 1 "$N"); do e="${e}${e:+,}${i}@127.0.0.1:$(client_port "$i")"; done; echo "$e"; }

hdr() { echo "run $STAMP  commit $COMMIT  $MACHINE  nodes=$N  election=300ms hb=75ms tick=25ms"; }

# ---------------------------------------------------------------------------
echo "=============================================================="
echo " BENCHMARK 1 -- steady-state write throughput + latency curve"
echo "=============================================================="
B1="$OUT/${STAMP}_bm1_write_curve.txt"
{ hdr; echo; printf "%-6s %-14s %-10s %-10s %-10s\n" conns "ops/s(ok)" p50_ms p99_ms p999_ms; } | tee "$B1"
for C in $CURVE; do
  export RAFT_BASE_PORT=$((9100 + C))
  start_cluster "$N"; sleep 1; wait_for_leader "$N" 20 >/dev/null
  csv="$WORKDIR/bm1_c${C}.csv"
  "$LG" --endpoints="$(endpoints)" -c "$C" -d "$DUR" --mix=put=100 \
        --keyspace=20000 --warmup="$WARMUP" --out="$csv" >/dev/null
  line=$(bench/report.py "$csv" --json)
  tp=$(echo "$line" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["throughput_ops_per_s"])')
  p50=$(echo "$line" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["latency_ms"]["all"]["p50"])')
  p99=$(echo "$line" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["latency_ms"]["all"]["p99"])')
  p999=$(echo "$line" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["latency_ms"]["all"]["p999"])')
  printf "%-6s %-14s %-10s %-10s %-10s\n" "$C" "$tp" "$p50" "$p99" "$p999" | tee -a "$B1"
  cp "$csv" "$OUT/${STAMP}_bm1_c${C}.csv"
  stop_cluster
done

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " BENCHMARK 2 -- leader-failure recovery time ($RECOVERY_TRIALS trials)"
echo "=============================================================="
B2="$OUT/${STAMP}_bm2_recovery.txt"
{ hdr; echo; echo "trial  recovery_gap_ms"; } | tee "$B2"
gaps=""
for t in $(seq 1 "$RECOVERY_TRIALS"); do
  export RAFT_BASE_PORT=$((9500 + t))
  start_cluster "$N"; sleep 1
  L=$(wait_for_leader "$N" 20); [[ -z "$L" ]] && { stop_cluster; continue; }
  csv="$WORKDIR/bm2_t${t}.csv"
  "$LG" --endpoints="$(endpoints)" -c 24 -d $((WARMUP + 12)) -r 400 \
        --mix=put=100 --keyspace=5000 --warmup="$WARMUP" --out="$csv" >/dev/null &
  lgpid=$!
  sleep $((WARMUP + 4))
  L=$(find_leader "$N")
  [[ -n "$L" ]] && kill -9 "${NODE_PIDS[$((L-1))]}" 2>/dev/null
  wait "$lgpid"
  gap=$(bench/report.py "$csv" --json | python3 -c 'import sys,json;print(json.load(sys.stdin)["recovery_gap_ms"][0])')
  echo "$t      $gap" | tee -a "$B2"
  gaps="$gaps $gap"
  stop_cluster
done
echo "$gaps" | python3 -c '
import sys
xs=sorted(float(x) for x in sys.stdin.read().split())
if xs:
  n=len(xs)
  def p(q):
    k=(n-1)*q; import math; lo=int(k);
    return xs[lo] if lo+1>=n else xs[lo]+(xs[lo+1]-xs[lo])*(k-lo)
  print(f"recovery gap ms: n={n} median={p(0.5):.0f} p95={p(0.95):.0f} worst={xs[-1]:.0f} best={xs[0]:.0f}")
' | tee -a "$B2"

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " BENCHMARK 3 -- partition tolerance (minority 2 of 5)"
echo "=============================================================="
B3="$OUT/${STAMP}_bm3_partition.txt"
{ hdr; echo; } > "$B3"
export RAFT_BASE_PORT=9800
start_cluster "$N"; sleep 1
L=$(wait_for_leader "$N" 20)
base_csv="$WORKDIR/bm3_baseline.csv"
"$LG" --endpoints="$(endpoints)" -c 48 -d "$DUR" --mix=put=100 --keyspace=10000 --warmup="$WARMUP" --out="$base_csv" >/dev/null
base_tp=$(bench/report.py "$base_csv" --json | python3 -c 'import sys,json;print(json.load(sys.stdin)["throughput_ops_per_s"])')

# minority = two non-leader nodes
MIN=(); for i in $(seq 1 "$N"); do [[ "$i" != "$L" && ${#MIN[@]} -lt 2 ]] && MIN+=("$i"); done
bash bench/chaos/partition_minority.sh "$N" "${MIN[@]}"
sleep 2
# majority endpoints only
MEP=""; for i in $(seq 1 "$N"); do
  skip=0; for m in "${MIN[@]}"; do [[ "$i" == "$m" ]] && skip=1; done
  [[ "$skip" == 0 ]] && MEP="${MEP}${MEP:+,}${i}@127.0.0.1:$(client_port "$i")"
done
part_csv="$WORKDIR/bm3_partitioned.csv"
"$LG" --endpoints="$MEP" -c 48 -d "$DUR" --mix=put=100 --keyspace=10000 --warmup="$WARMUP" --out="$part_csv" >/dev/null
part_tp=$(bench/report.py "$part_csv" --json | python3 -c 'import sys,json;print(json.load(sys.stdin)["throughput_ops_per_s"])')

# minority must refuse writes (never a stale OK)
probe=$(printf 'PUT probe v 0 0\nQUIT\n' | nc -w 2 127.0.0.1 "$(client_port "${MIN[0]}")" 2>/dev/null | head -1)
bash bench/chaos/partition_minority.sh --heal "$N"
stop_cluster
{
  echo "baseline throughput (5 nodes healthy) : $base_tp ops/s"
  echo "majority throughput (2/5 partitioned) : $part_tp ops/s"
  echo "minority write probe reply            : '${probe:-<no reply>}'  (must NOT be OK)"
} | tee -a "$B3"

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " BENCHMARK 4 -- durability gate (50 kill+recover cycles)"
echo "=============================================================="
B4="$OUT/${STAMP}_bm4_durability.txt"
NODE_BIN="$NODE_BIN" COUNT="${DUR_COUNT:-300}" KILLS=10 CYCLES=5 bash scripts/p3_durability.sh 2>&1 | tee "$B4"

# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " BENCHMARK 5 -- read throughput + latency curve (ReadIndex)"
echo "=============================================================="
B5="$OUT/${STAMP}_bm5_read_curve.txt"
{ hdr; echo; printf "%-6s %-14s %-10s %-10s\n" conns "ops/s(ok)" p50_ms p99_ms; } | tee "$B5"
for C in $CURVE; do
  export RAFT_BASE_PORT=$((9200 + C))
  start_cluster "$N"; sleep 1; wait_for_leader "$N" 20 >/dev/null
  "$LG" --endpoints="$(endpoints)" -c 8 -d 6 --mix=put=100 --keyspace=5000 --warmup=1 --out="$WORKDIR/seed.csv" >/dev/null
  csv="$WORKDIR/bm5_c${C}.csv"
  "$LG" --endpoints="$(endpoints)" -c "$C" -d "$DUR" --mix=get=100 \
        --keyspace=5000 --warmup="$WARMUP" --out="$csv" >/dev/null
  j=$(bench/report.py "$csv" --json)
  tp=$(echo "$j" | python3 -c 'import sys,json;print(json.load(sys.stdin)["throughput_ops_per_s"])')
  p50=$(echo "$j" | python3 -c 'import sys,json;print(json.load(sys.stdin)["latency_ms"]["all"]["p50"])')
  p99=$(echo "$j" | python3 -c 'import sys,json;print(json.load(sys.stdin)["latency_ms"]["all"]["p99"])')
  printf "%-6s %-14s %-10s %-10s\n" "$C" "$tp" "$p50" "$p99" | tee -a "$B5"
  cp "$csv" "$OUT/${STAMP}_bm5_c${C}.csv"
  stop_cluster
done

echo
echo "all results in $OUT/${STAMP}_*"
