#!/usr/bin/env bash
# Phase gate runner: build every sanitizer variant, run the full ctest on each,
# run the fuzz target (if built) for a fixed budget, and run the process-level
# smoke scripts for the phases completed so far.
#
# Usage:  scripts/check_all.sh [max_phase]
#   max_phase defaults to 5; pass e.g. 1 while only Phase 1 is done.
#
# Notes:
#  - LeakSanitizer is NOT supported on macOS/arm64, so ASan runs without
#    detect_leaks. ASan still catches use-after-free / OOB / etc.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MAXP="${1:-5}"
rc=0

run() { echo; echo "### $*"; "$@" || { echo "!!! FAILED: $*"; rc=1; }; }

for preset in debug asan ubsan tsan; do
  # tsan only matters once threads exist (Phase 2+)
  [[ "$preset" == "tsan" && "$MAXP" -lt 2 ]] && continue
  echo "==================== $preset ===================="
  cmake --preset "$preset" >/dev/null || { rc=1; continue; }
  cmake --build "build/$preset" -j8 >/dev/null || { rc=1; continue; }
  labels="phase1"
  for p in $(seq 2 "$MAXP"); do labels="$labels|phase$p"; done
  ( cd "build/$preset" && ctest -L "$labels" --output-on-failure -j8 ) \
    || { echo "!!! ctest failed under $preset"; rc=1; }
done

# ---- fuzz (Phase 3+) ----
if [[ "$MAXP" -ge 3 && -x build/debug/raftkv-fuzz ]]; then
  run build/debug/raftkv-fuzz --seeds=500 --steps=2000
fi

# ---- process smoke scripts ----
# N=2000 keeps the gate under a few minutes; F_FULLFSYNC is ~10ms/write on this
# box. A heavier N=5000/CYCLES=10 run is done separately and recorded in NOTES.
[[ "$MAXP" -ge 1 ]] && run env BIN=build/debug/raftkv-store N=2000 CYCLES=10 bash scripts/p1_durability.sh
[[ "$MAXP" -ge 2 && -f scripts/p2_elect.sh ]]     && run bash scripts/p2_elect.sh
[[ "$MAXP" -ge 2 && -f scripts/p2_partition.sh ]] && run bash scripts/p2_partition.sh
[[ "$MAXP" -ge 3 && -f scripts/p3_durability.sh ]] && run bash scripts/p3_durability.sh
[[ "$MAXP" -ge 4 && -f scripts/p4_client.sh ]]    && run bash scripts/p4_client.sh

echo
[[ "$rc" -eq 0 ]] && echo "ALL CHECKS PASSED (phases 1..$MAXP)" || echo "SOME CHECKS FAILED"
exit "$rc"
