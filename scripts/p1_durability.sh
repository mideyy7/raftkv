#!/usr/bin/env bash
# Phase 1 process-level durability gate.
#
# Per cycle: launch raftkv-store, stream N puts through a FIFO (so the process
# never sees stdin EOF), wait until all N writes are acknowledged, then `kill -9`
# it with no graceful shutdown. Relaunch and verify every acknowledged key reads
# back with the correct value (compared with `diff` against an expected file).
# Repeat CYCLES times. Any missing/incorrect key => FAIL.
#
# bash 3.2 compatible (stock macOS): no `coproc`, no bashisms beyond FIFOs.
set -u

BIN="${BIN:-build/debug/raftkv-store}"
N="${N:-5000}"
CYCLES="${CYCLES:-10}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/p1dur.XXXXXX")"
WAL="$WORK/store.wal"
FIFO="$WORK/in.fifo"
OUT="$WORK/out.txt"
trap 'rm -rf "$WORK"' EXIT

if [[ ! -x "$BIN" ]]; then echo "missing $BIN (build first)"; exit 2; fi

fail=0
for c in $(seq 1 "$CYCLES"); do
  rm -f "$FIFO" "$OUT"; mkfifo "$FIFO"; : > "$OUT"

  # Pre-generate the put stream and the expected recovery output in one shot.
  awk -v n="$N" -v c="$c" 'BEGIN{for(i=1;i<=n;i++) printf "put key_%d val_%d_%d\n", i, c, i}' > "$WORK/puts.txt"
  awk -v n="$N" -v c="$c" 'BEGIN{for(i=1;i<=n;i++) printf "VALUE val_%d_%d\n", c, i}' > "$WORK/expect.txt"
  awk -v n="$N"           'BEGIN{for(i=1;i<=n;i++) printf "get key_%d\n", i; print "exit"}' > "$WORK/gets.txt"

  "$BIN" --wal="$WAL" < "$FIFO" > "$OUT" 2>/dev/null &
  spid=$!
  exec 3> "$FIFO"                 # hold the write end open -> no EOF
  cat "$WORK/puts.txt" >&3        # single bulk write

  waited=0
  while : ; do
    acked=$(grep -c '^OK' "$OUT" 2>/dev/null || echo 0)
    [[ "$acked" -ge "$N" ]] && break
    waited=$((waited+1))
    if [[ "$waited" -gt 1200 ]]; then
      echo "cycle $c: timeout waiting for acks ($acked/$N)"; fail=1; break
    fi
    sleep 0.1
  done

  kill -9 "$spid" 2>/dev/null
  wait "$spid" 2>/dev/null
  exec 3>&-

  # --- recovery: reopen, read every key back, diff against expected ---
  "$BIN" --wal="$WAL" < "$WORK/gets.txt" 2>/dev/null | grep '^VALUE\|^NIL' > "$WORK/recov.txt"
  if diff -q "$WORK/expect.txt" "$WORK/recov.txt" >/dev/null; then
    echo "cycle $c: OK   ($N/$N keys recovered after kill -9)"
  else
    nbad=$(diff "$WORK/expect.txt" "$WORK/recov.txt" | grep -c '^<')
    echo "cycle $c: FAIL ($nbad keys missing/wrong)"
    fail=1
  fi
done

if [[ "$fail" -eq 0 ]]; then
  echo "PASS: $CYCLES/$CYCLES cycles, zero committed write loss"
  exit 0
fi
echo "FAIL"
exit 1
