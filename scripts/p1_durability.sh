#!/usr/bin/env bash
# Phase 1 process-level durability gate.
#
# Launch raftkv-store, stream N puts through a FIFO (so the process never sees
# stdin EOF), wait until all N writes are acknowledged, then `kill -9` it with
# no graceful shutdown. Relaunch and verify every acknowledged key reads back
# with the correct value. Repeat CYCLES times. Any missing/incorrect key => FAIL.
#
# Works on bash 3.2 (stock macOS) -- no `coproc`, no process substitution tricks.
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

  "$BIN" --wal="$WAL" < "$FIFO" > "$OUT" 2>/dev/null &
  spid=$!
  exec 3> "$FIFO"          # hold the write end open -> no EOF for the store

  for i in $(seq 1 "$N"); do
    printf 'put key_%d val_%d_%d\n' "$i" "$c" "$i" >&3
  done

  # Wait for N acknowledgements to appear in OUT.
  waited=0
  while : ; do
    acked=$(grep -c '^OK' "$OUT" 2>/dev/null || echo 0)
    [[ "$acked" -ge "$N" ]] && break
    waited=$((waited+1))
    if [[ "$waited" -gt 600 ]]; then
      echo "cycle $c: timeout waiting for acks ($acked/$N)"; fail=1; break
    fi
    sleep 0.1
  done

  kill -9 "$spid" 2>/dev/null
  wait "$spid" 2>/dev/null
  exec 3>&-                # close write end

  # --- recovery: reopen, read every key back ---
  miss=0
  recov="$WORK/recov.txt"
  {
    for i in $(seq 1 "$N"); do printf 'get key_%d\n' "$i"; done
    echo exit
  } | "$BIN" --wal="$WAL" 2>/dev/null > "$recov"

  i=0
  while IFS= read -r line; do
    i=$((i+1))
    [[ "$line" == "VALUE val_${c}_$i" ]] || miss=$((miss+1))
  done < "$recov"
  [[ "$i" -eq "$N" ]] || { echo "cycle $c: only $i/$N get replies"; miss=$((miss + N - i)); }

  if [[ "$miss" -eq 0 ]]; then
    echo "cycle $c: OK   ($N/$N keys recovered after kill -9)"
  else
    echo "cycle $c: FAIL ($miss keys missing/wrong)"
    fail=1
  fi
done

if [[ "$fail" -eq 0 ]]; then
  echo "PASS: $CYCLES/$CYCLES cycles, zero committed write loss"
  exit 0
fi
echo "FAIL"
exit 1
