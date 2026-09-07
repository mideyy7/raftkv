#!/usr/bin/env bash
# Shared helpers for the process-level cluster scripts. bash 3.2 compatible.

RAFT_BASE_PORT="${RAFT_BASE_PORT:-7001}"     # node i listens on RAFT_BASE_PORT + (i-1)*2
NODE_BIN="${NODE_BIN:-build/debug/raftkv-node}"
CLI_BIN="${CLI_BIN:-build/debug/raftkv-cli}"

raft_port() { echo $(( RAFT_BASE_PORT + ($1 - 1) * 2 )); }
client_port() { echo $(( RAFT_BASE_PORT + ($1 - 1) * 2 + 1 )); }

peers_spec() {  # peers_spec N   ->  "1@127.0.0.1:7001,2@127.0.0.1:7003,..."
  local n="$1" out="" i p
  for i in $(seq 1 "$n"); do
    p=$(raft_port "$i")
    out="${out}${out:+,}${i}@127.0.0.1:${p}"
  done
  echo "$out"
}

# send one line to a node's client port, print the single-line reply
ask() {  # ask <nodeId> <line...>
  local id="$1"; shift
  local port; port=$(client_port "$id")
  printf '%s\nQUIT\n' "$*" | nc -w 2 127.0.0.1 "$port" 2>/dev/null | head -1
}

# parse "role=leader term=3 leader=2 ..." -> echo the value of a field
field() { echo "$1" | tr ' ' '\n' | grep "^$2=" | head -1 | cut -d= -f2; }

# start N nodes; sets NODE_PIDS array and WORKDIR
start_cluster() {  # start_cluster N [extra node args...]
  local n="$1"; shift || true
  WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/raftkv.XXXXXX")"
  local peers; peers=$(peers_spec "$n")
  NODE_PIDS=()
  local i rp
  for i in $(seq 1 "$n"); do
    rp=$(raft_port "$i")
    mkdir -p "$WORKDIR/n$i"
    "$NODE_BIN" --id="$i" --listen_host=127.0.0.1 --listen_port="$rp" \
      --peers="$peers" --data_dir="$WORKDIR/n$i" \
      --election_timeout_ms=300 --heartbeat_ms=75 --tick_ms=25 "$@" \
      >"$WORKDIR/n$i.log" 2>&1 &
    NODE_PIDS+=("$!")
  done
}

stop_cluster() {
  local pid
  for pid in "${NODE_PIDS[@]:-}"; do kill "$pid" 2>/dev/null; done
  for pid in "${NODE_PIDS[@]:-}"; do wait "$pid" 2>/dev/null; done
  [[ -n "${WORKDIR:-}" ]] && rm -rf "$WORKDIR"
}

# PUT via the current leader, following REDIRECT and retrying RETRY.
# put_kv N key value [cid seq]  -> echoes "OK ..." on success, "" on give-up
put_kv() {
  local n="$1" key="$2" val="$3" cid="${4:-0}" seq="${5:-0}"
  local try leader resp
  leader=$(find_leader "$n")
  for try in $(seq 1 40); do
    [[ -z "$leader" ]] && leader=$(find_leader "$n")
    if [[ -n "$leader" ]]; then
      resp=$(ask "$leader" "PUT $key $val $cid $seq")
      case "$resp" in
        OK*) echo "$resp"; return 0 ;;
        REDIRECT*) leader=$(echo "$resp" | awk '{print $2}') ;;
        *) leader="" ;;
      esac
    fi
    sleep 0.1
  done
  echo ""
  return 1
}

# GET via the current leader, following REDIRECT / retrying RETRY.
get_kv() {
  local n="$1" key="$2" try leader resp
  leader=$(find_leader "$n")
  for try in $(seq 1 40); do
    [[ -z "$leader" ]] && leader=$(find_leader "$n")
    if [[ -n "$leader" ]]; then
      resp=$(ask "$leader" "GET $key")
      case "$resp" in
        VALUE*|NIL) echo "$resp"; return 0 ;;
        REDIRECT*) leader=$(echo "$resp" | awk '{print $2}') ;;
        *) leader="" ;;
      esac
    fi
    sleep 0.1
  done
  echo ""
  return 1
}

# find the current leader id among nodes 1..N (empty if none / split)
find_leader() {  # find_leader N
  local n="$1" i s role term
  local best_id="" best_term=-1 count=0
  for i in $(seq 1 "$n"); do
    s=$(ask "$i" STATUS)
    [[ -z "$s" ]] && continue
    role=$(field "$s" role); term=$(field "$s" term)
    if [[ "$role" == "leader" ]]; then
      count=$((count+1))
      if [[ "${term:-0}" -gt "$best_term" ]]; then best_term="$term"; best_id="$i"; fi
    fi
  done
  # only report if exactly one leader at the highest term
  echo "$best_id"
}

wait_for_leader() {  # wait_for_leader N [timeout_s]
  local n="$1" to="${2:-15}" i l
  for i in $(seq 1 $(( to * 5 ))); do
    l=$(find_leader "$n")
    [[ -n "$l" ]] && { echo "$l"; return 0; }
    sleep 0.2
  done
  return 1
}
