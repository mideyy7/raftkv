# RaftKV

A replicated, fault-tolerant key-value store built on the [Raft](https://raft.github.io/raft.pdf)
consensus algorithm. A cluster of 3–5 nodes behaves as one linearizable KV store
and keeps serving as long as a majority is alive, even when individual nodes
crash, restart, or lose network connectivity.

Written in C++20. Networking is raw TCP with a hand-rolled length-prefixed
binary protocol — no gRPC, no protobuf, no third-party libraries. Building
needs only a C++20 compiler and CMake.

## What it does

- **Leader election** with randomized timeouts and PreVote (a partitioned node
  cannot disrupt a healthy leader when it rejoins).
- **Log replication** with `AppendEntries`, fast conflict backup, and
  majority-commit that respects the current-term safety rule (Raft §5.4.2 /
  "Figure 8").
- **Durability**: `currentTerm`, `votedFor`, and every log entry are written to
  a write-ahead log and `fsync`'d before anything is acknowledged. A torn
  trailing record from a crash mid-write is detected by CRC and truncated on
  replay.
- **Linearizable client API**: clients discover the leader (followers redirect),
  retry across elections with capped exponential backoff + jitter, and every
  write carries a `(client_id, seq)` so a retried write is applied **exactly
  once**. Reads use ReadIndex — the leader confirms it still holds a quorum via
  a round-tagged heartbeat before serving, so a partitioned ex-leader cannot
  return stale data.

### Non-goals

Log compaction / snapshotting, live membership changes, pipelined replication,
and a log-structured storage engine are out of scope. This is a correctness- and
clarity-focused implementation, not a production datastore.

## Architecture

```
             ┌──────────────────────────────────────────────┐
 tick()      │                  RaftCore                     │
 step(msg)   │   pure, single-threaded, no clock, no I/O     │
 propose()   │   follower / candidate / leader FSM           │
 request_read│   log[], currentTerm, votedFor, commitIndex   │
─────────────▶                                               │
             └───────────────────────┬──────────────────────┘
                                     │ ready()  → { hard state to persist,
                                     │            entries to persist,
                                     │            messages to send,
                                     │            committed entries to apply }
                                     ▼
            RaftNode driver: 1 raft thread owns the core under a mutex,
            persists (WAL + fsync) → sends (TcpTransport) → applies (KvTable),
            then advance(). Client calls take the same mutex.
```

`RaftCore` reads no clock and touches no socket — all timing non-determinism
enters as a seed. That makes the consensus logic fully deterministic and lets
the whole cluster run in one process over an in-memory network for testing.
`TcpTransport` (real deployment) and `InProcTransport` (threaded tests) are
interchangeable behind one `Transport` interface.

## Build

```bash
cmake --preset debug      # or: asan / ubsan / tsan / release
cmake --build build/debug -j
```

Presets differ only in sanitizers / optimization. `release` is
`RelWithDebInfo`.

## Run a cluster

```bash
# start 5 node processes (distinct ports + data dirs), print client endpoints
scripts/run_cluster.sh          # Ctrl-C to stop

# from another shell:
build/debug/raftkv-cli --endpoints=1@127.0.0.1:9002,2@127.0.0.1:9004,3@127.0.0.1:9006,4@127.0.0.1:9008,5@127.0.0.1:9010 put foo bar
build/debug/raftkv-cli --endpoints=... get foo
build/debug/raftkv-cli --endpoints=... bench 1000
```

A node speaks two protocols: the binary Raft protocol between peers, and a
line protocol on `listen_port + 1` for clients
(`STATUS`, `PUT k v [cid seq]`, `GET k`, `DEL k [cid seq]`, `NETBLOCK <id>`,
`NETUNBLOCK <id>`).

`raftkv-store` is a standalone single-node version of just the durable store
(the Phase 1 building block).

## Testing

```bash
ctest --test-dir build/debug --output-on-failure        # unit + integration
ctest --test-dir build/tsan  -L "phase2|phase3|phase4"  # threaded paths under TSan
build/debug/raftkv-fuzz --seeds=500 --steps=2000        # randomized model checker
scripts/check_all.sh 5                                  # full gate: all sanitizers + fuzz + process scripts
```

- **Deterministic tests** drive `RaftCore` directly over a thread-free
  `SimCluster` that can partition / drop / reorder messages with a seeded RNG —
  zero timing flakiness.
- **`raftkv-fuzz`** runs random schedules of tick / deliver-k / crash / restart /
  partition / heal / propose against a `std::map` oracle and checks five safety
  invariants (election safety, log matching, state-machine safety, commit
  durability, commit bounds) plus KV agreement on the committed prefix **after
  every step**. A failing seed prints a replayable schedule.
- **Threaded tests** run real `RaftNode` instances over `InProcTransport` so the
  TSan build exercises the actual driver.
- **Process-level scripts** (`scripts/p1..p4`, `p3_durability`) launch real node
  processes and `kill -9` them under load, verifying every acknowledged write
  survives.
- A brute-force **linearizability checker** (`tests/lin/`) validates recorded
  client histories from fault-injected runs.

The last verified gate: all tests green under `debug` / `asan` / `ubsan` /
`tsan`; fuzz clean at 500 seeds × {3,5} nodes × 2000 steps; all process
scripts pass with zero committed write loss.

## Benchmarks

`bench/run_all.sh` regenerates every number into `docs/RESULTS/`. See
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for methodology and full tables.
Representative run (5 node processes on one 8-core machine, `F_FULLFSYNC` per
commit, 300 ms election timeout):

| metric | result |
|---|---|
| write throughput @ 200 clients | ~1,900 ops/sec, P50 103 ms, P99 162 ms |
| read throughput @ 200 clients (ReadIndex) | ~1,760 ops/sec |
| leader-failure recovery (25 `kill -9` trials) | median 520 ms, P95 577 ms, worst 601 ms |
| minority (2/5) partition | majority throughput unaffected; minority refuses writes |
| durability: 50 kill-and-recover cycles | zero committed write loss |

## Repository layout

```
src/
  common/     codec, crc32c, wire protocol, config, logging
  storage/    WAL, log store, KV table, session cache
  raft/       raft_core (the FSM), types, safety invariants
  transport/  Transport interface, TCP, in-process
  node/       RaftNode threaded driver
  client/     RaftKvClient
  server/     raftkv-node, raftkv-store, raftkv-cli entry points
bench/        loadgen, report.py, chaos scripts, run_all.sh
tests/        unit/ integration/ fuzz/ lin/ + harnesses
scripts/      run_cluster.sh, p1..p4 gates, check_all.sh
docs/         BENCHMARKS.md, RESULTS/
```

## License

MIT
