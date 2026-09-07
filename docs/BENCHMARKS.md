# RaftKV — Benchmarks & Chaos Testing (Phase 5)

All numbers below were produced by `bench/run_all.sh`, with the 5 nodes running
as **5 separate OS processes**, each with its own data directory and TCP ports.
Regenerate everything with:

```
cmake --preset release && cmake --build build/release -j
DUR=12 WARMUP=4 CURVE="10 50 100 200 400" RECOVERY_TRIALS=25 bench/run_all.sh
```

Raw per-request CSVs are written to `docs/RESULTS/<timestamp>_*.csv` (gitignored)
and the summary `.txt` files are kept.

The run recorded here: **`20260907_054420`, commit `72361f8`**, Darwin 24.6
arm64, 8 CPU.

## Setup

| | |
|---|---|
| Machine | Apple M-series, 8 cores, 8 GiB — macOS 15.6 arm64 |
| Build | `RelWithDebInfo`, Apple clang 17, `-std=c++20` |
| Cluster | 5 processes, `127.0.0.1`, distinct ports + data dirs |
| Raft timing | election timeout 300 ms (randomised to [300,600) ms via PreVote+vote), heartbeat 75 ms, logical tick 25 ms |
| Durability | every committed batch is `F_FULLFSYNC`'d on the leader **and** each acking follower before it counts toward commit (≈ 10 ms/call on this disk) |
| Transport | raw TCP, length-prefixed binary frames, `TCP_NODELAY`, one persistent connection per client |
| Load generator | `raftkv-loadgen`: N persistent connections, closed loop unless `-r`; per-request send/recv timestamps via `steady_clock`; first `--warmup` s excluded from the steady window |
| Clock | one machine ⇒ one clock; no cross-node clock-skew caveat applies |

### Trusting the harness

`bench/test_report.py` (run by `ctest -L phase5`) checks the reporting math on
synthetic inputs: percentile interpolation, warmup-window exclusion,
`sent == ok + fail` reconciliation, and recovery-gap detection against a known
340 ms hole (auto-detected and via explicit `--kill-ns`). Every request row has
a unique id; the reporter asserts the counts reconcile.

A note on magnitudes: this is a teaching implementation. It batches concurrent
proposals into one fsync + one replication round, but does **no** pipelining,
no log-structured storage engine, and no read leases — so absolute throughput
is far below etcd. The point is that every number is measured honestly and the
methodology reproduces, not that the number is large.

---

## Benchmark 1 — steady-state write throughput & latency curve

Healthy 5-node cluster, `--mix=put=100`, keyspace 20 000, closed loop, 12 s
steady window per point.

| clients | writes/sec (ok) | P50 ms | P99 ms | P99.9 ms |
|--------:|----------------:|-------:|-------:|---------:|
| 10  | 136   | 71.2  | 110.6 | 121.6 |
| 50  | 508   | 96.0  | 140.7 | 172.4 |
| 100 | 974   | 102.6 | 136.3 | 153.9 |
| 200 | 1 902 | 102.7 | 162.2 | 179.8 |
| 400 | 2 685 | 139.9 | 223.8 | 240.9 |

Throughput scales close to linearly to ~1.9 k writes/sec while P50 stays flat at
the commit-round floor (~100 ms: one leader fsync + one replication round trip +
one follower fsync + apply). Past **c ≈ 200** the single-writer commit path
starts to saturate — added concurrency buys throughput (2 685/s at c=400) at the
cost of latency (P50 103→140 ms, P99 162→224 ms). The knee is **c ≈ 200**.

---

## Benchmark 2 — leader-failure recovery time

Load generator runs continuously (open loop, 400 ops/sec, 24 connections). After
the warmup, `kill -9` the current leader. Recovery gap = (first successful
response after the kill) − (last successful response before it). **25 trials**,
fresh cluster each time.

| statistic | recovery gap |
|-----------|-------------:|
| best      | 434 ms |
| **median**| **520 ms** |
| P95       | 577 ms |
| worst     | 601 ms |

The gap is: follower election-timeout expiry (randomised 300–600 ms) + one
PreVote round + one RequestVote round + the new leader's first heartbeat + the
client's reconnect/redirect. It is tightly clustered (434–601 ms over 25 trials)
because the randomised timeout dominates and is bounded.

---

## Benchmark 3 — partition tolerance (minority of 2 / 5)

Baseline: loadgen (48 conns) against the healthy cluster. Then `NETBLOCK`
isolates 2 non-leader nodes both directions; loadgen runs against the 3-node
majority; a probe client sends a write to an isolated minority node.

| scenario | write throughput |
|----------|-----------------:|
| baseline, 5 nodes healthy        | 544 ops/sec |
| 2 of 5 partitioned, majority side | 566 ops/sec |
| isolated minority write probe     | **`RETRY`** (never `OK`) |

The majority side shows **no degradation** (566 vs 544 ops/sec is within
run-to-run noise) — quorum is still 3 of 5 and the leader was already only
waiting on a fast majority. The isolated minority **refuses** the write rather
than returning a stale `OK`: it cannot reach a quorum, so it can neither commit
nor safely serve.

---

## Benchmark 4 — durability gate (kill-and-recover cycles)

`scripts/p3_durability.sh`, 5 cycles × 10 `kill -9`+restart events, 200
acknowledged writes per cycle — **50 induced crashes**. After each cycle every
key acknowledged with `OK` before the kills is read back.

```
cycle 1: OK  (200 acked, all present after 10 kill/restarts)
cycle 2: OK  (200 acked, all present after 10 kill/restarts)
cycle 3: OK  (200 acked, all present after 10 kill/restarts)
cycle 4: OK  (200 acked, all present after 10 kill/restarts)
cycle 5: OK  (200 acked, all present after 10 kill/restarts)
PASS: zero committed write loss
```

A pass/fail correctness gate, not a performance number. Reinforced by the
deterministic fuzz: 500 seeds × {3,5} nodes × 2 000 steps of random
crash/restart/partition schedules, commit-durability invariant checked every
step, 0 violations.

---

## Benchmark 5 — read throughput & latency (ReadIndex)

Same curve as Benchmark 1 but `--mix=get=100`, keyspace pre-seeded. Reads go
through ReadIndex: the leader confirms it still holds a quorum via a round-tagged
heartbeat before serving.

| clients | reads/sec (ok) | P50 ms | P99 ms |
|--------:|---------------:|-------:|-------:|
| 10  | 148   | 65.0  | 102.9 |
| 50  | 564   | 87.3  | 120.2 |
| 100 | 974   | 102.6 | 141.9 |
| 200 | 1 758 | 112.5 | 174.9 |
| 400 | 2 493 | 162.5 | 227.8 |

Reads do not fsync, but they still pay one heartbeat-quorum round trip for
linearizability, so per-op latency and the shape of the curve track the write
path closely.

---

All numbers reproduce via `bench/run_all.sh`; results and per-run headers
(seed + commit + machine) are under `docs/RESULTS/`.
