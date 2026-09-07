#!/usr/bin/env python3
# Self-tests for report.py: percentile math, steady-window exclusion, recovery
# gap detection, and the sent == ok + fail reconciliation. Run by ctest.
import os, subprocess, sys, tempfile, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "report.py")

def run(csv_text, extra=()):
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
        f.write(csv_text)
        path = f.name
    out = subprocess.check_output([sys.executable, REPORT, path, "--json", *extra])
    os.unlink(path)
    return json.loads(out)

def mk(rows, t0=0, warmup_s=0):
    head = f"# t0_ns={t0} warmup_s={warmup_s} duration_s=1 conns=1 rate=0 keyspace=1 put_pct=100 seed=1\n"
    head += "id,worker,op,t_send_ns,t_recv_ns,status\n"
    body = "".join(f"{i},0,{op},{ts},{tr},{st}\n" for i,(ts,tr,op,st) in enumerate(rows,1))
    return head + body

fails = 0
def check(cond, msg):
    global fails
    print(("ok   " if cond else "FAIL ") + msg)
    if not cond: fails += 1

MS = 1_000_000

# --- percentiles: 100 ok ops with latency 1..100 ms ---
rows = [(i*MS, i*MS + i*MS, "P", "ok") for i in range(1, 101)]
r = run(mk(rows))
check(abs(r["latency_ms"]["all"]["p50"] - 50.5) < 1.0, f"p50 ~= 50.5 (got {r['latency_ms']['all']['p50']})")
check(abs(r["latency_ms"]["all"]["p99"] - 99.0) < 1.5, f"p99 ~= 99 (got {r['latency_ms']['all']['p99']})")
check(r["ops_ok"] == 100 and r["ops_fail"] == 0, "sent == ok + fail (100/0)")

# --- availability with failures ---
rows = [(i*MS, i*MS+MS, "P", "ok" if i % 10 else "fail") for i in range(1, 101)]
r = run(mk(rows))
check(r["ops_ok"] == 90 and r["ops_fail"] == 10, "90 ok / 10 fail counted")
check(abs(r["availability_pct"] - 90.0) < 0.01, f"availability 90% (got {r['availability_pct']})")

# --- steady window excludes warmup ---
# warmup 5s: ops with t_send < 5e9 are dropped
rows = [(t, t + MS, "P", "ok") for t in (1_000_000_000, 4_999_000_000, 6_000_000_000, 9_000_000_000)]
r = run(mk(rows, t0=0, warmup_s=5))
check(r["ops_ok"] == 2, f"2 of 4 ops inside the steady window (got {r['ops_ok']})")

# --- recovery gap: a 340 ms hole between consecutive successes ---
base = 6_000_000_000
seq = []
t = base
for _ in range(50):
    seq.append((t, t + MS, "P", "ok")); t += 5 * MS          # 5 ms apart
gap_start = t
t += 340 * MS                                                 # <-- the outage
for _ in range(50):
    seq.append((t, t + MS, "P", "ok")); t += 5 * MS
r = run(mk(seq, warmup_s=0))
g = r["recovery_gap_ms"][0]
check(330 <= g <= 360, f"auto-detected recovery gap ~= 340 ms (got {g})")

# explicit --kill-ns near the hole gives the same answer
r2 = run(mk(seq, warmup_s=0), extra=("--kill-ns", str(gap_start + MS)))
check(330 <= r2["recovery_gap_ms"][0] <= 360, f"--kill-ns gap ~= 340 ms (got {r2['recovery_gap_ms'][0]})")

sys.exit(1 if fails else 0)
