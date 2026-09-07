#!/usr/bin/env python3
"""Summarise a raftkv-loadgen CSV: throughput, latency percentiles, the
leader-failure recovery gap, and availability.

  bench/report.py run.csv [--kill-ns N ...] [--json]

Latency = t_recv - t_send (nanoseconds) over the *steady window* (after the
warmup recorded in the file header). Recovery gap: if --kill-ns instants are
given, the gap for each is (first ok t_recv after it) - (last ok t_recv before
it); otherwise the single largest gap between consecutive successful t_recv is
reported as a heuristic.
"""
import sys, json

def pct(xs, p):
    if not xs: return 0
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    lo = int(k)
    if lo + 1 < len(xs):
        return xs[lo] + (xs[lo+1] - xs[lo]) * (k - lo)
    return xs[lo]

def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__); sys.exit(2)
    path = args[0]
    kills = [int(a) for i, a in enumerate(args) if args[i-1] == "--kill-ns"] if "--kill-ns" in args else []
    as_json = "--json" in args

    hdr = {}
    rows = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line: continue
            if line.startswith("#"):
                for tok in line[1:].split():
                    if "=" in tok:
                        k, v = tok.split("=", 1)
                        hdr[k] = v
                continue
            if line.startswith("id,"): continue
            _id, worker, op, ts, tr, st = line.split(",")
            rows.append((int(ts), int(tr), op, st == "ok"))

    if not rows:
        print("no rows"); sys.exit(1)

    t0 = int(hdr.get("t0_ns", min(r[0] for r in rows)))
    warmup_ns = int(hdr.get("warmup_s", "5")) * 1_000_000_000
    steady_start = t0 + warmup_ns
    steady_end = max(r[1] for r in rows)

    steady = [r for r in rows if r[0] >= steady_start]
    ok = [r for r in steady if r[3]]
    fail = [r for r in steady if not r[3]]

    span_s = (steady_end - steady_start) / 1e9 if steady_end > steady_start else 1e-9
    lat_all = [(tr - ts) / 1e6 for (ts, tr, op, o) in ok]           # ms
    lat_put = [(tr - ts) / 1e6 for (ts, tr, op, o) in ok if op == "P"]
    lat_get = [(tr - ts) / 1e6 for (ts, tr, op, o) in ok if op == "G"]

    # recovery gap(s)
    ok_recv = sorted(tr for (_ts, tr, _op, _o) in ok)
    gaps = []
    if kills:
        for k in kills:
            before = [t for t in ok_recv if t <= k]
            after = [t for t in ok_recv if t > k]
            if before and after:
                gaps.append((after[0] - before[-1]) / 1e6)  # ms
    else:
        big = 0
        for a, b in zip(ok_recv, ok_recv[1:]):
            big = max(big, b - a)
        gaps.append(big / 1e6)

    out = {
        "file": path,
        "config": hdr,
        "steady_window_s": round(span_s, 2),
        "ops_ok": len(ok),
        "ops_fail": len(fail),
        "throughput_ops_per_s": round(len(ok) / span_s, 1),
        "availability_pct": round(100.0 * len(ok) / max(1, len(ok) + len(fail)), 3),
        "latency_ms": {
            "all": {"p50": round(pct(lat_all, 50), 2), "p90": round(pct(lat_all, 90), 2),
                    "p99": round(pct(lat_all, 99), 2), "p999": round(pct(lat_all, 99.9), 2),
                    "max": round(max(lat_all), 2) if lat_all else 0},
            "put": {"p50": round(pct(lat_put, 50), 2), "p99": round(pct(lat_put, 99), 2), "n": len(lat_put)},
            "get": {"p50": round(pct(lat_get, 50), 2), "p99": round(pct(lat_get, 99), 2), "n": len(lat_get)},
        },
        "recovery_gap_ms": [round(g, 1) for g in gaps],
    }

    if as_json:
        print(json.dumps(out, indent=2))
        return
    print(f"file                : {path}")
    print(f"config              : {hdr}")
    print(f"steady window       : {out['steady_window_s']} s")
    print(f"throughput (ok)     : {out['throughput_ops_per_s']} ops/s   "
          f"({out['ops_ok']} ok, {out['ops_fail']} fail)")
    print(f"availability        : {out['availability_pct']} %")
    L = out["latency_ms"]["all"]
    print(f"latency ms (all)    : p50={L['p50']}  p90={L['p90']}  p99={L['p99']}  p99.9={L['p999']}  max={L['max']}")
    P, G = out["latency_ms"]["put"], out["latency_ms"]["get"]
    if P["n"]: print(f"latency ms (put)   : p50={P['p50']}  p99={P['p99']}  (n={P['n']})")
    if G["n"]: print(f"latency ms (get)   : p50={G['p50']}  p99={G['p99']}  (n={G['n']})")
    label = "recovery gap (kill) " if kills else "largest ok gap      "
    print(f"{label}: {out['recovery_gap_ms']} ms")

if __name__ == "__main__":
    main()
