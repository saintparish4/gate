#!/usr/bin/env python3
"""Summarize open-loop runs (loadSim --scenario openloop), stage by stage.

Reads one or more .jsonl files, one line per scheduled request, and prints per
stage: offered and achieved rate, the status mix, latency from the intended
send time (coordinated-omission free) and from the actual send, the generator's
send lag, the server's Server-Timing split, and which replicas answered.

    python3 scripts/perf/summarize.py run.jsonl [--json]

Standard library only. Percentiles are nearest-rank on the raw rows. Times are
as measured (monotonic); analyze.py rescales them by the run's clock ratio.
"""
import json
import math
import sys
from collections import Counter, defaultdict


def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    k = max(0, min(len(sorted_vals) - 1, math.ceil(q * len(sorted_vals)) - 1))
    return sorted_vals[k]


def summarize(rows):
    by_stage = defaultdict(list)
    for r in rows:
        by_stage[r["stage"]].append(r)
    out = []
    for stage in sorted(by_stage):
        rs = by_stage[stage]
        t0 = min(r["intended"] for r in rs)
        t1 = max(r["intended"] for r in rs)
        span = max(1e-9, (t1 - t0) / 1000.0)
        answered = [r for r in rs if r["status"] > 0]
        lat = sorted(r["latency"] for r in answered)
        svc = sorted(r["service"] for r in answered)
        lag = sorted(r["sent"] - r["intended"] for r in rs)
        store = sorted(r["store"] for r in answered if r.get("store") is not None)
        gate = sorted(r["gate"] for r in answered if r.get("gate") is not None)
        # Throughput is answers completed inside the stage's window, whichever
        # stage scheduled them; a stage's own requests finish late when the
        # server falls behind.
        t_end = t1 + 1000.0 / rs[0]["rate"]
        completed = sum(1 for r in rows if r["status"] > 0 and t0 <= r["done"] < t_end)
        done_span = None
        out.append({
            "stage": stage,
            "offered_rps": rs[0]["rate"],
            "requests": len(rs),
            "answered_rps": round(completed / max(1e-9, (t_end - t0) / 1000.0), 1),
            "status": dict(Counter(str(r["status"]) for r in rs)),
            "errors": dict(Counter(r["error"] for r in rs if r["error"])),
            "degraded": sum(1 for r in rs if r.get("degraded")),
            "latency_ms": {q: pct(lat, p) for q, p in
                           [("p50", .5), ("p90", .9), ("p99", .99), ("p999", .999), ("max", 1.0)]},
            "service_ms": {q: pct(svc, p) for q, p in [("p50", .5), ("p99", .99)]},
            "send_lag_ms": {q: pct(lag, p) for q, p in [("p50", .5), ("p99", .99), ("max", 1.0)]},
            "store_ms_p50": pct(store, .5), "store_ms_p99": pct(store, .99),
            "gate_ms_p50": pct(gate, .5), "gate_ms_p99": pct(gate, .99),
            "instances": dict(Counter(r["instance"] for r in answered if r.get("instance"))),
        })
    return out


def load(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


if __name__ == "__main__":
    paths = [a for a in sys.argv[1:] if not a.startswith("--")]
    rows = [r for p in paths for r in load(p)]
    res = summarize(rows)
    if "--json" in sys.argv:
        print(json.dumps(res, indent=1))
        sys.exit(0)
    def f(v):
        return "-" if v is None else f"{v:.1f}"
    print(f"{'stg':>3} {'offer':>6} {'ans/s':>6} {'p50':>7} {'p90':>7} {'p99':>7} {'p99.9':>8} {'max':>8} {'lag99':>7} {'store50':>7} {'gate50':>7}  status / errors")
    for s in res:
        L = s["latency_ms"]
        print(f"{s['stage']:>3} {s['offered_rps']:>6.0f} {s['answered_rps']:>6.0f} {f(L['p50']):>7} {f(L['p90']):>7} {f(L['p99']):>7} {f(L['p999']):>8} {f(L['max']):>8} {f(s['send_lag_ms']['p99']):>7} {f(s['store_ms_p50']):>7} {f(s['gate_ms_p50']):>7}  {s['status']} {s['errors'] or ''} deg={s['degraded']}")
