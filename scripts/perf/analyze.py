#!/usr/bin/env python3
"""Analyze the overload study's runs (docs/perf/2026-10-20-localstack-overload).

    analyze.py perf-results/2026-10-20 docs/perf/2026-10-20-localstack-overload/data

Reads every run directory under <root>/phase1 and <root>/phase2 and writes, to
the output directory: per-run stage tables (stages.jsonl), per-condition
percentile curves with bootstrap 95% intervals over runs (curves.json and one
SVG per metric), saturation points (saturation.json), phase 2 timelines,
failure order, recovery times and alerts (phase2.json), and a Little's Law
check (littles_law.json).

Every number is computed from the raw rows. Durations and rates are rescaled
by each run's own wall/monotonic ratio (plan.md, "The clock"). The
definitions (saturation point, recovery time, Little's Law) are the plan's,
word for word in code. Standard library only; deterministic for a seed.
"""
import json
import math
import random
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

SEED = 20261020
BOOT = 2000


# --------------------------------------------------------------------- loading

def load_jsonl(p):
    with open(p) as f:
        return [json.loads(l) for l in f if l.strip()]


def load_run(d: Path):
    man = json.loads((d / "manifest.json").read_text())
    meta = json.loads((d / "requests.jsonl.meta.json").read_text())
    factor = meta["wall_elapsed_s"] / meta["elapsed_s"]
    rows = load_jsonl(d / "requests.jsonl")
    for r in rows:
        for k in ("intended", "sent", "done", "latency", "service"):
            r[k] = r[k] * factor
        for k in ("store", "gate"):
            if r.get(k) is not None:
                r[k] = r[k] * factor
        r["failed"] = r["status"] == 0 or r["status"] >= 500 or bool(r.get("degraded"))
    cg = load_jsonl(d / "cgroups.jsonl") if (d / "cgroups.jsonl").exists() else []
    prom = json.loads((d / "prometheus.json").read_text()) if (d / "prometheus.json").exists() else {}
    corr = None
    for p in d.glob("correctness-*.json"):
        corr = json.loads(p.read_text()).get("overall")
    usage = None
    if (d / "usage-check.txt").exists():
        lines = [l for l in (d / "usage-check.txt").read_text().splitlines() if l.strip()]
        usage = lines[-1] if lines else None
        dropped = next((l for l in lines if l.startswith("dropped before Kinesis")), None)
        usage_dropped = int(dropped.split()[-1]) if dropped else None
    else:
        usage_dropped = None
    return {"dir": d, "manifest": man, "meta": meta, "factor": factor, "rows": rows,
            "cgroups": cg, "prom": prom, "correctness": corr, "usage": usage,
            "usage_dropped": usage_dropped, "wall0": meta["wall_start_ms"] / 1000.0}


# ------------------------------------------------------------------ statistics

def pct(vals, q):
    if not vals:
        return None
    s = sorted(vals)
    k = max(0, min(len(s) - 1, math.ceil(q * len(s)) - 1))
    return s[k]


def boot_ci(groups, stat, n=BOOT, seed=SEED):
    """Bootstrap over runs: resample whole runs, pool their values, apply stat."""
    rnd = random.Random(seed)
    groups = [g for g in groups if g]
    if not groups:
        return (None, None)
    vals = []
    for _ in range(n):
        pool = []
        for _ in groups:
            pool += groups[rnd.randrange(len(groups))]
        v = stat(pool)
        if v is not None:
            vals.append(v)
    vals.sort()
    return (vals[int(0.025 * len(vals))], vals[int(0.975 * len(vals)) - 1])


def boot_median(xs, n=BOOT, seed=SEED):
    rnd = random.Random(seed)
    xs = [x for x in xs if x is not None]
    if not xs:
        return (None, None)
    meds = sorted(statistics.median(rnd.choice(xs) for _ in xs) for _ in range(n))
    return (meds[int(0.025 * n)], meds[int(0.975 * n) - 1])


# ---------------------------------------------------------------------- stages

def stage_spans(run):
    """Real-time span (ms from start) of each stage, from the schedule."""
    spans = {}
    for r in run["rows"]:
        lo, hi = spans.get(r["stage"], (math.inf, -math.inf))
        spans[r["stage"]] = (min(lo, r["intended"]), max(hi, r["intended"]))
    # A stage ends where the next begins.
    keys = sorted(spans)
    out = {}
    for i, k in enumerate(keys):
        start = spans[k][0]
        end = spans[keys[i + 1]][0] if i + 1 < len(keys) else spans[k][1] + 1000.0 / run["rows"][0]["rate"]
        out[k] = (start, end)
    return out


def cg_window(run, names_like, t0, t1):
    """CPU-ms used and throttling over [t0, t1] (wall seconds) by matching containers."""
    by = defaultdict(list)
    for s in run["cgroups"]:
        if any(n in s["name"] for n in names_like):
            by[s["name"]].append(s)
    used = 0.0
    periods = throttled = 0
    for name, ss in by.items():
        inside = [s for s in ss if t0 <= s["t"] <= t1]
        if len(inside) < 2:
            continue
        a, b = inside[0], inside[-1]
        span = b["t"] - a["t"]
        if span <= 0:
            continue
        # Scale the sampled span to the full window.
        scale = (t1 - t0) / span
        used += (b["usage_usec"] - a["usage_usec"]) / 1000.0 * scale
        periods += b["nr_periods"] - a["nr_periods"]
        throttled += b["nr_throttled"] - a["nr_throttled"]
    return used, periods, throttled


def prom_mean(run, key, t0, t1, label_filter=None):
    series = run["prom"].get(key)
    if not isinstance(series, list):
        return None
    # Sum across series at each timestamp, then average over the window.
    at = defaultdict(float)
    for s in series:
        if label_filter and not label_filter(s["metric"]):
            continue
        for ts, v in s["values"]:
            if t0 <= ts <= t1 and v not in ("NaN", "+Inf"):
                at[ts] += float(v)
    return statistics.mean(at.values()) if at else None


def stages_of(run):
    spans = stage_spans(run)
    by = defaultdict(list)
    for r in run["rows"]:
        by[r["stage"]].append(r)
    out = []
    target = run["manifest"].get("target", "gate")
    for st in sorted(by):
        rs = by[st]
        start, end = spans[st]
        dur_s = (end - start) / 1000.0
        answered = [r for r in rs if r["status"] > 0]
        ok = [r for r in answered if not r["failed"]]
        fails = [r for r in rs if r["failed"]]
        lat = [r["latency"] for r in answered]
        lag = [r["sent"] - r["intended"] for r in rs]
        w0, w1 = run["wall0"] + start / 1000.0, run["wall0"] + end / 1000.0
        gate_names = ["rate-limiter"]
        cpu_ms, periods, thr = cg_window(run, gate_names, w0, w1)
        ls_cpu, _, _ = cg_window(run, ["localstack"], w0, w1)
        server_times = [r["store"] + r["gate"] for r in answered if r.get("store") is not None]
        # The gauge counts Prometheus's own /metrics scrape, one per replica.
        in_flight = prom_mean(run, "in_flight", w0, w1) if target == "gate" else None
        if in_flight is not None:
            in_flight = max(0.0, in_flight - run["manifest"]["replicas"])
        # Throughput: answers completed inside this stage's window, whichever
        # stage scheduled them. Counting a stage's own requests instead makes a
        # collapsing server look as if it answered everything, eventually.
        completed = sum(1 for r in run["rows"] if r["status"] > 0 and start <= r["done"] < end)
        lam = completed / dur_s if dur_s > 0 else None
        # Little's Law uses the stage's own requests: arrival rate x their time.
        lam_own = len(answered) / dur_s if dur_s > 0 else None
        out.append({
            "stage": st,
            "offered_rps": round(rs[0]["rate"] / run["factor"], 2),
            "nominal_rps": rs[0]["rate"],
            "duration_s": round(dur_s, 2),
            "requests": len(rs),
            "answered_rps": round(lam, 2) if lam else None,
            "ok": len(ok), "failed": len(fails),
            "failed_frac": len(fails) / len(rs),
            "status": dict(Counter(str(r["status"]) for r in rs)),
            "errors": dict(Counter(r["error"] for r in rs if r["error"])),
            "degraded": sum(1 for r in rs if r.get("degraded")),
            "p50": pct(lat, .5), "p90": pct(lat, .9), "p99": pct(lat, .99), "p999": pct(lat, .999),
            "max": max(lat) if lat else None,
            "send_lag_p99": pct(lag, .99),
            "generator_limited": (pct(lag, .99) or 0) > 10.0,
            "store_p50": pct([r["store"] for r in answered if r.get("store") is not None], .5),
            "gate_p50": pct([r["gate"] for r in answered if r.get("gate") is not None], .5),
            "gate_p99": pct([r["gate"] for r in answered if r.get("gate") is not None], .99),
            "cpu_ms_per_answer": round(cpu_ms / completed, 3) if completed and cpu_ms and target == "gate" else None,
            "localstack_cpu_ms_per_answer": round(ls_cpu / completed, 3) if completed and ls_cpu else None,
            "localstack_cores": round(ls_cpu / 1000.0 / dur_s, 2) if ls_cpu and dur_s > 0 else None,
            "gate_cores": round(cpu_ms / 1000.0 / dur_s, 2) if cpu_ms and dur_s > 0 else None,
            "throttled_frac": (thr / periods) if periods else None,
            # Little's Law, both sides (plan.md).
            "client_L": round(sum(lat) / 1000.0 / dur_s, 3) if lat and dur_s > 0 else None,
            "server_L_gauge": round(in_flight, 3) if in_flight is not None else None,
            "server_L_lambda_w": round(lam_own * statistics.mean(server_times) / 1000.0, 3) if server_times and lam_own else None,
            "instances": dict(Counter(r["instance"] for r in answered if r.get("instance"))),
        })
    return out


def saturation(stages):
    """The plan's rule: the highest offered stage, before the first failing one,
    with answered >= 95% offered, failures < 1%, and p99 < 1 s. Stage 0 is warm-up."""
    best = None
    for s in stages:
        if s["stage"] == 0:
            continue
        good = (s["answered_rps"] or 0) >= 0.95 * s["offered_rps"] and s["failed_frac"] < 0.01 \
            and s["p99"] is not None and s["p99"] < 1000.0
        if not good:
            return best, s
        best = s
    return best, None


# ----------------------------------------------------------------------- SVG

PALETTE = ["#2a6fdb", "#e0782b", "#2e9e5b", "#b04ad6", "#8a8a8a"]


def svg_curves(path, title, series, ylabel, log_y=True):
    """series: [(label, [(x, y, lo, hi), ...])]. Lines with interval bands."""
    W, H, L, R, T, B = 720, 420, 70, 170, 40, 50
    xs = [p[0] for _, pts in series for p in pts]
    ys = [v for _, pts in series for p in pts for v in p[1:] if v is not None and v > 0]
    if not xs or not ys:
        return
    x0, x1 = 0, max(xs) * 1.05
    y0, y1 = (min(ys) * 0.8, max(ys) * 1.25) if log_y else (0, max(ys) * 1.1)
    fy = (lambda v: math.log10(v)) if log_y else (lambda v: v)
    X = lambda x: L + (x - x0) / (x1 - x0) * (W - L - R)
    Y = lambda y: T + (1 - (fy(y) - fy(y0)) / (fy(y1) - fy(y0))) * (H - T - B)
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="sans-serif" font-size="12">',
         f'<rect width="{W}" height="{H}" fill="#fff"/>',
         f'<text x="{L}" y="22" font-size="14" font-weight="bold">{title}</text>',
         f'<line x1="{L}" y1="{H - B}" x2="{W - R}" y2="{H - B}" stroke="#999"/>',
         f'<line x1="{L}" y1="{T}" x2="{L}" y2="{H - B}" stroke="#999"/>',
         f'<text x="{(L + W - R) / 2}" y="{H - 12}" text-anchor="middle">offered requests per second (real time)</text>',
         f'<text x="16" y="{(T + H - B) / 2}" transform="rotate(-90 16 {(T + H - B) / 2})" text-anchor="middle">{ylabel}</text>']
    ticks = [v for v in [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000]
             if y0 <= v <= y1] if log_y else [y0 + (y1 - y0) * i / 5 for i in range(6)]
    for v in ticks:
        o.append(f'<line x1="{L}" y1="{Y(v):.1f}" x2="{W - R}" y2="{Y(v):.1f}" stroke="#eee"/>')
        o.append(f'<text x="{L - 6}" y="{Y(v) + 4:.1f}" text-anchor="end">{v:g}</text>')
    step = 50 if x1 < 600 else 100
    for v in range(0, int(x1) + 1, step):
        o.append(f'<text x="{X(v):.1f}" y="{H - B + 16}" text-anchor="middle">{v}</text>')
    for i, (label, pts) in enumerate(series):
        c = PALETTE[i % len(PALETTE)]
        pts = [p for p in pts if p[1] is not None and p[1] > 0]
        band = [p for p in pts if p[2] and p[3] and p[2] > 0]
        if len(band) > 1:
            poly = [(X(p[0]), Y(p[3])) for p in band] + [(X(p[0]), Y(p[2])) for p in reversed(band)]
            o.append(f'<polygon points="{" ".join(f"{a:.1f},{b:.1f}" for a, b in poly)}" fill="{c}" fill-opacity="0.15"/>')
        o.append(f'<polyline points="{" ".join(f"{X(p[0]):.1f},{Y(p[1]):.1f}" for p in pts)}" fill="none" stroke="{c}" stroke-width="2"/>')
        for p in pts:
            o.append(f'<circle cx="{X(p[0]):.1f}" cy="{Y(p[1]):.1f}" r="3" fill="{c}"/>')
        o.append(f'<rect x="{W - R + 14}" y="{T + 18 * i}" width="12" height="3" fill="{c}"/>')
        o.append(f'<text x="{W - R + 32}" y="{T + 18 * i + 5}">{label}</text>')
    o.append("</svg>")
    Path(path).write_text("\n".join(o))


# --------------------------------------------------------------------- phase 1

def phase1(root: Path, out: Path):
    runs = defaultdict(list)
    for d in sorted((root / "phase1").glob("*/rep*")):
        if (d / "manifest.json").exists():
            runs[d.parent.name].append(load_run(d))
    per_run, sat, curves = [], {}, {}
    for cond, rs in sorted(runs.items()):
        sat_vals = []
        stage_rows = defaultdict(list)
        for run in rs:
            st = stages_of(run)
            best, first_bad = saturation(st)
            sat_vals.append(best["offered_rps"] if best else 0.0)
            per_run.append({"condition": cond, "rep": run["manifest"]["rep"], "factor": run["factor"],
                            "saturation_rps": best["offered_rps"] if best else 0.0,
                            "first_failing_stage": first_bad, "stages": st,
                            "commit": run["manifest"]["commit"], "server": run["manifest"].get("server_health")})
            for r in run["rows"]:
                stage_rows[r["stage"]].append((run["manifest"]["rep"], r))
        sat[cond] = {"runs": sat_vals, "median": statistics.median(sat_vals),
                     "min": min(sat_vals), "max": max(sat_vals), "ci95_median": boot_median(sat_vals)}
        pts = {"p50": [], "p90": [], "p99": []}
        for stage in sorted(stage_rows):
            if stage == 0:
                continue
            by_rep = defaultdict(list)
            for rep, r in stage_rows[stage]:
                if r["status"] > 0:
                    by_rep[rep].append(r["latency"])
            groups = list(by_rep.values())
            pooled = [v for g in groups for v in g]
            # Offered real rate of this stage, averaged over runs.
            offered = statistics.mean(s["offered_rps"] for pr in per_run if pr["condition"] == cond
                                      for s in pr["stages"] if s["stage"] == stage)
            for q, name in [(.5, "p50"), (.9, "p90"), (.99, "p99")]:
                lo, hi = boot_ci(groups, lambda pool, q=q: pct(pool, q))
                pts[name].append((round(offered, 1), pct(pooled, q), lo, hi))
        curves[cond] = pts
    (out / "stages.json").write_text(json.dumps(per_run, indent=1, default=str))
    (out / "saturation.json").write_text(json.dumps(sat, indent=1))
    (out / "curves.json").write_text(json.dumps(curves, indent=1))
    for q in ("p50", "p99"):
        svg_curves(out / f"latency-{q}.svg", f"{q} latency from intended send time, 95% interval over 5 runs",
                   [(c, curves[c][q]) for c in sorted(curves)], f"{q} latency (ms, log scale)")
    return per_run, sat, curves


# --------------------------------------------------------------------- phase 2

def timeline(run):
    """Per real second (by intended time): offered, ok, failed, degraded, p99."""
    by = defaultdict(list)
    for r in run["rows"]:
        by[int(r["intended"] // 1000)].append(r)
    out = []
    for sec in sorted(by):
        rs = by[sec]
        lat = [r["latency"] for r in rs if r["status"] > 0]
        out.append({"t": sec, "n": len(rs), "failed": sum(r["failed"] for r in rs),
                    "degraded": sum(1 for r in rs if r.get("degraded")),
                    "no_answer": sum(1 for r in rs if r["status"] == 0),
                    "s5xx": sum(1 for r in rs if r["status"] >= 500),
                    "p99": pct(lat, .99), "p50": pct(lat, .5)})
    return out


def first_prom(run, key, pred, after_wall, label_filter=None):
    series = run["prom"].get(key)
    if not isinstance(series, list):
        return None
    at = defaultdict(float)
    for s in series:
        if label_filter and not label_filter(s["metric"]):
            continue
        for ts, v in s["values"]:
            if v not in ("NaN", "+Inf"):
                at[ts] += float(v)
    for ts in sorted(at):
        if ts >= after_wall and pred(at[ts]):
            return round(ts - after_wall, 1)
    return None


def alerts_seen(run, key, after_wall):
    series = run["prom"].get(key)
    if not isinstance(series, list):
        return {}
    seen = {}
    for s in series:
        name = s["metric"].get("alertname")
        ts = min(float(t) for t, _ in s["values"])
        if ts >= after_wall - 1:
            seen[name] = min(seen.get(name, math.inf), round(ts - after_wall, 1))
    return seen


def phase2(root: Path, out: Path):
    res = defaultdict(list)
    for d in sorted((root / "phase2").glob("*/rep*")):
        if not (d / "manifest.json").exists():
            continue
        run = load_run(d)
        m = run["manifest"]
        tl = timeline(run)
        spans = stage_spans(run)
        if m["fault"] != "none":
            onset_ms = m["fault_at"] * 1000.0
            drop_ms = (m["fault_at"] + m["fault_for"]) * 1000.0
        else:
            onset_ms = spans[1][0]
            drop_ms = spans[2][0]
        pre = [r["latency"] for r in run["rows"] if r["intended"] < onset_ms and r["intended"] > 10_000 and r["status"] > 0]
        pre_p99 = pct(pre, .99)
        # Recovery (plan.md): from the drop to the start of the first 10 s
        # window with no failures and p99 under 2x the pre-overload p99.
        recovery = None
        by_sec = {x["t"]: x for x in tl}
        t = int(drop_ms // 1000)
        last = max(by_sec) if by_sec else 0
        while t + 10 <= last:
            win = [by_sec.get(t + k) for k in range(10)]
            rows = [r for r in run["rows"] if t * 1000 <= r["intended"] < (t + 10) * 1000]
            lat = [r["latency"] for r in rows if r["status"] > 0]
            if rows and not any(r["failed"] for r in rows) and pre_p99 and pct(lat, .99) < 2 * pre_p99:
                recovery = round(t - drop_ms / 1000.0, 1)
                break
            t += 1
        onset_wall = run["wall0"] + onset_ms / 1000.0
        degraded_rows = [r for r in run["rows"] if r.get("degraded")]
        order = {
            "p99_over_1s": next((x["t"] - onset_ms / 1000 for x in tl if x["t"] * 1000 >= onset_ms and (x["p99"] or 0) > 1000), None),
            "first_failure": next((x["t"] - onset_ms / 1000 for x in tl if x["t"] * 1000 >= onset_ms and x["failed"] > 0), None),
            "bulkhead_waiting": first_prom(run, "bulkhead_queued", lambda v: v > 0, onset_wall,
                                           lambda mt: mt.get("name") == "ratelimit-operations"),
            "bulkhead_full": first_prom(run, "bulkhead_in_flight", lambda v: v >= 100, onset_wall,
                                        lambda mt: mt.get("name") == "ratelimit-operations"),
            "shed_bulkhead": first_prom(run, "degraded_by_reason", lambda v: v > 0, onset_wall,
                                        lambda mt: mt.get("reason") == "bulkhead"),
            "shed_error": first_prom(run, "degraded_by_reason", lambda v: v > 0, onset_wall,
                                     lambda mt: mt.get("reason") == "error"),
            "shed_breaker": first_prom(run, "degraded_by_reason", lambda v: v > 0, onset_wall,
                                       lambda mt: mt.get("reason") == "circuit_breaker"),
            "breaker_open": first_prom(run, "breaker_state", lambda v: v >= 0.99, onset_wall),
            "events_dropped": first_prom(run, "events_dropped_rate", lambda v: v > 0, onset_wall),
            "event_queue_over_1000": first_prom(run, "event_queue_depth", lambda v: v > 1000, onset_wall),
        }
        res[m["name"]].append({
            "rep": m["rep"], "factor": run["factor"], "pre_p99_ms": pre_p99,
            "recovery_s": recovery, "order_s": order,
            "alerts_pending": alerts_seen(run, "alerts_pending", onset_wall),
            "alerts_firing": alerts_seen(run, "alerts", onset_wall),
            "correctness": run["correctness"], "usage": run["usage"], "usage_dropped": run["usage_dropped"],
            "degraded_service_ms": {"p50": pct([r["service"] for r in degraded_rows], .5),
                                    "p90": pct([r["service"] for r in degraded_rows], .9),
                                    "n": len(degraded_rows)},
            "statuses": dict(Counter(str(r["status"]) + ("d" if r.get("degraded") else "") for r in run["rows"])),
            "errors": dict(Counter(r["error"] for r in run["rows"] if r["error"])),
            "max_send_lag_p99_by_stage": max((pct([r["sent"] - r["intended"] for r in run["rows"] if r["stage"] == s], .99) or 0) for s in spans),
            "timeline": tl,
        })
    (out / "phase2.json").write_text(json.dumps(res, indent=1))
    return res


if __name__ == "__main__":
    root, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    if (root / "phase1").exists():
        phase1(root, out)
    if (root / "phase2").exists():
        phase2(root, out)
    print(f"wrote {out}")
