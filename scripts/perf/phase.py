#!/usr/bin/env python3
"""Run one phase of the overload plan (docs/perf/.../plan.md), resumably.

    phase.py 1 perf-results/2026-10-20            # phase 1: ramps
    phase.py 2 perf-results/2026-10-20 --sat r1=80 r3=80
    phase.py 1 perf-results/2026-10-20 --dry-run  # print the order only

Five blocks; in each block every condition once, shuffled with seed
20261020 + block. A run whose manifest.json exists is skipped, so a stopped
phase resumes where it left off. A run that fails for an infrastructure reason
is retried once at the end of the phase, as the plan says.
"""
import argparse
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAMP = "10x30," + ",".join(f"{r}x15" for r in [20, 40, 60, 80, 100, 120, 150, 180, 220, 270, 330, 400])
INMEM = "50x30," + ",".join(f"{r}x15" for r in [100, 200, 300, 400, 500])


def phase1():
    return {
        "ramp-r1": ["--replicas", "1", "--stages", RAMP],
        "ramp-r2": ["--replicas", "2", "--stages", RAMP],
        "ramp-r3": ["--replicas", "3", "--stages", RAMP],
        "localstack-alone": ["--replicas", "1", "--stages", RAMP, "--target", "dynamodb"],
        "inmemory-r1": ["--replicas", "1", "--stages", INMEM, "--backend", "in-memory"],
    }


def phase2(sat):
    def hold(k, r):
        s = sat[f"r{r}"]
        return ["--replicas", str(r), "--stages", f"{s * .5:g}x30,{s * k:g}x180,{s * .5:g}x120",
                "--correctness", "--correctness-at", "30"]
    s3 = sat["r3"]
    fault = lambda kind, extra: ["--replicas", "3", "--stages", f"{s3 * .5:g}x240", "--fault", kind,
                                 "--fault-at", "60", "--correctness", "--correctness-at", "50", *extra]
    return {
        "hold1.5-r1": hold(1.5, 1), "hold1.5-r3": hold(1.5, 3),
        "hold3-r1": hold(3, 1), "hold3-r3": hold(3, 3),
        "fault-latency": fault("latency", ["--fault-for", "60", "--latency-ms", "1000"]),
        "fault-pause": fault("pause", ["--fault-for", "30"]),
        "fault-aggregator": fault("aggregator", ["--fault-for", "60"]),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", type=int)
    ap.add_argument("root")
    ap.add_argument("--sat", nargs="*", default=[])
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    conds = phase1() if a.phase == 1 else phase2({k: float(v) for k, v in (s.split("=") for s in a.sat)})
    order = []
    for block in range(1, 6):
        names = sorted(conds)
        random.Random(20261020 + block).shuffle(names)
        order += [(block, n) for n in names]
    if a.dry_run:
        for b, n in order:
            print(b, n, " ".join(conds[n]))
        return
    failed = []
    for i, (block, name) in enumerate(order + [None] * 0):
        out = Path(a.root) / f"phase{a.phase}" / name / f"rep{block}"
        if (ROOT / out / "manifest.json").exists():
            continue
        print(f"[{time.strftime('%H:%M:%S')}] {i + 1}/{len(order)} block {block} {name}", flush=True)
        r = subprocess.run([sys.executable, "scripts/perf/run.py", "--name", name, "--rep", str(block),
                            *conds[name], "--out", str(out)], cwd=ROOT)
        if r.returncode != 0:
            failed.append((block, name))
            print(f"  infrastructure failure, will retry once: {name} rep{block}", flush=True)
    for block, name in failed:
        out = Path(a.root) / f"phase{a.phase}" / name / f"rep{block}"
        print(f"[{time.strftime('%H:%M:%S')}] retry {name} rep{block}", flush=True)
        (ROOT / out / "RETRIED").parent.mkdir(parents=True, exist_ok=True)
        (ROOT / out / "RETRIED").write_text("first attempt failed for an infrastructure reason\n")
        subprocess.run([sys.executable, "scripts/perf/run.py", "--name", name, "--rep", str(block),
                        *conds[name], "--out", str(out)], cwd=ROOT)


if __name__ == "__main__":
    main()
