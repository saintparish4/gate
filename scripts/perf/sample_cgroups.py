#!/usr/bin/env python3
"""Sample each container's cgroup every second until the stop file appears.

    sample_cgroups.py OUT.jsonl STOPFILE name=containerid ...

One line per container per second: CPU used, CPU throttled (count and time,
from cpu.stat), and memory, read from the host's /sys/fs/cgroup/docker/<id>.
"""
import json
import os
import sys
import time


def read_kv(path):
    try:
        with open(path) as f:
            return dict(line.split() for line in f if line.strip())
    except OSError:
        return {}


def read_one(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


out, stop = sys.argv[1], sys.argv[2]
targets = dict(a.split("=", 1) for a in sys.argv[3:])
with open(out, "w") as f:
    while not os.path.exists(stop):
        t = time.time()
        for name, cid in targets.items():
            base = f"/sys/fs/cgroup/docker/{cid}"
            st = read_kv(f"{base}/cpu.stat")
            if not st:
                continue
            f.write(json.dumps({
                "t": round(t, 3), "name": name,
                "usage_usec": int(st.get("usage_usec", 0)),
                "nr_periods": int(st.get("nr_periods", 0)),
                "nr_throttled": int(st.get("nr_throttled", 0)),
                "throttled_usec": int(st.get("throttled_usec", 0)),
                "memory_bytes": int(read_one(f"{base}/memory.current") or 0),
            }) + "\n")
        f.flush()
        time.sleep(max(0.0, 1.0 - (time.time() - t)))
