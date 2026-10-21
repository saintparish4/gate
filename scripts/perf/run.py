#!/usr/bin/env python3
"""One overload run, end to end, on a fresh stack (docs/perf).

    run.py --name ramp-r1 --rep 1 --replicas 1 --stages 10x30,20x15,... \\
           [--fault latency|pause|aggregator --fault-at 60 --fault-for 30] \\
           [--correctness] [--backend in-memory] [--target dynamodb] \\
           --out perf-results/<plan>/<name>/rep1

It brings up LocalStack, Toxiproxy, the replicas, the aggregator, nginx and
Prometheus on the pinned cores (docker-compose.perf.yml), samples every
container's cgroup each second, drives the open-loop generator on cpus 6-7,
injects the fault on schedule, optionally runs invariants A-D beside the load,
then runs the usage check, exports Prometheus series and alerts, stops the
stack so each JVM writes its GC log and JFR recording, copies them out, and
tears everything down. Everything lands in --out; manifest.json says what ran.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "gateperf"
FILES = ["-f", "docker-compose.yml", "-f", "docker-compose.scale.yml", "-f", "docker-compose.perf.yml"]
SERVICES = ["localstack", "toxiproxy", "rate-limiter", "usage-aggregator", "lb", "prometheus"]
GEN_CPUS = os.environ.get("GEN_CPUS", "6,7")


def sh(cmd, env=None, check=True, capture=False, timeout=None):
    r = subprocess.run(cmd, cwd=ROOT, env=env, text=True, timeout=timeout,
                       stdout=subprocess.PIPE if capture else None,
                       stderr=subprocess.STDOUT if capture else None)
    if check and r.returncode != 0:
        raise SystemExit(f"failed ({r.returncode}): {' '.join(cmd)}\n{r.stdout or ''}")
    return r


def compose(*args, env, **kw):
    return sh(["docker", "compose", *FILES, *args], env=env, **kw)


def container_ids(env):
    out = compose("ps", "-q", env=env, capture=True).stdout.split()
    names = {}
    for cid in out:
        name = sh(["docker", "inspect", "-f", "{{.Name}}", cid], capture=True).stdout.strip().lstrip("/")
        full = sh(["docker", "inspect", "-f", "{{.Id}}", cid], capture=True).stdout.strip()
        names[name] = full
    return names


def prom_range(expr, start, end):
    q = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": "1"})
    with urllib.request.urlopen(f"http://localhost:9090/api/v1/query_range?{q}", timeout=30) as r:
        return json.load(r)["data"]["result"]


PROM_SERIES = {
    "in_flight": "sum by (instance) (gate_http_in_flight)",
    "answers_by_code": 'sum by (code, degraded) (rate(gate_http_requests_total{route=~"/v1/.+"}[5s]))',
    "event_queue_depth": "sum by (instance) (gate_events_queue_depth)",
    "events_dropped_rate": "sum(rate(gate_events_dropped_total[5s]))",
    "events_published_rate": "sum(rate(gate_events_published_total[5s]))",
    "bulkhead_in_flight": "sum by (instance, name) (gate_bulkhead_in_flight)",
    "bulkhead_queued": "sum by (instance, name) (gate_bulkhead_queued)",
    "breaker_state": "max by (instance) (gate_circuit_breaker_state)",
    "degraded_by_reason": "sum by (reason) (rate(gate_degraded_total[5s]))",
    "server_latency_p99": 'histogram_quantile(0.99, sum by (le) (rate(gate_http_request_duration_seconds_bucket{route=~"/v1/.+"}[5s])))',
    "server_store_p99": 'histogram_quantile(0.99, sum by (le) (rate(gate_http_store_seconds_bucket{route=~"/v1/.+"}[5s])))',
    "aggregator_lag_ms": "max(gate_usage_millis_behind_latest)",
    "alerts": 'ALERTS{alertstate="firing"}',
    "alerts_pending": 'ALERTS{alertstate="pending"}',
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--rep", type=int, required=True)
    ap.add_argument("--replicas", type=int, default=1)
    ap.add_argument("--stages", required=True)
    ap.add_argument("--fault", choices=["none", "latency", "pause", "aggregator"], default="none")
    ap.add_argument("--fault-at", type=float, default=0)
    ap.add_argument("--fault-for", type=float, default=0)
    ap.add_argument("--latency-ms", type=int, default=1000)
    ap.add_argument("--correctness", action="store_true")
    ap.add_argument("--correctness-at", type=float, default=0)
    ap.add_argument("--backend", default="dynamodb")
    ap.add_argument("--target", default="gate")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    out = (ROOT / a.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    cp = Path(os.environ["LOADSIM_CP"]).read_text().strip()
    env = dict(os.environ, COMPOSE_PROJECT_NAME=PROJECT, GATE_REPLICAS=str(a.replicas),
               GATE_STORAGE_BACKEND=a.backend,
               GATE_KINESIS_ENABLED="false" if a.backend == "in-memory" else "true")
    manifest = {"name": a.name, "rep": a.rep, "replicas": a.replicas, "stages": a.stages,
                "fault": a.fault, "fault_at": a.fault_at, "fault_for": a.fault_for,
                "latency_ms": a.latency_ms if a.fault == "latency" else None,
                "correctness": a.correctness, "backend": a.backend, "target": a.target,
                "commit": sh(["git", "describe", "--always", "--dirty"], capture=True).stdout.strip(),
                "generator_cpus": GEN_CPUS}

    compose("down", "-v", "--remove-orphans", env=env, check=False, capture=True)
    t_up = time.time()
    compose("up", "-d", "--no-build", "--wait", *SERVICES, env=env, capture=True, timeout=600)
    manifest["stack_up_s"] = round(time.time() - t_up, 1)
    ids = container_ids(env)
    manifest["containers"] = sorted(ids)
    manifest["images"] = {s: sh(["docker", "inspect", "-f", "{{.Image}}", cid], capture=True).stdout.strip()
                          for s, cid in ids.items()}
    health = sh(["curl", "-sf", "http://localhost:8080/health"], capture=True, check=False).stdout
    manifest["server_health"] = health.strip()

    stop = out / ".stop"
    stop.unlink(missing_ok=True)
    sampler = subprocess.Popen([sys.executable, str(ROOT / "scripts/perf/sample_cgroups.py"),
                                str(out / "cgroups.jsonl"), str(stop),
                                *[f"{n}={i}" for n, i in ids.items()]])

    agg = next((n for n in ids if "usage-aggregator" in n), None)
    ls = next((n for n in ids if "localstack" in n), None)

    def fault_on():
        if a.fault == "latency":
            body = json.dumps({"name": "store_latency", "type": "latency", "stream": "downstream",
                               "attributes": {"latency": a.latency_ms, "jitter": 0}}).encode()
            urllib.request.urlopen(urllib.request.Request(
                "http://localhost:8474/proxies/dynamodb/toxics", data=body,
                headers={"Content-Type": "application/json"}, method="POST"), timeout=10)
        elif a.fault == "pause":
            sh(["docker", "pause", ls])
        elif a.fault == "aggregator":
            sh(["docker", "kill", agg])

    def fault_off():
        if a.fault == "latency":
            urllib.request.urlopen(urllib.request.Request(
                "http://localhost:8474/proxies/dynamodb/toxics/store_latency", method="DELETE"), timeout=10)
        elif a.fault == "pause":
            sh(["docker", "unpause", ls])
        elif a.fault == "aggregator":
            sh(["docker", "start", agg])

    events = []

    def fault_thread(t0):
        time.sleep(max(0, t0 + a.fault_at - time.time()))
        events.append({"event": "fault_on", "t": time.time()})
        fault_on()
        time.sleep(a.fault_for)
        fault_off()
        events.append({"event": "fault_off", "t": time.time()})

    gen_cmd = ["taskset", "-c", GEN_CPUS, "java", "-XX:ActiveProcessorCount=2", "-cp", cp, "LoadSim",
               "--scenario", "openloop", "--target", a.target,
               "--url", "http://localhost:4566" if a.target == "dynamodb" else "http://localhost:8080",
               "--stages", a.stages, "--out", str(out / "requests.jsonl"),
               "--run-id", f"{a.name}-{a.rep}-{int(time.time())}"]
    corr = None
    t0 = time.time()
    manifest["wall_start"] = t0
    if a.fault != "none":
        threading.Thread(target=fault_thread, args=(t0,), daemon=True).start()
    corr_box = []

    def start_correctness():
        time.sleep(max(0, t0 + a.correctness_at - time.time()))
        corr_box.append(subprocess.Popen(["taskset", "-c", GEN_CPUS, "java", "-cp", cp, "LoadSim",
                                          "--scenario", "correctness", "--url", "http://localhost:8080",
                                          "--concurrency", "5", "--results-dir", str(out)],
                                         cwd=ROOT, stdout=open(out / "correctness.log", "w"),
                                         stderr=subprocess.STDOUT))
    corr_thread = None
    if a.correctness:
        corr_thread = threading.Thread(target=start_correctness, daemon=True)
        corr_thread.start()
    if False:
        # Beside the load, at low concurrency, on the generator's cores: its
        # verdict says whether overload ever over-issues.
        corr = subprocess.Popen(["taskset", "-c", GEN_CPUS, "java", "-cp", cp, "LoadSim",
                                 "--scenario", "correctness", "--url", "http://localhost:8080",
                                 "--concurrency", "5", "--results-dir", str(out)],
                                cwd=ROOT, stdout=open(out / "correctness.log", "w"), stderr=subprocess.STDOUT)
    gen = sh(gen_cmd, capture=True, check=False)
    (out / "generator.log").write_text(gen.stdout)
    manifest["generator_exit"] = gen.returncode
    if corr_thread:
        corr_thread.join()
        manifest["correctness_exit"] = corr_box[0].wait()
    t1 = time.time()
    manifest["wall_end"] = t1
    manifest["events"] = events

    if a.backend != "in-memory" and a.target == "gate":
        uc = sh(["bash", "scripts/check-usage-pipeline.sh"], capture=True, check=False,
                env=dict(env, COMPOSE="docker compose " + " ".join(FILES), WAIT_SECONDS="240", SETTLE_SECONDS="10"))
        (out / "usage-check.txt").write_text(uc.stdout)
        manifest["usage_check_exit"] = uc.returncode

    prom = {}
    for key, expr in PROM_SERIES.items():
        try:
            prom[key] = prom_range(expr, t0 - 5, time.time())
        except Exception as e:  # noqa: BLE001
            prom[key] = {"error": str(e)}
    (out / "prometheus.json").write_text(json.dumps(prom))

    stop.touch()
    sampler.wait(timeout=30)
    compose("stop", "-t", "60", "rate-limiter", env=env, capture=True, check=False)
    for name, cid in ids.items():
        if "rate-limiter" in name:
            for f in ("gc.log", "gate.jfr"):
                sh(["docker", "cp", f"{cid}:/tmp/{f}", str(out / f"{name}.{f}")], check=False, capture=True)
            logs = sh(["docker", "logs", cid], capture=True, check=False).stdout
            (out / f"{name}.log").write_text(logs)
    compose("down", "-v", "--remove-orphans", env=env, capture=True, check=False)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(json.dumps({k: manifest.get(k) for k in ("name", "rep", "generator_exit", "correctness_exit", "usage_check_exit", "stack_up_s")}))


if __name__ == "__main__":
    main()
