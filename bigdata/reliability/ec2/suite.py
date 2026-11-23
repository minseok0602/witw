"""Run the cloud experiment in phases; never present an unfinished phase as passed."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

import requests

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))
from run_experiments import compose, wait_for, query, seed
from wait_pipeline_idle import wait_idle
from lab_settings import PROMETHEUS_URL

STATE = LAB / ".generated/ec2/suite-status.json"


def checkpoint(status, **fields):
    data = {"status": status, "updated_at_utc": datetime.now(timezone.utc).isoformat(), **fields}
    STATE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(json.dumps(data), flush=True)


def run_script(filename, *args):
    subprocess.run([sys.executable, str(LAB / filename), *map(str, args)], check=True)


def latest_result(previous):
    folders = [path for path in (LAB / "results").iterdir() if path.is_dir() and path.name not in previous]
    if len(folders) != 1:
        raise RuntimeError("Cannot uniquely identify the result directory")
    return folders[0]


def results():
    return {path.name for path in (LAB / "results").iterdir()} if (LAB / "results").exists() else set()


def main(resume_core=None):
    (LAB / "results").mkdir(exist_ok=True)
    core = Path(resume_core).resolve() if resume_core else None
    if core:
        if core.parent != LAB / "results" or not all((core / name).is_file() for name in ("summary.json", "verification.json")):
            raise ValueError("Resume requires an existing independently verified core result in this lab")
        checkpoint("resuming-after-core", core=str(core))
    else:
        checkpoint("starting-services")
        compose("up", "-d", "kafka", "postgres", "redis", "toxiproxy", "receiver", "alertmanager", "kafka-exporter")
        run_script("init_lab.py")
        compose("up", "-d", "backend-before", "backend-after", "prometheus")
        wait_for(lambda: query("after", "SELECT COUNT(*) FROM pipeline_event_inbox"), timeout=300, label="Flyway readiness")
    def monitoring_ready():
        response = requests.get(PROMETHEUS_URL + "/api/v1/targets", timeout=5)
        response.raise_for_status()
        targets = response.json()["data"]["activeTargets"]
        return len(targets) >= 6 and all(target["health"] == "up" for target in targets)
    wait_for(monitoring_ready, timeout=300, label="all Prometheus scrape targets ready before formal trials")
    observers = []
    try:
        if not core:
            observers.append(subprocess.Popen([sys.executable, str(LAB / "observe_notifications.py")]))
            long_watch = subprocess.Popen([sys.executable, str(LAB / "watch_long.py")])
            checkpoint("core-fault-trials", repetitions=3, articles_per_trial=1000)
            previous = results()
            run_script("run_experiments.py", "--repetitions", 3, "--count", 1000)
            core = latest_result(previous)
            if long_watch.wait(timeout=1900) != 0:
                raise RuntimeError("Long-outage watcher failed")
            def resolved():
                path = LAB / ".generated/state/notification-observations.jsonl"
                return path.exists() and any('"CollectorDeliveryStalled"' in line and '"resolved"' in line
                                            for line in path.read_text().splitlines())
            wait_for(resolved, timeout=360, label="resolved outbox notification")
        wait_idle(core)
        if not resume_core:
            run_script("verify_evidence.py", core)
        checkpoint("broker-and-spark-trials", core=str(core))
        previous = results()
        run_script("ec2/extended.py")
        extended = latest_result(previous)
        run_script("verify_evidence.py", extended)
        checkpoint("isolated-performance-trials", core=str(core), extended=str(extended))
        performance = []
        for repetition in range(1, 4):
            order = ("before", "after") if repetition % 2 else ("after", "before")
            for variant in order:
                previous = results()
                run_script("load_soak.py", 10000, "--variant", variant)
                performance.append(str(latest_result(previous)))
        wait_idle(core)
        for output in [core, extended, *map(Path, performance)]:
            run_script("capture_evidence.py", output)
        checkpoint("container-trials-complete-host-trials-pending", core=str(core), extended=str(extended),
                   performance=performance,
                   note="External operator must still run EC2 stop/start trials; no host-failure pass is claimed yet")
    except Exception as error:
        checkpoint("failed", error=repr(error), core=str(core) if core else None)
        raise
    finally:
        (LAB / ".generated/state/stop-notification-observer").write_text("stop")
        for observer in observers:
            observer.wait(timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume-core", type=Path)
    args = parser.parse_args()
    main(args.resume_core)
