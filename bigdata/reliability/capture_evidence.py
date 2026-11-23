"""Export only this isolated project's evidence before stopping its services."""
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from run_experiments import LAB, ROOT, PROJECT, COMPOSE, VARIANTS, connect
import requests
from lab_settings import RECEIVER_URL, PROMETHEUS_URL


def command(*args):
    return subprocess.check_output(list(map(str, args)), text=True, encoding="utf-8", errors="replace")


output = Path(sys.argv[1]).resolve()
raw = output / "raw"
raw.mkdir(exist_ok=True)
containers = [item for item in command("docker", "ps", "-a", "--filter",
    "label=com.docker.compose.project=" + PROJECT, "--format", "{{.ID}}").splitlines() if item]
details = json.loads(command("docker", "inspect", *containers))
images = {}
services = {}
for item in details:
    services[item["Config"]["Labels"]["com.docker.compose.service"]] = {
        "image": item["Config"]["Image"], "image_id": item["Image"],
        "cpu_limit": item["HostConfig"]["NanoCpus"] / 1e9,
        "memory_limit_bytes": item["HostConfig"]["Memory"],
        "oom_killed": item["State"]["OOMKilled"], "restart_count": item["RestartCount"]}
    if item["Image"] not in images:
        image = json.loads(command("docker", "image", "inspect", item["Image"]))[0]
        images[item["Image"]] = {key: image.get(key) for key in ("RepoTags", "RepoDigests", "Architecture", "Os", "Created")}
metadata = {"captured_at_utc": datetime.now(timezone.utc).isoformat(),
    "docker": json.loads(command("docker", "info", "--format", '{{json .}}')),
    "services": services, "images": images}
metadata["host_python"] = sys.version
metadata["host_python_packages"] = {name: importlib.metadata.version(name)
                                    for name in ("confluent-kafka", "psycopg", "requests")}
# Keep resource/runtime properties, avoiding host identity and environment values.
metadata["docker"] = {key: metadata["docker"].get(key) for key in
    ("ServerVersion", "OperatingSystem", "OSType", "Architecture", "NCPU", "MemTotal", "KernelVersion", "Driver")}
if os.name == "nt":
    metadata["host_hardware"] = json.loads(command("powershell", "-NoProfile", "-Command",
        "[pscustomobject]@{cpu=(Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors); "
        "ram_bytes=(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory} | ConvertTo-Json -Depth 4"))
else:
    metadata["host_hardware"] = {"lscpu": json.loads(command("lscpu", "--json")),
        "ram_bytes": os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"),
        "clock_sync": command("timedatectl", "show", "--property=NTPSynchronized")}
(output / "environment.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
shutil.copy2(LAB / ".generated/source-manifest.json", output / "source-manifest.json")
sources = sorted((LAB.parent / "collector").glob("*.py")) + sorted((LAB.parent / "spark/apps").glob("*.py"))
sources += sorted((ROOT / "backend/src/main/java/com/ssafy/witw/infra/kafka/dbwriter").glob("*.java"))
sources += sorted(LAB.glob("*.py")) + [LAB / "EdgeProbe.java", LAB / "compose.yml", LAB / "prometheus.yml",
    LAB.parent / "monitoring/prometheus/alerts.yml"]
sources += sorted((LAB / "ec2").glob("*.py"))
hashes = {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
(output / "source-sha256.json").write_text(json.dumps(hashes, indent=2), encoding="utf-8")
for variant in VARIANTS:
    with connect(variant) as db:
        if variant == "after":
            metadata["postgresql_version"] = db.execute("SELECT version()").fetchone()[0]
        with (raw / ("issue-write-audit-" + variant + ".jsonl")).open("w", encoding="utf-8") as stream:
            for row in db.execute("SELECT id,issue_name,operation,at FROM lab_issue_writes ORDER BY id"):
                stream.write(json.dumps(list(row), default=str) + "\n")
        state = {"pending_events": db.execute("SELECT COUNT(*) FROM pipeline_event_inbox WHERE status='PENDING'").fetchone()[0],
                 "applied_events": db.execute("SELECT COUNT(*) FROM pipeline_event_inbox WHERE status='APPLIED'").fetchone()[0]}
        (raw / ("inbox-final-" + variant + ".json")).write_text(json.dumps(state), encoding="utf-8")
    if "--db-dump" in sys.argv:
        postgres = next(item["Id"] for item in details
                        if item["Config"]["Labels"]["com.docker.compose.service"] == "postgres")
        with (raw / ("witw-" + variant + ".pgdump")).open("wb") as stream:
            subprocess.run(["docker", "exec", postgres, "pg_dump", "-U", "postgres", "--format=custom",
                            "witw_" + variant], stdout=stream, stderr=subprocess.PIPE, check=True)
(output / "environment.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
for name in ("notifications.jsonl", "notification-observations.jsonl"):
    source = LAB / ".generated/state" / name
    if source.exists():
        shutil.copy2(source, raw / name)
if os.getenv("LAB_EC2_INVENTORY"):
    response = requests.get(RECEIVER_URL + "/", timeout=10)
    response.raise_for_status()
    (raw / "notifications.jsonl").write_text(response.text, encoding="utf-8")
    from ec2.docker_router import inventory, execute
    nodes = {}
    for role in ("infra", "compute", "app"):
        nodes[role] = {"docker_info": json.loads(execute(role, ["info", "--format", "{{json .}}"], capture=True)),
                       "instance_id": inventory()["nodes"][role]["instance_id"],
                       "private_ip": inventory()["nodes"][role]["private_ip"]}
        nodes[role]["docker_info"] = {key: nodes[role]["docker_info"].get(key) for key in
            ("ServerVersion", "NCPU", "MemTotal", "KernelVersion", "OperatingSystem")}
    metadata["ec2_nodes"] = nodes
    (output / "environment.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
for service in services:
    (raw / (service + ".log")).write_text(command(*COMPOSE, "logs", "--no-color", "--timestamps", service), encoding="utf-8")
for endpoint in ("alerts", "targets", "rules"):
    response = requests.get(PROMETHEUS_URL + "/api/v1/" + endpoint, timeout=10)
    response.raise_for_status()
    (raw / ("prometheus-" + endpoint + ".json")).write_text(response.text, encoding="utf-8")
files = {path.relative_to(output).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
         for path in sorted(output.rglob("*")) if path.is_file() and path.name != "evidence-sha256.json"}
(output / "evidence-sha256.json").write_text(json.dumps(files, indent=2), encoding="utf-8")
print("Captured", len(files), "evidence files for", output.name)
