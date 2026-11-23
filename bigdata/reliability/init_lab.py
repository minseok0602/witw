import json
from pathlib import Path
import subprocess
import sys
import time

LAB = Path(__file__).resolve().parent
sys.path.insert(0, str(LAB / ".generated/python-deps"))
import psycopg
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from lab_settings import PROJECT, KAFKA_BOOTSTRAP, DB_HOST, DB_PORT, DB_PASSWORD, PROXY_URL

deadline = time.monotonic() + 120
admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
while True:
    try:
        admin.list_topics(timeout=5)
        break
    except Exception:
        if time.monotonic() > deadline:
            raise
        time.sleep(1)
for name, partitions in [("news.raw.v1", 6), ("news.enriched.v1", 6), ("news.event.v1", 3),
                         ("news.unclassified.v1", 6), ("news.event.v1.dlt", 3)]:
    if name not in admin.list_topics(timeout=5).topics:
        admin.create_topics([NewTopic(name, partitions, 1)])[name].result(20)
while True:
    try:
        connection = psycopg.connect(host=DB_HOST, port=DB_PORT, dbname="postgres", user="postgres", password=DB_PASSWORD,
                                     autocommit=True, connect_timeout=3)
        break
    except Exception:
        if time.monotonic() > deadline:
            raise
        time.sleep(1)
with connection:
    existing = {row[0] for row in connection.execute("SELECT datname FROM pg_database")}
    for name in ("witw_before", "witw_after"):
        if name not in existing:
            connection.execute("CREATE DATABASE " + name)
response = requests.post(PROXY_URL + "/proxies", json={"name": "isolated", "listen": "0.0.0.0:39092",
                         "upstream": "kafka:39092", "enabled": True}, timeout=5)
if response.status_code not in (200, 201, 409):
    response.raise_for_status()
subprocess.run(["docker", "compose", "-p", PROJECT, "-f", str(LAB / "compose.yml"),
                "up", "-d", "collector-long", "kafka-exporter"], check=True)
state = LAB / ".generated/state/long-network"
deadline = time.monotonic() + 60
while not (state / "staged.json").exists():
    if time.monotonic() > deadline:
        raise TimeoutError("Long collector failed to stage")
    time.sleep(0.25)
requests.post(PROXY_URL + "/proxies/isolated", json={"enabled": False}, timeout=5).raise_for_status()
start = {"fault_epoch_ns": time.time_ns(), "fault_monotonic_ns": time.monotonic_ns(),
         "outage_type": "real TCP proxy disabled", "collector_poll_seconds": 600,
         "alert_rules_sha256": __import__("hashlib").sha256((LAB.parent / "monitoring/prometheus/alerts.yml").read_bytes()).hexdigest()}
(state / "fault.json").write_text(json.dumps(start, indent=2), encoding="utf-8")
(state / "release").write_text("release", encoding="utf-8")
print(json.dumps(start), flush=True)
