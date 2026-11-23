"""Prepare/verify a durable source batch around an externally controlled EC2 restart."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))
from fixtures import embedding
from lab_settings import KAFKA_BOOTSTRAP
from run_experiments import (compose, wait_for, query, VARIANTS, Consumer, TopicPartition,
                             AdminClient, OffsetSpec)
from wait_pipeline_idle import wait_idle


def prepare(run, count):
    state = LAB / ".generated/state" / run
    output = LAB / "results/host-cycles" / run
    (output / "raw").mkdir(parents=True, exist_ok=False)
    wait_idle(output)
    env = {**os.environ, "LAB_RUN": run, "LAB_VARIANT": "after", "LAB_COUNT": str(count), "LAB_FAILPOINT": "outbox_commit"}
    compose("up", "-d", "--no-deps", "--force-recreate", "collector-case", env=env)
    wait_for(lambda: (state / "hit.json").exists(), timeout=180, label="durable batch before host failure")
    hit = json.loads((state / "hit.json").read_text())
    if hit["outbox"]["pending"] != count or hit.get("pause_protocol") != "blocking-trace-v2":
        raise RuntimeError("Host failpoint was not reached; do not run the host interruption")
    for filename in ("inputs.jsonl", "hit.json"):
        shutil.copy2(state / filename, output / "raw" / filename)
    (output / "prepared.json").write_text(json.dumps({"run": run, "input_articles": count, "failpoint": hit,
        "prepared_at_utc": datetime.now(timezone.utc).isoformat()}, indent=2))
    print("HOST_CASE_PREPARED", run, flush=True)


def verify(run):
    state = LAB / ".generated/state" / run
    output = LAB / "results/host-cycles" / run
    manifest = {row["article_id"]: row for row in map(json.loads, (output / "raw/inputs.jsonl").read_text().splitlines())}
    # After a compute-host reboot the collector resumes automatically from SQLite.
    # Other host trials release the trace while that remote host is unavailable.
    (state / "resume").touch()
    def source_drained():
        if not (state / "delivery.json").exists():
            return False
        status = json.loads((state / "delivery.json").read_text()).get("outbox", {})
        return status.get("pending") == 0 and status.get("acknowledged", 0) >= len(manifest)
    wait_for(source_drained, timeout=1200, label="all source records acknowledged after host restart")
    wait_for(lambda: query("after", "SELECT 1"), timeout=300, label="PostgreSQL readiness after reboot")
    admin_ready = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    wait_for(lambda: "news.event.v1" in admin_ready.list_topics(timeout=5).topics,
             timeout=300, label="Kafka readiness after reboot")
    wait_idle(output, timeout=1200)
    expected = set(manifest)
    result = {"run": run, "input_articles": len(expected), "versions": {}}
    for variant in VARIANTS:
        rows = query(variant, """SELECT a.id,a.original_title,a.summary,a.summary_embedding,a.theme_id,a.url,i.name
            FROM article a LEFT JOIN issue i ON a.issue_id=i.id WHERE a.id=ANY(%s) OR a.original_title LIKE %s""",
            (list(expected), run + " article %"))
        actual = {row[0] for row in rows}
        errors = []
        for key, title, summary, vector_text, theme, url, issue in rows:
            target = manifest.get(key)
            vector = json.loads(vector_text) if vector_text else []
            if (not target or title != target["title"] or summary != target["summary"].strip()
                    or url != target["url"] or theme != 900001 or issue != "lab-seed-issue"
                    or len(vector) != 768 or any(abs(a-b) > 1e-6 for a,b in zip(vector, embedding(key)))):
                errors.append(key)
        result["versions"][variant] = {"persisted": len(expected & actual), "missing_ids": sorted(expected - actual),
            "unexpected_ids": sorted(actual - expected), "content_or_assignment_errors": errors}
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    futures = admin.list_offsets({TopicPartition("news.raw.v1", index): OffsetSpec.latest() for index in range(6)})
    ends = {key.partition: future.result(10).offset for key, future in futures.items()}
    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "host-oracle-" + run,
                         "enable.auto.commit": False})
    consumer.assign([TopicPartition("news.raw.v1", index, 0) for index in range(6)])
    copies = Counter()
    hash_errors = []
    try:
        deadline = time.monotonic() + 180
        with (output / "raw/raw-kafka-oracle.jsonl").open("w") as stream:
            while time.monotonic() < deadline:
                message = consumer.poll(0.25)
                if message and not message.error():
                    value = json.loads(message.value())
                    key = value.get("article_id")
                    if key in expected:
                        copies[key] += 1
                        digest = hashlib.sha256(value["body"].encode()).hexdigest()
                        if digest != manifest[key]["body_sha256"]:
                            hash_errors.append(key)
                        stream.write(json.dumps({"article_id": key, "partition": message.partition(),
                            "offset": message.offset(), "body_sha256": digest}) + "\n")
                positions = consumer.position([TopicPartition("news.raw.v1", index) for index in range(6)])
                if all(item.offset >= ends[item.partition] for item in positions):
                    break
            else:
                raise TimeoutError("Independent Kafka observer did not reach the frozen end offsets")
    finally:
        consumer.close()
    result["raw_unique_articles"] = len(copies)
    result["raw_records"] = sum(copies.values())
    result["raw_body_hash_errors"] = hash_errors
    result["verified_at_utc"] = datetime.now(timezone.utc).isoformat()
    result["passed"] = len(copies) == len(expected) and not hash_errors and all(
        not row["missing_ids"] and not row["unexpected_ids"] and not row["content_or_assignment_errors"]
        for row in result["versions"].values())
    (output / "summary.json").write_text(json.dumps(result, indent=2))
    compose("stop", "-t", "3", "collector-case")
    if not result["passed"]:
        raise RuntimeError("Host restart data oracle failed: " + run)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["prepare", "verify"])
    parser.add_argument("run")
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args()
    prepare(args.run, args.count) if args.phase == "prepare" else verify(args.run)
