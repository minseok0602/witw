"""Real broker and Spark interruptions; restart must preserve Kafka/checkpoints."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))
from run_experiments import (Experiments, compose, container, command, article_event, wait_articles,
                             snapshot, wait_for, query, VARIANTS, AdminClient)
from lab_settings import KAFKA_BOOTSTRAP
from wait_pipeline_idle import wait_idle


def broker_outage(experiment, repetition):
    run = f"ext-broker-r{repetition}-{experiment.id[-7:-1]}"
    events = [article_event(run, index) for index in range(experiment.count)]
    experiment.register(events)
    ids = [event["payload"]["article_id"] for event in events]
    compose("stop", "-t", "3", "kafka")
    errors = []
    def publish():
        try:
            experiment.publisher.send(events)
        except Exception as error:
            errors.append(repr(error))
    start = time.monotonic()
    sender = threading.Thread(target=publish)
    sender.start()
    time.sleep(30)
    compose("start", "kafka")
    # The measurement client must discover the new leader epoch after a real
    # broker restart. A stale AdminClient can keep returning FENCED_LEADER_EPOCH
    # even after both application consumers have committed every input record.
    experiment.admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    wait_for(lambda: all(partition.leader >= 0 for partition in
             experiment.admin.list_topics(timeout=5).topics["news.event.v1"].partitions.values()),
             timeout=120, label="fresh observer metadata after broker restart")
    sender.join(120)
    if sender.is_alive() or errors:
        raise RuntimeError("Broker outage producer: " + str(errors))
    wait_articles(ids, timeout=300)
    experiment.wait_idle()
    final = snapshot(ids)
    experiment.save({"scenario": "broker_unavailable_30s", "run": run, "input_articles": len(ids),
        "offset_observer": "new AdminClient and fresh partition leaders after broker restart",
        "fault_to_recovered_seconds": time.monotonic() - start,
        "versions": {variant: {"persisted_articles": len(rows), "missing_ids": len(set(ids) - set(rows))}
                     for variant, rows in final.items()}})


def spark_crash(experiment, repetition):
    run = f"ext-spark-r{repetition}-{experiment.id[-7:-1]}"
    ids = [hashlib.sha256(f"https://example.com/{run}/{index}".encode()).hexdigest() for index in range(experiment.count)]
    env = {**os.environ, "LAB_RUN": run, "LAB_VARIANT": "after", "LAB_COUNT": str(experiment.count), "LAB_FAILPOINT": ""}
    compose("up", "-d", "--no-deps", "--force-recreate", "collector-case", env=env)
    checkpoint = LAB / ".generated/state/spark-checkpoint"
    def checkpoint_lag():
        from run_experiments import TopicPartition, OffsetSpec
        batches = [int(path.name) for path in (checkpoint / "commits").iterdir() if path.name.isdigit()]
        batch = max(batches) if batches else -1
        offsets = json.loads((checkpoint / "offsets" / str(batch)).read_text().splitlines()[-1])["news.enriched.v1"] if batch >= 0 else {}
        latest = experiment.admin.list_offsets({TopicPartition("news.enriched.v1", index): OffsetSpec.latest() for index in range(6)})
        lag = sum(max(0, future.result(10).offset - offsets.get(str(key.partition), 0)) for key, future in latest.items())
        return {"committed_batch": batch, "committed_offsets": offsets, "uncommitted_input_records": lag} if lag else None
    # Witness committed articles while the input batch is not checkpoint-committed.
    wait_for(lambda: query("after", "SELECT COUNT(*) FROM article WHERE id=ANY(%s)", (ids,))[0][0] > 0
             and checkpoint_lag(), timeout=300, label="Spark partial output before checkpoint commit")
    boundary = checkpoint_lag()
    if not boundary:
        raise RuntimeError("Missed Spark crash boundary; do not count this trial as tested")
    start = time.monotonic()
    command(["docker", "kill", "--signal=KILL", container("spark")])
    compose("start", "spark")
    wait_idle(experiment.output, timeout=900)
    final = wait_articles(ids, {key: "lab-seed-issue" for key in ids}, timeout=300)
    compose("stop", "-t", "3", "collector-case")
    experiment.save({"scenario": "spark_partial_output_sigkill", "run": run,
        "collector_version": "after", "input_articles": len(ids), "checkpoint_at_failure": boundary,
        "fault_to_recovered_seconds": time.monotonic() - start,
        "versions": {variant: {"persisted_articles": len(rows), "missing_ids": len(set(ids) - set(rows)),
                               "incorrect_assignments": sum(rows.get(key, {}).get("issue") != "lab-seed-issue" for key in ids)}
                     for variant, rows in final.items()}})


if __name__ == "__main__":
    experiment = Experiments(3, 1000)
    print("OUTPUT", experiment.output, flush=True)
    try:
        for repetition in range(1, 4):
            broker_outage(experiment, repetition)
            spark_crash(experiment, repetition)
    finally:
        experiment.close()
