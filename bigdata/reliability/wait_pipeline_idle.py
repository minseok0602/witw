"""Fence final observations on every Kafka stage, including Spark checkpoint commit."""
import argparse
import json
from pathlib import Path
import time

from run_experiments import LAB, AdminClient, OffsetSpec, TopicPartition, ConsumerGroupTopicPartitions, query
from lab_settings import KAFKA_BOOTSTRAP, NLP_GROUP


def wait_idle(output, timeout=1800):
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    deadline = time.monotonic() + timeout
    checkpoint = LAB / ".generated/state/spark-checkpoint"
    stable = 0
    while time.monotonic() < deadline:
        ends = {}
        for topic, count in (("news.raw.v1", 6), ("news.enriched.v1", 6), ("news.event.v1", 3)):
            futures = admin.list_offsets({TopicPartition(topic, index): OffsetSpec.latest() for index in range(count)})
            ends[topic] = {str(key.partition): future.result(10).offset for key, future in futures.items()}
        positions = {}
        for group, topic in ((NLP_GROUP, "news.raw.v1"), ("db-writer-live-before", "news.event.v1"),
                             ("db-writer-live-after", "news.event.v1")):
            committed = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(10)
            positions[group] = {str(item.partition): max(0, item.offset) for item in committed.topic_partitions if item.topic == topic}
        batches = [int(path.name) for path in (checkpoint / "commits").iterdir() if path.name.isdigit()]
        batch = max(batches) if batches else None
        spark = {}
        if batch is not None:
            lines = (checkpoint / "offsets" / str(batch)).read_text().splitlines()
            spark = json.loads(lines[-1]).get("news.enriched.v1", {})
        positions["spark-committed-batch"] = spark
        pairs = [(NLP_GROUP, "news.raw.v1"), ("spark-committed-batch", "news.enriched.v1"),
                 ("db-writer-live-before", "news.event.v1"), ("db-writer-live-after", "news.event.v1")]
        lag = {name: sum(max(0, value - positions[name].get(partition, 0)) for partition, value in ends[topic].items())
               for name, topic in pairs}
        pending = query("after", "SELECT COUNT(*) FROM pipeline_event_inbox WHERE status='PENDING'")[0][0]
        stable = stable + 1 if not any(lag.values()) and pending == 0 else 0
        if stable >= 3:
            result = {"all_stages_drained": True, "latest_offsets": ends, "committed_positions": positions,
                      "lag": lag, "after_pending_inbox": pending, "spark_committed_batch": batch,
                      "three_consecutive_samples_seconds_apart": 2}
            (output / "pipeline-drained.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            print(json.dumps(result), flush=True)
            return result
        time.sleep(2)
    raise TimeoutError("Pipeline did not drain: " + json.dumps({"lag": lag, "pending": pending}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    wait_idle(args.output, args.timeout)
