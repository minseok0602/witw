"""Real Kafka/Spark/PostgreSQL fault experiments. Only the lab project is controlled."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import queue
import statistics
import subprocess
import sys
import threading
import time

LAB = Path(__file__).resolve().parent
ROOT = LAB.parents[1]
sys.path.insert(0, str(LAB / ".generated/python-deps"))
sys.path.insert(0, str(LAB))
import psycopg
from psycopg.types.json import Jsonb
import requests
from confluent_kafka import Consumer, Producer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from fixtures import BASE, embedding
from lab_settings import PROJECT, KAFKA_BOOTSTRAP, DB_HOST, DB_PORT, DB_PASSWORD

COMPOSE = ["docker", "compose", "-p", PROJECT, "-f", str(LAB / "compose.yml")]
VARIANTS = ("before", "after")
TOPIC = "news.event.v1"


def command(args, **kwargs):
    return subprocess.run(list(map(str, args)), check=True, text=True, capture_output=True, **kwargs)


def compose(*args, env=None):
    return command(COMPOSE + list(args), env=env)


def container(service):
    return compose("ps", "-a", "-q", service).stdout.strip()


def wait_for(function, timeout=180, label="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = function()
            if last:
                return last
        except Exception as exception:
            last = repr(exception)
        time.sleep(0.5)
    raise TimeoutError(f"{label}: {str(last)[:300]}")


def connect(variant):
    return psycopg.connect(host=DB_HOST, port=DB_PORT, dbname=f"witw_{variant}",
                           user="postgres", password=DB_PASSWORD, autocommit=True, connect_timeout=3)


def query(variant, sql, parameters=()):
    with connect(variant) as connection:
        return connection.execute(sql, parameters).fetchall()


def seed():
    for variant in VARIANTS:
        wait_for(lambda: query(variant, "SELECT COUNT(*) FROM pipeline_event_inbox"), label="Flyway " + variant)
        with connect(variant) as db:
            db.execute("INSERT INTO field(id,code,name,description) VALUES (900001,'LAB','Reliability lab','lab') "
                       "ON CONFLICT(id) DO NOTHING")
            db.execute("INSERT INTO theme(id,field_id,code,name,description) VALUES "
                       "(900001,900001,'LAB','Reliability lab','lab') ON CONFLICT(id) DO NOTHING")
            db.execute("INSERT INTO issue(id,theme_id,name,embedding,first_seen_at,created_at,updated_at,last_active_at) "
                       "VALUES(900001,900001,'lab-seed-issue',%s,LOCALTIMESTAMP,LOCALTIMESTAMP,LOCALTIMESTAMP,LOCALTIMESTAMP) "
                       "ON CONFLICT(id) DO NOTHING", (json.dumps(BASE),))
            db.execute("SELECT setval(pg_get_serial_sequence('issue','id'), "
                       "GREATEST((SELECT MAX(id) FROM issue),900001),true)")
            db.execute("CREATE TABLE IF NOT EXISTS lab_issue_writes(id BIGSERIAL PRIMARY KEY, "
                       "issue_name TEXT, operation TEXT, at TIMESTAMPTZ DEFAULT clock_timestamp())")
            db.execute("""CREATE OR REPLACE FUNCTION lab_record_issue_write() RETURNS trigger AS $$
                       BEGIN INSERT INTO lab_issue_writes(issue_name,operation) VALUES(NEW.name,TG_OP);
                       RETURN NEW; END; $$ LANGUAGE plpgsql""")
            db.execute("DROP TRIGGER IF EXISTS lab_issue_write ON issue")
            db.execute("CREATE TRIGGER lab_issue_write AFTER INSERT OR UPDATE ON issue "
                       "FOR EACH ROW EXECUTE FUNCTION lab_record_issue_write()")


class Observer:
    def __init__(self, output):
        self.consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "live-observer-" + output.name,
                                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
        self.consumer.subscribe(["news.raw.v1", "news.enriched.v1", TOPIC, TOPIC + ".dlt"])
        self.output = (output / "raw/kafka-observations.jsonl").open("a", encoding="utf-8")
        self.raw = Counter()
        self.dlt = []
        self.lock = threading.Lock()
        self.stop = False
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop:
            message = self.consumer.poll(0.25)
            if message is None or message.error():
                continue
            value = message.value()
            try:
                data = json.loads(value)
            except Exception:
                data = {}
            payload = data.get("payload", data)
            record = {"observed_monotonic_ns": time.monotonic_ns(), "topic": message.topic(),
                      "partition": message.partition(), "offset": message.offset(),
                      "event_id": data.get("event_id"), "article_id": payload.get("article_id"),
                      "value_sha256": hashlib.sha256(value).hexdigest(),
                      "value_size_bytes": len(value),
                      "body_size_bytes": len(data["body"].encode()) if "body" in data else None,
                      "body_sha256": hashlib.sha256(data["body"].encode()).hexdigest() if "body" in data else None,
                      "headers": {key: raw.decode(errors="replace") if raw else None for key, raw in message.headers() or []}}
            with self.lock:
                if message.topic() == "news.raw.v1":
                    self.raw[payload.get("article_id")] += 1
                if message.topic().endswith(".dlt"):
                    self.dlt.append({"record": record, "data": data})
            self.output.write(json.dumps(record) + "\n")
            self.output.flush()
        self.consumer.close()
        self.output.close()

    def dlt_for(self, run):
        with self.lock:
            return [entry for entry in self.dlt if run in json.dumps(entry["data"])]

    def close(self):
        self.stop = True
        self.thread.join(10)


class Publisher:
    def __init__(self):
        self.client = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all",
                                "enable.idempotence": True, "compression.type": "snappy"})
        self.errors = []
        self.acks = 0

    def send(self, events):
        metadata = []
        def delivered(error, message):
            if error:
                self.errors.append(str(error))
            else:
                self.acks += 1
                metadata.append({"partition": message.partition(), "offset": message.offset()})
        for event in events:
            payload = event["payload"]
            key = payload.get("article_id", event["event_id"])
            value = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
            while True:
                try:
                    self.client.produce(TOPIC, key=key, value=value, on_delivery=delivered)
                    break
                except BufferError:
                    self.client.poll(0.1)
            self.client.poll(0)
        if self.client.flush(60) or self.errors:
            raise RuntimeError("Real Kafka publish failed: " + str(self.errors))
        return metadata


def now():
    return datetime.now(timezone.utc).isoformat()


def article_event(run, index):
    article_id = f"{run}-a{index:05}"
    timestamp = now()
    return {"event_id": f"lab:{run}:article:{index}", "event_type": "ARTICLE_UPSERT", "occurred_at": timestamp,
            "payload": {"article_id": article_id, "publisher_country_code": "US", "theme_id": 900001,
                        "theme_relevance_score": 0.99, "publisher": "example.com",
                        "url": f"https://example.com/{run}/{index}", "original_title": f"{run} title {index}",
                        "translated_title": f"{run} title {index}", "summary": f"{run} summary {index}",
                        "summary_embedding": embedding(article_id), "crawled_at": timestamp,
                        "tone_score": 50.0, "published_at": timestamp, "mentioned_countries": []}}


def issue_event(run, ids, index):
    return {"event_id": f"lab:{run}:issue:{index}", "event_type": "ISSUE_CREATE", "occurred_at": now(),
            "payload": {"theme_id": 900001, "name": f"{run}-i{index:04}", "embedding": BASE,
                        "article_ids": ids, "first_seen_at": now(),
                        "article_similarities": {article_id: 0.99 for article_id in ids}}}


def snapshot(ids):
    return {variant: {row[0]: {"title": row[1], "issue": row[2], "theme": row[3], "summary": row[4],
                              "embedding_length": len(json.loads(row[5])) if row[5] else 0}
                      for row in query(variant, "SELECT a.id,a.original_title,i.name,a.theme_id,a.summary,a.summary_embedding "
                                       "FROM article a LEFT JOIN issue i ON i.id=a.issue_id WHERE a.id=ANY(%s)", (ids,))}
            for variant in VARIANTS}


def wait_articles(ids, expected_issues=None, timeout=180, require_before=True):
    last = {}
    def done():
        nonlocal last
        last = snapshot(ids)
        for variant in VARIANTS if require_before else ("after",):
            if len(last[variant]) != len(ids):
                return False
            if expected_issues and any(last[variant][key]["issue"] != name for key, name in expected_issues.items()):
                return False
        return True
    try:
        wait_for(done, timeout, "DB IDs/assignments")
    except TimeoutError:
        pass
    return last


class Experiments:
    def __init__(self, repetitions, count):
        self.id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.output = LAB / "results" / self.id
        (self.output / "raw").mkdir(parents=True)
        protocol = {"repetitions": repetitions, "articles_per_trial": count,
                    "collector_pause_protocol": "blocking-trace-v2", "collector_observation_limit_seconds": 600,
                    "db_failpoint_filters_exact_event_id": True, "db_recovery_fences_committed_offset": True,
                    "lab_source_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                          for path in sorted(LAB.glob("*.py"))}}
        (self.output / "protocol.json").write_text(json.dumps(protocol, indent=2), encoding="utf-8")
        self.publisher = Publisher()
        self.observer = Observer(self.output)
        self.repetitions = repetitions
        self.count = count
        self.results = []
        self.manifest = (self.output / "raw/input-manifest.jsonl").open("a", encoding="utf-8")
        self.admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

    def save(self, result):
        self.results.append(result)
        (self.output / "summary.json").write_text(json.dumps({"experiment_id": self.id, "results": self.results},
                                                            ensure_ascii=False, indent=2), encoding="utf-8")
        print("RESULT", json.dumps(result, ensure_ascii=False), flush=True)

    def register(self, events):
        for event in events:
            self.manifest.write(json.dumps(event, separators=(",", ":")) + "\n")
        self.manifest.flush()

    def audit(self, run):
        return {variant: query(variant, "SELECT COUNT(*) FROM lab_issue_writes WHERE issue_name LIKE %s",
                               (run + "-i%",))[0][0] for variant in VARIANTS}

    def group_ready(self):
        for variant in VARIANTS:
            group = self.admin.describe_consumer_groups(["db-writer-live-" + variant])
            if not group["db-writer-live-" + variant].result(5).members:
                return False
        return True

    def wait_idle(self):
        def idle():
            futures = self.admin.list_offsets({TopicPartition(TOPIC, index): OffsetSpec.latest() for index in range(3)})
            ends = {key.partition: future.result(5).offset for key, future in futures.items()}
            for variant in VARIANTS:
                group = "db-writer-live-" + variant
                offsets = self.admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(5)
                committed = {item.partition: max(0, item.offset) for item in offsets.topic_partitions if item.topic == TOPIC}
                if any(committed.get(index, 0) < offset for index, offset in ends.items()):
                    return False
            return True
        wait_for(idle, timeout=180, label="real committed Kafka offsets")

    def duplicate_and_order(self, repetition, reordered):
        run = f"r{repetition}-{'ord' if reordered else 'dup'}-{self.id[-7:-1]}"
        events = [article_event(run, index) for index in range(self.count)]
        ids = [event["payload"]["article_id"] for event in events]
        groups = [issue_event(run, ids[index:index + 10], index // 10) for index in range(0, len(ids), 10)]
        expected = {article_id: event["payload"]["name"] for event in groups for article_id in event["payload"]["article_ids"]}
        self.register(events + groups)
        start = time.monotonic()
        if reordered:
            self.publisher.send(groups)
            time.sleep(10)
            self.publisher.send(events)
            final = wait_articles(ids, expected, require_before=False)
            # Ensure the baseline had time to finish its non-retryable DLT writes.
            self.wait_idle()
            final = snapshot(ids)
        else:
            self.publisher.send(events)
            wait_articles(ids)
            self.publisher.send(groups)
            wait_articles(ids, expected)
            self.publisher.send(groups[:max(1, len(groups) // 5)])
            self.wait_idle()
            final = snapshot(ids)
        self.save({"scenario": "reordered_dependencies" if reordered else "duplicate_events", "run": run,
                   "input_articles": len(ids), "unique_issue_events": len(groups),
                   "duplicate_issue_events": 0 if reordered else max(1, len(groups) // 5),
                   "elapsed_seconds": time.monotonic() - start,
                   "versions": {variant: {"persisted_articles": len(final[variant]),
                       "missing_ids": len(set(ids) - set(final[variant])),
                       "incorrect_assignments": sum(final[variant].get(key, {}).get("issue") != name for key, name in expected.items()),
                       "committed_issue_writes": self.audit(run)[variant]} for variant in VARIANTS},
                   "dlt_records": len(self.observer.dlt_for(run))})

    def probe(self, variant, point, event_id):
        class_name = "DbWriterKafkaConsumer" if point == "postcommit" else "DbWriterService"
        method = "consume" if point == "postcommit" else "handle"
        name = PROJECT + "-probe-" + variant
        args = ["docker", "run", "--rm", "--name", name, "--network", PROJECT + "_lab",
                "--memory", "512m", "--mount", f"type=bind,source={LAB},target=/lab,readonly",
                "--entrypoint", "java", os.getenv("LAB_BUILD_IMAGE", "news-kafka-hdfs-writer-test:latest"), "--add-modules", "jdk.jdi",
                "/lab/EdgeProbe.java", "backend-" + variant, "com.ssafy.witw.infra.kafka.dbwriter." + class_name, method, event_id]
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        messages = queue.Queue()
        transcript = []
        def read():
            for line in process.stdout:
                transcript.append(line.rstrip())
                messages.put(line.rstrip())
        threading.Thread(target=read, daemon=True).start()
        def await_message(text):
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    line = messages.get(timeout=1)
                    if text in line:
                        return line
                except queue.Empty:
                    if process.poll() is not None:
                        raise RuntimeError("JDI probe failed: " + "\n".join(transcript))
            raise TimeoutError("JDI " + text + ": " + "\n".join(transcript))
        await_message("ARMED")
        return process, await_message, transcript

    def db_crash(self, repetition, point):
        run = f"r{repetition}-{point}-{self.id[-7:-1]}"
        article = article_event(run, 0)
        ids = [article["payload"]["article_id"]]
        issue = issue_event(run, ids, 0)
        self.register([article, issue])
        self.publisher.send([article])
        wait_articles(ids)
        self.wait_idle()
        probes = {variant: self.probe(variant, point, issue["event_id"]) for variant in VARIANTS}
        metadata = self.publisher.send([issue])
        for variant in VARIANTS:
            probes[variant][1]("HIT")
        before_kill = snapshot(ids)
        visible = {variant: before_kill[variant][ids[0]]["issue"] for variant in VARIANTS}
        offsets = {}
        for variant in VARIANTS:
            group = "db-writer-live-" + variant
            result = self.admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)]).get(group).result(5)
            offsets[variant] = [{"topic": item.topic, "partition": item.partition, "offset": item.offset}
                                for item in result.topic_partitions]
            committed = next((item["offset"] for item in offsets[variant]
                              if item["topic"] == TOPIC and item["partition"] == metadata[0]["partition"]), -1)
            if committed >= metadata[0]["offset"] + 1:
                raise AssertionError("Probe missed the Kafka pre-ack boundary")
            expected_visible = issue["payload"]["name"] if point == "postcommit" else None
            if visible[variant] != expected_visible:
                raise AssertionError("Probe missed the DB transaction boundary")
        fault = time.monotonic()
        for variant in VARIANTS:
            command(["docker", "kill", "--signal=KILL", container("backend-" + variant)])
        for variant in VARIANTS:
            probes[variant][0].wait(timeout=20)
            (self.output / "raw" / f"{run}-{variant}-jdi.log").write_text("\n".join(probes[variant][2]), encoding="utf-8")
        # Read the committed state while both application processes are dead.
        after_kill = snapshot(ids)
        compose("start", "backend-before", "backend-after")
        restarted = time.monotonic()
        replayed = {}
        def replay_complete():
            for variant in VARIANTS:
                if variant in replayed:
                    continue
                group = "db-writer-live-" + variant
                current = self.admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(5)
                if any(item.topic == TOPIC and item.partition == metadata[0]["partition"]
                       and item.offset >= metadata[0]["offset"] + 1 for item in current.topic_partitions):
                    replayed[variant] = time.monotonic() - restarted
            return len(replayed) == len(VARIANTS)
        wait_for(replay_complete, timeout=180, label="actual replay and offset commit after restart")
        self.wait_idle()
        expected = {ids[0]: issue["payload"]["name"]}
        final = wait_articles(ids, expected)
        self.save({"scenario": "db_" + point + "_sigkill", "run": run,
                   "kafka_record": metadata, "committed_offsets_at_failpoint": offsets,
                   "visible_before_sigkill": visible,
                   "visible_after_sigkill": {variant: after_kill[variant][ids[0]]["issue"] for variant in VARIANTS},
                   "fault_to_recovered_seconds": time.monotonic() - fault,
                   "restart_to_replay_committed_seconds": replayed,
                   "committed_issue_writes": self.audit(run),
                   "final_assignment": {variant: final[variant].get(ids[0], {}).get("issue") for variant in VARIANTS}})

    def collector_crash(self, repetition, variant, point):
        run = f"r{repetition}-c{variant[0]}-{point[:3]}-{self.id[-7:-1]}"
        ids = [hashlib.sha256(f"https://example.com/{run}/{index}".encode()).hexdigest() for index in range(self.count)]
        state = LAB / ".generated/state" / run
        env = {**os.environ, "LAB_RUN": run, "LAB_VARIANT": variant, "LAB_FAILPOINT": point,
               "LAB_COUNT": str(self.count)}
        compose("up", "-d", "--no-deps", "--force-recreate", "collector-case", env=env)
        wait_for(lambda: (state / "hit.json").exists(), timeout=90, label="Collector failpoint " + point)
        hit = json.loads((state / "hit.json").read_text())
        fault = time.monotonic()
        target = container("collector-case")
        command(["docker", "kill", "--signal=KILL", target])
        command(["docker", "start", target])
        wait_for(lambda: (state / "delivery.json").exists(), timeout=90, label="Collector restarted")
        time.sleep(2)
        with self.observer.lock:
            arrived = [key for key in ids if self.observer.raw[key] > 0]
        # Spark runs the actual 15-second production micro-batch cadence.
        expected_ids = ids if variant == "after" else arrived
        wait_articles(expected_ids, {key: "lab-seed-issue" for key in expected_ids}, timeout=600)
        self.wait_idle()
        final = snapshot(ids)
        compose("stop", "-t", "3", "collector-case")
        with self.observer.lock:
            raw_count = sum(self.observer.raw[key] for key in ids)
            raw_unique = sum(self.observer.raw[key] > 0 for key in ids)
        mismatches = {}
        for backend, rows in final.items():
            mismatches[backend] = sum(rows.get(key, {}).get("title") != f"{run} article {index}"
                                      or rows.get(key, {}).get("issue") != "lab-seed-issue"
                                      or rows.get(key, {}).get("embedding_length") != 768
                                      for index, key in enumerate(ids))
        self.save({"scenario": "collector_" + point + "_sigkill", "run": run, "collector_version": variant,
                   "input_articles": len(ids), "actual_kafka_raw_records": raw_count,
                   "actual_kafka_unique_articles": raw_unique,
                   "fault_to_observed_final_seconds": time.monotonic() - fault, "failpoint": hit,
                   "versions": {backend: {"persisted_articles": len(rows), "missing_ids": len(set(ids) - set(rows)),
                                          "payload_or_assignment_mismatches": mismatches[backend]}
                                for backend, rows in final.items()}})

    def database_outage(self, repetition):
        run = f"r{repetition}-dbnet-{self.id[-7:-1]}"
        events = [article_event(run, index) for index in range(self.count)]
        ids = [event["payload"]["article_id"] for event in events]
        self.register(events)
        fault = time.monotonic()
        # Pausing preserves DB sockets and reproduces an unresponsive dependency.
        command(["docker", "pause", container("postgres")])
        try:
            self.publisher.send(events)
            time.sleep(40)
        finally:
            command(["docker", "unpause", container("postgres")])
        restored = time.monotonic()
        final = wait_articles(ids, timeout=180)
        self.wait_idle()
        self.save({"scenario": "database_unresponsive_40s", "run": run, "input_articles": len(ids),
                   "fault_duration_seconds": restored - fault,
                   "restore_to_drained_seconds": time.monotonic() - restored,
                   "versions": {variant: {"persisted_articles": len(rows), "missing_ids": len(set(ids) - set(rows))}
                                for variant, rows in final.items()},
                   "dlt_records": len(self.observer.dlt_for(run))})

    def run(self, collector_only=False, start_repetition=1):
        wait_for(self.group_ready, label="initial consumer groups")
        self.wait_idle()
        seed()
        compose("up", "-d", "nlp-replay", "spark")
        for repetition in range(start_repetition, start_repetition + self.repetitions):
            if not collector_only:
                self.duplicate_and_order(repetition, False)
                self.duplicate_and_order(repetition, True)
                self.db_crash(repetition, "precommit")
                self.db_crash(repetition, "postcommit")
                self.database_outage(repetition)
            order = VARIANTS if repetition % 2 else tuple(reversed(VARIANTS))
            for variant in order:
                for point in ("outbox_commit", "ack_before_delete"):
                    self.collector_crash(repetition, variant, point)

    def close(self):
        self.observer.close()
        self.manifest.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--collector-only", action="store_true")
    parser.add_argument("--start-repetition", type=int, default=1)
    args = parser.parse_args()
    experiments = Experiments(args.repetitions, args.count)
    print("OUTPUT", experiments.output, flush=True)
    try:
        experiments.run(args.collector_only, args.start_repetition)
    finally:
        experiments.close()
