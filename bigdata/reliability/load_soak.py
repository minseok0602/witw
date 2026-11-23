"""Observe a real 10k post-fetch burst through Kafka, fixed NLP, Spark and both DBs."""
import csv
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time

LAB = Path(__file__).resolve().parent
sys.path.insert(0, str(LAB / ".generated/python-deps"))
from run_experiments import compose, connect, VARIANTS as ALL_VARIANTS, Observer, AdminClient, wait_for
from lab_settings import KAFKA_BOOTSTRAP
from load_fixtures import THEMES, CANDIDATES, THEME_FIRST, ISSUE_FIRST, issue_embedding, article_embedding, expected_issue
from wait_pipeline_idle import wait_idle


def percentile(values, fraction):
    values = sorted(values)
    index = (len(values) - 1) * fraction
    low = math.floor(index)
    return values[low] + (values[math.ceil(index)] - values[low]) * (index - low)


parser = argparse.ArgumentParser()
parser.add_argument("count", type=int, nargs="?", default=10000)
parser.add_argument("--variant", choices=["both", "before", "after"], default="both")
args = parser.parse_args()
VARIANTS = ALL_VARIANTS if args.variant == "both" else (args.variant,)
run = "soak-" + datetime.now(timezone.utc).strftime("%H%M%S")
count = args.count
state = LAB / ".generated/state" / run
output = LAB / "results" / run
(output / "raw").mkdir(parents=True)
indices = {hashlib.sha256(f"https://example.com/{run}/{index}".encode()).hexdigest(): index for index in range(count)}
expected = set(indices)
seen = {variant: {} for variant in VARIANTS}
connections = {variant: connect(variant) for variant in ALL_VARIANTS}
for db in connections.values():
    for theme in range(THEMES):
        db.execute("INSERT INTO theme(id,field_id,code,name,description) VALUES(%s,900001,%s,%s,'burst fixture') "
                   "ON CONFLICT(id) DO NOTHING", (THEME_FIRST + theme, f'BURST{theme}', f'Burst theme {theme}'))
        for candidate in range(CANDIDATES):
            db.execute("""INSERT INTO issue(id,theme_id,name,embedding,first_seen_at,created_at,updated_at,last_active_at)
                VALUES(%s,%s,%s,%s,LOCALTIMESTAMP,LOCALTIMESTAMP,LOCALTIMESTAMP,LOCALTIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET last_active_at=LOCALTIMESTAMP""",
                (ISSUE_FIRST + theme * CANDIDATES + candidate, THEME_FIRST + theme,
                 f'lab-burst-t{theme:02}-i{candidate:02}', json.dumps(issue_embedding(theme, candidate))))
    db.execute("SELECT setval(pg_get_serial_sequence('issue','id'),(SELECT MAX(id) FROM issue),true)")
observer = Observer(output)
inactive = None
if args.variant != "both":
    inactive = "after" if args.variant == "before" else "before"
    compose("stop", "-t", "10", "backend-" + inactive)
    compose("start", "backend-" + args.variant)
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    group = "db-writer-live-" + args.variant
    wait_for(lambda: admin.describe_consumer_groups([group])[group].result(10).members,
             timeout=180, label="isolated benchmark consumer group")
compose("up", "-d", "--no-deps", "--force-recreate", "nlp-replay")
env = {**os.environ, "LAB_RUN": run, "LAB_VARIANT": "after", "LAB_COUNT": str(count), "LAB_FAILPOINT": ""}
compose("up", "-d", "--no-deps", "--force-recreate", "collector-case", env=env)
start = time.monotonic()
last_print = 0
deadline = start + 1200
with (output / "raw/latencies.csv").open("w", encoding="utf-8", newline="") as stream:
    csv_output = csv.writer(stream)
    csv_output.writerow(["version", "article_id", "observed_discovery_to_final_assignment_seconds"])
    while time.monotonic() < deadline:
        for variant in VARIANTS:
            remaining = list(expected - set(seen[variant]))
            if not remaining:
                continue
            rows = connections[variant].execute("""
                SELECT id,issue_id,EXTRACT(EPOCH FROM clock_timestamp()-(crawled_at AT TIME ZONE 'Asia/Seoul'))
                FROM article WHERE id=ANY(%s) AND issue_id IS NOT NULL
                """, (remaining,)).fetchall()
            for article_id, issue_id, lag in rows:
                if issue_id != expected_issue(indices[article_id]):
                    continue
                seen[variant][article_id] = float(lag)
                csv_output.writerow([variant, article_id, float(lag)])
            stream.flush()
        if time.monotonic() - last_print >= 10:
            print("PROGRESS", run, {variant: len(values) for variant, values in seen.items()}, flush=True)
            last_print = time.monotonic()
        if all(len(values) == count for values in seen.values()):
            break
        time.sleep(0.5)
manifest = {row["article_id"]: row for row in map(json.loads, (state / "inputs.jsonl").read_text().splitlines())}
report = {"run": run, "input_articles": count, "sampling_interval_seconds": 0.5,
          "measurement": "PostgreSQL first observation of committed final assignment minus collector discovery time; includes queueing and source staging",
          "shared_host_paired_consumers": True, "themes": THEMES, "issue_candidates_per_theme": CANDIDATES,
          "candidate_dimension_comparisons": count * CANDIDATES * 768, "versions": {}}
report["shared_host_paired_consumers"] = args.variant == "both"
report["consumer_mode"] = args.variant
for variant in VARIANTS:
    values = list(seen[variant].values())
    rows = connections[variant].execute("""
        SELECT id,original_title,summary,summary_embedding,issue_id,theme_id,url,publisher_country_code
        FROM article WHERE original_title LIKE %s
        """, (run + " article %",)).fetchall()
    actual = {row[0] for row in rows}
    errors = 0
    for article_id, title, summary_text, vector_text, issue, theme, url, country in rows:
        index = indices.get(article_id)
        if index is None:
            errors += 1
            continue
        vector = json.loads(vector_text) if vector_text else []
        target = article_embedding(article_id, index % THEMES)
        errors += (title != manifest[article_id]["title"] or summary_text != manifest[article_id]["summary"].strip()
                   or len(vector) != 768 or any(abs(a-b) > 1e-6 for a,b in zip(vector, target))
                   or issue != expected_issue(index) or theme != THEME_FIRST + index % THEMES
                   or url != manifest[article_id]["url"] or country != "US")
    report["versions"][variant] = {
        "unique_persisted": len(actual), "missing_ids": len(expected - actual),
        "unexpected_ids": len(actual - expected), "payload_or_assignment_errors": errors,
        "p50_seconds": percentile(values, 0.50) if values else None,
        "p95_seconds": percentile(values, 0.95) if values else None,
        "p99_seconds": percentile(values, 0.99) if values else None,
        "last_arrival_seconds": max(values) if values else None,
        "batch_completion_articles_per_second": count / max(values) if len(values) == count else None,
    }
    connections[variant].close()
report["collector_delivery"] = json.loads((state / "delivery.json").read_text()) if (state / "delivery.json").exists() else None
report["input_manifest_sha256"] = hashlib.sha256((state / "inputs.jsonl").read_bytes()).hexdigest()
__import__("shutil").copy2(state / "inputs.jsonl", output / "raw/inputs.jsonl")
if inactive:
    connections[inactive].close()
    # This backfill starts only after every latency and content measurement above.
    compose("start", "backend-" + inactive)
wait_idle(output)
observer.close()
raw_count = sum(observer.raw[key] for key in expected)
report["raw_kafka_records"] = raw_count
report["raw_kafka_unique_articles"] = sum(observer.raw[key] > 0 for key in expected)
hash_errors = 0
raw_sizes, body_sizes = [], []
for record in map(json.loads, (output / "raw/kafka-observations.jsonl").open(encoding="utf-8")):
    if record["topic"] == "news.raw.v1" and record["article_id"] in expected:
        hash_errors += record["body_sha256"] != manifest[record["article_id"]]["body_sha256"]
        raw_sizes.append(record["value_size_bytes"])
        body_sizes.append(record["body_size_bytes"])
report["raw_body_hash_mismatches"] = hash_errors
report["raw_json_payload_bytes_total"] = sum(raw_sizes)
report["raw_json_payload_size_bytes"] = {"min": min(raw_sizes), "median": percentile(raw_sizes, 0.5),
                                          "p95": percentile(raw_sizes, 0.95), "max": max(raw_sizes)}
report["body_size_bytes"] = {"min": min(body_sizes), "max": max(body_sizes)}
(output / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
compose("stop", "-t", "3", "collector-case")
print(json.dumps(report, indent=2), flush=True)
