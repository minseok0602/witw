"""Independent ID/content oracle; run after run_experiments.py has completed."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys

from fixtures import embedding
from run_experiments import LAB, VARIANTS, connect


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(output):
    summary = json.loads((output / "summary.json").read_text())
    expected = {}
    issue_names = {}
    direct = output / "raw/input-manifest.jsonl"
    for event in map(json.loads, direct.read_text().splitlines()):
        payload = event["payload"]
        if event["event_type"] == "ARTICLE_UPSERT":
            expected[payload["article_id"]] = {"title": payload["original_title"],
                "summary": payload["summary"].strip(), "embedding": payload["summary_embedding"],
                "theme": payload["theme_id"], "url": payload["url"], "country": payload["publisher_country_code"],
                "run": payload["article_id"].rsplit("-a", 1)[0]}
        elif event["event_type"] == "ISSUE_CREATE":
            for key in payload["article_ids"]:
                issue_names[key] = payload["name"]

    manifests = []
    for row in summary["results"]:
        if "collector_version" not in row:
            continue
        source = LAB / ".generated/state" / row["run"] / "inputs.jsonl"
        destination = output / "raw" / (row["run"] + "-inputs.jsonl")
        shutil.copy2(source, destination)
        for name in ("hit.json", "delivery.json", "staged.json"):
            marker = source.parent / name
            if marker.exists():
                shutil.copy2(marker, output / "raw" / (row["run"] + "-" + name))
        manifests.append({"file": destination.name, "sha256": digest(destination)})
        for item in map(json.loads, destination.read_text().splitlines()):
            expected[item["article_id"]] = {"title": item["title"], "summary": item["summary"].strip(),
                "embedding": embedding(item["article_id"]), "theme": 900001,
                "body_sha256": item["body_sha256"], "url": item["url"], "country": "US", "run": row["run"]}
            issue_names[item["article_id"]] = "lab-seed-issue"

    # Dedicated long outage is kept separate from the repeated fault matrix.
    long_source = LAB / ".generated/state/long-network"
    if (long_source / "observation.json").exists():
        long_output = output / "raw/long-network"
        long_output.mkdir(exist_ok=True)
        for name in ("fault.json", "observation.json", "delivery.json"):
            shutil.copy2(long_source / name, long_output / name)
        if (long_source / "inputs.jsonl").exists():
            shutil.copy2(long_source / "inputs.jsonl", long_output / "inputs.jsonl")
            provenance = {"origin": "Manifest frozen before sending"}
        else:
            # The long watcher began before manifest freezing was added. Do not
            # misrepresent this reconstructed oracle as a pre-registered manifest.
            count = json.loads((long_source / "staged.json").read_text())["count"]
            with (long_output / "inputs.jsonl").open("w", encoding="utf-8") as stream:
                for index in range(count):
                    url = f"https://example.com/long-network/{index}"
                    body = f"long-network unique article number {index}. " + "This is fixed factual source text for delivery testing. " * (15 + index % 5 * 25)
                    stream.write(json.dumps({"article_id": hashlib.sha256(url.encode()).hexdigest(), "url": url,
                        "title": f"long-network article {index}", "summary": body[:300],
                        "body_sha256": hashlib.sha256(body.encode()).hexdigest()}) + "\n")
            provenance = {"origin": "Reconstructed after the run from known count/run/input-generation rule; not pre-registered",
                          "count_source": "long-network/staged.json", "corroboration": "independent Kafka body hash and final DB oracle"}
        (long_output / "manifest-provenance.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")
        timing = json.loads((long_source / "observation.json").read_text())
        notifications = LAB / ".generated/state/notification-observations.jsonl"
        deliveries = {}
        for item in map(json.loads, notifications.read_text().splitlines()):
            if item["preexisting_when_observer_started"]:
                continue
            for alert in item["notification"]["payload"].get("alerts", []):
                labels = alert.get("labels", {})
                if labels.get("alertname") == "CollectorDeliveryStalled" and labels.get("instance") == "collector-long:9103":
                    deliveries.setdefault(alert["status"], item)
        observed = {"observer_poll_seconds": 0.5,
                    "fault_to_first_firing_observed_seconds":
                        (deliveries["firing"]["observed_monotonic_ns"] - timing["fault"]["fault_monotonic_ns"]) / 1e9,
                    "outbox_drain_to_resolved_observed_seconds":
                        (deliveries["resolved"]["observed_monotonic_ns"] - timing["drained_monotonic_ns"]) / 1e9,
                    "first_firing": deliveries["firing"], "first_resolved": deliveries["resolved"]}
        (long_output / "notification-lifecycle.json").write_text(json.dumps(observed, indent=2), encoding="utf-8")
        for item in map(json.loads, (long_output / "inputs.jsonl").read_text().splitlines()):
            expected[item["article_id"]] = {"title": item["title"], "summary": item["summary"].strip(),
                "embedding": embedding(item["article_id"]), "theme": 900001,
                "body_sha256": item["body_sha256"], "url": item["url"], "country": "US", "run": "long-network"}
            issue_names[item["article_id"]] = "lab-seed-issue"

    raw_counts = Counter()
    raw_hash_errors = Counter()
    observations = output / "raw/kafka-observations.jsonl"
    for item in map(json.loads, observations.open(encoding="utf-8")):
        key = item["article_id"]
        if item["topic"] == "news.raw.v1" and key in expected:
            raw_counts[key] += 1
            if item["body_sha256"] != expected[key].get("body_sha256"):
                raw_hash_errors[expected[key]["run"]] += 1

    report = {"experiment_id": summary["experiment_id"], "verified_at_utc": datetime.now(timezone.utc).isoformat(),
              "input_unique_articles": len(expected),
              "collector_manifests": manifests, "versions": {}, "raw_body_hash_mismatches": dict(raw_hash_errors)}
    runs = sorted({item["run"] for item in expected.values()})
    with (output / "raw/independent-id-content-oracle.jsonl").open("w", encoding="utf-8") as evidence:
        for variant in VARIANTS:
            with connect(variant) as db:
                actual = {}
                unexpected = defaultdict(list)
                # Query by run title independently of the expected ID list, so extra IDs are visible.
                for run in runs:
                    rows = db.execute("""SELECT a.id,a.original_title,a.summary,a.summary_embedding,a.theme_id,i.name,a.url,a.publisher_country_code
                        FROM article a LEFT JOIN issue i ON a.issue_id=i.id
                        WHERE a.original_title LIKE %s""", (run + "%",)).fetchall()
                    for key, title, summary_text, vector, theme, issue, url, country in rows:
                        if key not in expected:
                            unexpected[run].append(key)
                        else:
                            actual[key] = (title, summary_text, json.loads(vector) if vector else [], theme, issue, url, country)
                grouped = {}
                for run in runs:
                    ids = {key for key, item in expected.items() if item["run"] == run}
                    missing = sorted(ids - set(actual))
                    wrong_content, wrong_issue = [], []
                    for key in sorted(ids & set(actual)):
                        item = expected[key]
                        title, text, vector, theme, issue, url, country = actual[key]
                        vector_error = (len(vector) != len(item["embedding"]) or
                            any(abs(left - right) > 1e-6 for left, right in zip(vector, item["embedding"])))
                        content_ok = (title == item["title"] and text == item["summary"] and
                                      theme == item["theme"] and url == item["url"] and country == item["country"] and not vector_error)
                        assignment_ok = issue == issue_names.get(key)
                        if not content_ok:
                            wrong_content.append(key)
                        if not assignment_ok:
                            wrong_issue.append(key)
                        evidence.write(json.dumps({"version": variant, "run": run, "article_id": key,
                            "content_correct": content_ok, "assignment_correct": assignment_ok,
                            "actual_issue": issue, "raw_copies_observed": raw_counts[key]}) + "\n")
                    grouped[run] = {"input": len(ids), "persisted": len(ids & set(actual)),
                        "missing_ids": missing, "unexpected_ids": unexpected[run],
                        "content_mismatch_ids": wrong_content, "assignment_mismatch_ids": wrong_issue}
                report["versions"][variant] = grouped
    (output / "verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"experiment_id": report["experiment_id"], "input_unique_articles": len(expected),
        "raw_body_hash_mismatches": dict(raw_hash_errors),
        "versions": {variant: {"missing": sum(len(row["missing_ids"]) for row in rows.values()),
            "unexpected": sum(len(row["unexpected_ids"]) for row in rows.values()),
            "content_errors": sum(len(row["content_mismatch_ids"]) for row in rows.values()),
            "assignment_errors": sum(len(row["assignment_mismatch_ids"]) for row in rows.values())}
            for variant, rows in report["versions"].items()}}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    verify(parser.parse_args().output.resolve())
