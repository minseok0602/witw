"""Aggregate qualified trials; keep instrumentation pilots visibly excluded."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics


def describe(values):
    return {"n": len(values), "min": min(values), "median": statistics.median(values), "max": max(values)}


def aggregate(paths):
    included, excluded = [], []
    for path in paths:
        data = json.loads((path / "summary.json").read_text())
        verification = json.loads((path / "verification.json").read_text()) if (path / "verification.json").exists() else None
        for row in data["results"]:
            if "collector_version" in row and row["failpoint"].get("pause_protocol") != "blocking-trace-v2":
                excluded.append({"run": row["run"], "reason": "Unverified namespace PID 1 self-SIGSTOP boundary"})
            else:
                if "collector_version" in row and verification:
                    row["initial_snapshot_versions"] = row["versions"]
                    final_versions = {}
                    for variant in ("before", "after"):
                        final = verification["versions"][variant][row["run"]]
                        mismatches = set(final["missing_ids"]) | set(final["content_mismatch_ids"]) | set(final["assignment_mismatch_ids"])
                        final_versions[variant] = {"persisted_articles": final["persisted"],
                            "missing_ids": len(final["missing_ids"]), "payload_or_assignment_mismatches": len(mismatches)}
                    row["versions"] = final_versions
                included.append(row)
    groups = defaultdict(list)
    for row in included:
        key = row["scenario"] + (":" + row["collector_version"] if "collector_version" in row else "")
        groups[key].append(row)
    report = {"result_directories": [path.name for path in paths], "qualified_trials": len(included),
              "excluded_trials": excluded,
              "collector_final_counts_source": "Independent verification.json after all-stage pipeline drain; summary.json preserves original bounded per-trial snapshots",
              "groups": {}}
    for key, rows in groups.items():
        item = {"trials": len(rows), "runs": [row["run"] for row in rows]}
        if "input_articles" in rows[0]:
            item["input_articles"] = sum(row["input_articles"] for row in rows)
        if "collector_version" in rows[0]:
            item.update({"unique_kafka_articles": sum(row["actual_kafka_unique_articles"] for row in rows),
                         "kafka_records": sum(row["actual_kafka_raw_records"] for row in rows),
                         "versions": {variant: {
                             "persisted_articles": sum(row["versions"][variant]["persisted_articles"] for row in rows),
                             "missing_ids": sum(row["versions"][variant]["missing_ids"] for row in rows),
                             "payload_or_assignment_mismatches": sum(row["versions"][variant]["payload_or_assignment_mismatches"] for row in rows)}
                             for variant in ("before", "after")},
                         "fault_to_initial_snapshot_seconds": describe([row["fault_to_observed_final_seconds"] for row in rows]),
                         "initial_snapshots_with_mismatches": [row["run"] for row in rows
                             if any(item["payload_or_assignment_mismatches"] for item in row.get("initial_snapshot_versions", row["versions"]).values())]})
        elif "committed_issue_writes" in rows[0]:
            item["committed_issue_writes"] = {variant: sum(row["committed_issue_writes"][variant] for row in rows)
                                               for variant in ("before", "after")}
            item["restart_to_replay_seconds"] = {variant: describe([row["restart_to_replay_committed_seconds"][variant] for row in rows])
                                                  for variant in ("before", "after")}
        else:
            fields = rows[0]["versions"]["after"].keys()
            item["versions"] = {variant: {field: sum(row["versions"][variant][field] for row in rows) for field in fields}
                                for variant in ("before", "after")}
        if "dlt_records" in rows[0]:
            item["dlt_records"] = sum(row["dlt_records"] for row in rows)
        if "restore_to_drained_seconds" in rows[0]:
            item["restore_to_drained_seconds"] = describe([row["restore_to_drained_seconds"] for row in rows])
            item["fault_duration_seconds"] = describe([row["fault_duration_seconds"] for row in rows])
        report["groups"][key] = item
    (paths[0] / "aggregate.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", type=Path, nargs="+")
    aggregate(parser.parse_args().paths)
