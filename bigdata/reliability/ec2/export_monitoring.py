"""Preserve monitoring samples through read-only APIs before deleting the lab.

This exports original sample timestamps, not a native Prometheus TSDB backup.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

import requests

LAB = Path(__file__).resolve().parents[1]


def export_history(inventory):
    pieces = inventory["run_id"].split("-")
    start = datetime.strptime("-".join(pieces[2:4]), "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
    end = datetime.now(timezone.utc)
    output = LAB / ".generated/ec2/observability/prometheus-history" / end.strftime("%Y%m%dT%H%M%SZ")
    output.mkdir(parents=True, exist_ok=False)
    base = "http://" + inventory["nodes"]["app"]["private_ip"] + ":39900"
    selectors = {
        "scrape-availability": "up",
        "alerts": "ALERTS",
        "pipeline": '{__name__=~"witw_.*|witw:.*"}',
        "kafka-and-notifications": '{__name__=~"alertmanager_notifications.*|kafka_consumergroup_.*|kafka_topic_partition_(current|oldest)_offset"}',
    }
    window = max(1, math.ceil((end - start).total_seconds()))
    manifest = {"run_id": inventory["run_id"], "start_utc": start.isoformat(), "end_utc": end.isoformat(),
                "source_scrape_interval_seconds": 15, "resampling": "none",
                "scope": "Selected original samples from an instant query with a range selector; original timestamps preserve scrape gaps. Not a TSDB backup.",
                "files": {}}
    for name, selector in selectors.items():
        # A range-selector instant query returns actual scrape timestamps.
        # query_range would resample and carry values across short scrape gaps.
        response = requests.get(base + "/api/v1/query", params={"query": selector + f"[{window}s]",
            "time": end.timestamp()}, timeout=60)
        response.raise_for_status()
        result = response.json()
        if result.get("status") != "success" or result["data"]["resultType"] != "matrix":
            raise RuntimeError("Prometheus history export failed: " + name)
        path = output / (name + ".json")
        path.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
        manifest["files"][path.name] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                      "selector": selector, "series": len(result["data"]["result"]),
                                      "samples": sum(len(series["values"]) for series in result["data"]["result"])}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps({"monitoring_history_export": str(output), "files": manifest["files"]}), flush=True)
    return output


if __name__ == "__main__":
    export_history(json.loads((LAB / ".generated/ec2/inventory.json").read_text()))
