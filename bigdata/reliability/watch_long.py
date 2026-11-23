"""Observe real production-duration alerting and restore the blocked TCP path."""
import json
from pathlib import Path
import time
import requests
from lab_settings import RECEIVER_URL, PROXY_URL

LAB = Path(__file__).resolve().parent
STATE = LAB / ".generated/state/long-network"
fault = json.loads((STATE / "fault.json").read_text())
report = {"scenario": "production-cadence-network-outage", "count": 100, "fault": fault,
          "poll_interval_seconds": 600, "scrape_interval_seconds": 15,
          "alert_for_seconds": 120, "alert_group_wait_seconds": 30}
restored = False
deadline = time.monotonic() + 1800
last_notice = 0
while time.monotonic() < deadline:
    try:
        lines = requests.get(RECEIVER_URL + "/", timeout=3).text.splitlines()
        matches = []
        for line in lines:
            notification = json.loads(line)
            for alert in notification["payload"].get("alerts", []):
                if (alert["labels"].get("alertname") == "CollectorDeliveryStalled"
                        and alert["labels"].get("instance") == "collector-long:9103"
                        and alert.get("status") == "firing"):
                    matches.append(notification)
        if matches and not restored:
            report["notification"] = matches[0]
            report["notification_observed_monotonic_ns"] = time.monotonic_ns()
            report["fault_to_notification_observed_seconds"] = (
                time.monotonic_ns() - fault["fault_monotonic_ns"]) / 1e9
            print("REAL_WEBHOOK_RECEIVED", report["fault_to_notification_observed_seconds"], flush=True)
            requests.post(PROXY_URL + "/proxies/isolated", json={"enabled": True}, timeout=5).raise_for_status()
            report["restored_monotonic_ns"] = time.monotonic_ns()
            restored = True
        if restored:
            delivery = json.loads((STATE / "delivery.json").read_text())
            if delivery.get("outbox", {}).get("pending") == 0:
                report["drained_monotonic_ns"] = time.monotonic_ns()
                report["restore_to_outbox_drained_seconds"] = (
                    report["drained_monotonic_ns"] - report["restored_monotonic_ns"]) / 1e9
                report["outbox_drained"] = True
                break
        if time.monotonic() - last_notice > 60:
            print("WATCHING", round((time.monotonic_ns() - fault["fault_monotonic_ns"]) / 1e9),
                  "seconds; restored=", restored, flush=True)
            last_notice = time.monotonic()
    except Exception as exception:
        print(type(exception).__name__, str(exception)[:120], flush=True)
    time.sleep(1)
else:
    report["error"] = "Timed out before notification/recovery"
(STATE / "observation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2), flush=True)
