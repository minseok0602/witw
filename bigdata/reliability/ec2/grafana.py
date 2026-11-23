"""Add an observation-only Grafana to the existing, owned EC2 lab app network.

Run on the app host. No AWS credentials, public ports, or pipeline restarts.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import secrets
import subprocess
import time

import requests

LAB = Path(__file__).resolve().parents[1]
MONITORING = LAB.parent / "monitoring"
GENERATED = LAB / ".generated/ec2/grafana"
BASE = "http://127.0.0.1:39903"


def docker(*args):
    return subprocess.check_output(["/usr/bin/docker", *args], text=True)


def start(inventory):
    network = inventory["project"] + "_lab"
    actual = json.loads(docker("network", "inspect", network))[0]
    if actual.get("Labels", {}).get("com.docker.compose.project") != inventory["project"]:
        raise RuntimeError("Refusing to attach to an unowned experiment network")
    GENERATED.mkdir(parents=True, exist_ok=True)
    credentials = GENERATED / "credentials.json"
    if not credentials.exists():
        credentials.touch(mode=0o600)
        credentials.write_text(json.dumps({"user": "admin", "password": secrets.token_urlsafe(32)}))
    auth = json.loads(credentials.read_text())
    environment = GENERATED / "grafana.env"
    environment.touch(mode=0o600)
    environment.write_text("GF_SECURITY_ADMIN_USER=" + auth["user"] + "\n"
                           "GF_SECURITY_ADMIN_PASSWORD=" + auth["password"] + "\n")
    config = {
        "name": inventory["project"] + "-grafana",
        "services": {"grafana": {
            "image": "grafana/grafana:13.2.1", "restart": "always",
            "cpus": 0.25, "mem_limit": "384m", "ports": ["127.0.0.1:39903:3000"],
            "env_file": [str(environment)],
            "environment": {"GF_USERS_ALLOW_SIGN_UP": "false", "GF_AUTH_ANONYMOUS_ENABLED": "false",
                            "GF_ANALYTICS_REPORTING_ENABLED": "false", "GF_ANALYTICS_CHECK_FOR_UPDATES": "false"},
            "volumes": [str(MONITORING / "grafana/provisioning") + ":/etc/grafana/provisioning:ro",
                        str(MONITORING / "grafana/dashboards") + ":/var/lib/grafana/dashboards:ro",
                        "grafana-data:/var/lib/grafana"], "networks": ["lab"]}},
        "networks": {"lab": {"external": True, "name": network}},
        "volumes": {"grafana-data": {"labels": {"LabRun": inventory["run_id"]}}},
    }
    path = GENERATED / "compose.json"
    path.write_text(json.dumps(config, indent=2))
    print(docker("compose", "-f", str(path), "up", "-d"), flush=True)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            if requests.get(BASE + "/api/health", timeout=3).json().get("database") == "ok":
                return
        except (requests.RequestException, ValueError):
            pass
        time.sleep(3)
    raise TimeoutError("Grafana did not become ready")


def verify(inventory):
    auth = json.loads((GENERATED / "credentials.json").read_text())
    session = requests.Session()
    session.auth = (auth["user"], auth["password"])

    def get(path, **params):
        response = session.get(BASE + path, params=params, timeout=20)
        response.raise_for_status()
        return response.json()

    # Alertmanager is a frontend/proxy data source: the backend plugin health
    # endpoint is unsupported. Check the same API that its UI uses instead.
    status = get("/api/datasources/proxy/uid/alertmanager-bigdata/api/v2/status")
    get("/api/datasources/proxy/uid/alertmanager-bigdata/api/v2/alerts")
    health = {"prometheus-bigdata": get("/api/datasources/uid/prometheus-bigdata/health"),
              "alertmanager-bigdata": {"status": "OK", "check": "Grafana proxy: /api/v2/status and /api/v2/alerts",
                                       "version": status.get("versionInfo", {}).get("version")}}
    if any(item.get("status") != "OK" for item in health.values()):
        raise RuntimeError("Grafana data source health check failed: " + json.dumps(health))
    dashboard = get("/api/dashboards/uid/witw-pipeline-reliability")["dashboard"]
    checks = []
    proxy = "/api/datasources/proxy/uid/prometheus-bigdata"
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            result = get(proxy + "/api/v1/query", query=target["expr"])
            if result.get("status") != "success":
                raise RuntimeError("Dashboard query failed: " + target["expr"])
            checks.append({"panel_id": panel["id"], "query": target["expr"],
                           "series_count": len(result["data"]["result"])})
    # This is the Grafana Alerting UI endpoint, in addition to panel proxy queries.
    rules = get("/api/prometheus/prometheus-bigdata/api/v1/rules")
    if rules.get("status") != "success" or not rules["data"]["groups"]:
        raise RuntimeError("Grafana Alerting could not load Prometheus rules")
    current = get(proxy + "/api/v1/alerts")
    app = inventory["nodes"]["app"]["private_ip"]
    response = requests.get(f"http://{app}:39904/", timeout=10)
    response.raise_for_status()
    notifications = [json.loads(line) for line in response.text.splitlines() if line]
    relevant = [{"received_epoch_ns": record["received_epoch_ns"], "status": alert["status"],
                 "startsAt": alert.get("startsAt"), "endsAt": alert.get("endsAt")}
                for record in notifications for alert in record["payload"].get("alerts", [])
                if alert.get("labels", {}).get("alertname") == "CollectorDeliveryStalled"]
    record = {"captured_at_utc": datetime.now(timezone.utc).isoformat(), "run_id": inventory["run_id"],
              "scope": "Grafana HTTP API, production alert rules, real lab HTTP receiver; no external channel",
              "dashboard_uid": dashboard["uid"], "datasource_health": health, "queries": checks,
              "prometheus_rule_groups": len(rules["data"]["groups"]),
              "current_alerts": current["data"]["alerts"], "collector_delivery_notifications": relevant,
              "image_id": docker("image", "inspect", "grafana/grafana:13.2.1", "--format", "{{.Id}}").strip()}
    # Keep auxiliary observations outside results/: the live suite identifies
    # each new experimental result directory before and after a trial phase.
    output = LAB / ".generated/ec2/observability"
    output.mkdir(parents=True, exist_ok=True)
    path = output / (datetime.now(timezone.utc).strftime("grafana-%Y%m%dT%H%M%SZ.json"))
    path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    (output / "grafana-dashboard.json").write_text(json.dumps(dashboard, indent=2), encoding="utf-8")
    print(json.dumps({"evidence": str(path), "queries_passed": len(checks),
                      "datasource_health": health, "collector_delivery_notifications": relevant}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["start", "verify"])
    args = parser.parse_args()
    inventory = json.loads((LAB / ".generated/ec2/inventory.json").read_text())
    if args.operation == "start":
        start(inventory)
    verify(inventory)
    if args.operation == "verify":
        from export_monitoring import export_history
        export_history(inventory)
