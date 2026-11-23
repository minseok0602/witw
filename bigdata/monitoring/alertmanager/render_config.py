"""Write Alertmanager configuration without interpolating secrets into YAML."""
import json
import os
from pathlib import Path
from urllib.parse import urlsplit


def build_config(webhook_url):
    receiver = {"name": "pipeline"}
    if webhook_url:
        url = urlsplit(webhook_url)
        if url.scheme not in ("https", "http") or not url.netloc:
            raise ValueError("ALERT_WEBHOOK_URL must be an HTTP(S) URL")
        receiver["webhook_configs"] = [{"url": webhook_url, "send_resolved": True}]
    return {
        "route": {"receiver": "pipeline", "group_by": ["alertname", "cluster"],
                  "group_wait": "30s", "group_interval": "5m", "repeat_interval": "4h"},
        "receivers": [receiver],
    }


if __name__ == "__main__":
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    path = Path("/config/alertmanager.yml")
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(build_config(url), indent=2), encoding="utf-8")
    os.chown(temporary, 65534, 65534)
    temporary.chmod(0o600)
    temporary.replace(path)
    print("Alertmanager webhook configured" if url else
          "ALERT_WEBHOOK_URL is empty: alerts are visible in Alertmanager; external delivery is disabled")
