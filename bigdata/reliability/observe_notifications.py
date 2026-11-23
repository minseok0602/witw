"""Timestamp the first host observation of every real local webhook delivery."""
import hashlib
import json
from pathlib import Path
import time
import requests
from lab_settings import RECEIVER_URL

STATE = Path(__file__).resolve().parent / ".generated/state"
seen = set()
start_epoch = time.time_ns()
with (STATE / "notification-observations.jsonl").open("a", encoding="utf-8") as output:
    while not (STATE / "stop-notification-observer").exists():
        try:
            lines = requests.get(RECEIVER_URL + "/", timeout=3).text.splitlines()
            for line in lines:
                key = hashlib.sha256(line.encode()).hexdigest()
                if key not in seen:
                    notification = json.loads(line)
                    record = {"observed_monotonic_ns": time.monotonic_ns(),
                              "observed_epoch_ns": time.time_ns(),
                              "preexisting_when_observer_started": notification["received_epoch_ns"] < start_epoch,
                              "notification": notification}
                    output.write(json.dumps(record) + "\n")
                    output.flush()
                    seen.add(key)
        except Exception as exception:
            print(type(exception).__name__, flush=True)
        time.sleep(0.5)
