"""Stable IDs for retries that receive a different Kafka offset or event time."""
import hashlib
import json


def event_id(event_type, payload):
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":"), allow_nan=False)
    return event_type + ":" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
