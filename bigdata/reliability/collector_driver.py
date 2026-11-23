"""Fixed post-fetch inputs, real production reservation/producer/SQLite code.

sys.settrace pauses only at crash boundaries; it never replaces Kafka callbacks.
"""
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import time

sys.path.insert(0, "/collector")
VARIANT = os.getenv("LAB_VARIANT", "after")
RUN = os.getenv("LAB_RUN", "case")
COUNT = int(os.getenv("LAB_COUNT", "1000"))
FAILPOINT = os.getenv("LAB_FAILPOINT", "")
STATE = Path("/state") / RUN
STATE.mkdir(parents=True, exist_ok=True)
TRACE_FIRED = (STATE / "hit.json").exists()
source = Path("/collector/collection_kafka_producer_v2.py") if VARIANT == "after" else Path("/lab/.generated/collector-before.py")
spec = importlib.util.spec_from_file_location("lab_collector", source)
c = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = c
spec.loader.exec_module(c)


def body_for(index):
    return f"{RUN} unique article number {index}. " + "This is fixed factual source text for delivery testing. " * (15 + index % 5 * 25)


# Freeze the oracle before any Kafka delivery or crash injection.
input_manifest = STATE / "inputs.jsonl"
if not input_manifest.exists():
    with input_manifest.open("w", encoding="utf-8") as output:
        for index in range(COUNT):
            url = f"https://example.com/{RUN}/{index}"
            body = body_for(index)
            output.write(json.dumps({"article_id": c.make_article_id(url), "url": url,
                                     "title": f"{RUN} article {index}",
                                     "body_sha256": __import__("hashlib").sha256(body.encode()).hexdigest(),
                                     "summary": body[:300]}) + "\n")
        output.flush()
        os.fsync(output.fileno())
database_root = Path(os.environ["LAB_DB_ROOT"]) / RUN if "LAB_DB_ROOT" in os.environ else STATE
database_root.mkdir(parents=True, exist_ok=True)
db = c.DB(database_root / "collector.db")
producer = c.KafkaRawProducer(os.environ["KAFKA_BOOTSTRAP_SERVERS"], "news.raw.v1", STATE / "failed.jsonl")
from collector_metrics import start_metrics_server
if VARIANT == "after":
    metrics = start_metrics_server(db, 9103)


def marker(name, details):
    path = STATE / (name + ".json")
    with path.open("w", encoding="utf-8") as output:
        json.dump({"run": RUN, "epoch_ns": time.time_ns(), **details}, output)
        output.flush()
        os.fsync(output.fileno())


def trace(frame, event, arg):
    global TRACE_FIRED
    if frame.f_code.co_filename != str(source) or not FAILPOINT or TRACE_FIRED:
        return None
    name = frame.f_code.co_name
    if name not in ("reserve_delivery", "record_delivery", "_delivery_callback"):
        return None
    hit = False
    if FAILPOINT == "outbox_commit" and name == "reserve_delivery" and event == "return" and arg == (True, ""):
        hit = frame.f_locals["article"].normalized_url.endswith(f"/{COUNT - 1}")
    if FAILPOINT == "ack_before_delete":
        if VARIANT == "after" and name == "record_delivery" and event == "call":
            hit = frame.f_locals.get("error") is None
        if VARIANT == "before" and name == "_delivery_callback" and event == "call":
            hit = frame.f_locals.get("err") is None
    if hit:
        TRACE_FIRED = True
        details = {"point": FAILPOINT, "function": name, "variant": VARIANT,
                   "pause_protocol": "blocking-trace-v2"}
        if VARIANT == "after":
            details["outbox"] = db.delivery_snapshot()
        marker("hit", details)
        print("FAILPOINT", json.dumps(details), flush=True)
        # Namespace PID 1 can ignore self-SIGSTOP. Block this Python frame until
        # the controller sends a real external SIGKILL; do not return to the caller.
        while True:
            if (STATE / "resume").exists():
                break
            time.sleep(1)
    return trace


sys.settrace(trace)
now = c.iso(c.utc_now())
for index in range(COUNT):
    article = c.Article(source_id="lab-source", source_name="example.com", country="United States",
                        url=f"https://example.com/{RUN}/{index}",
                        normalized_url=f"https://example.com/{RUN}/{index}", published_at=now,
                        discovery_type="FIXED_INPUT", endpoint_url="https://example.com/feed",
                        feed_title=f"{RUN} article {index}")
    if not db.enqueue_article(article):
        pending = db.conn.execute("SELECT 1 FROM pending_articles WHERE normalized_url=?",
                                  (article.normalized_url,)).fetchone()
        if not pending:
            continue
    body = body_for(index)
    fetched = {"title": f"{RUN} article {index}", "body": body, "body_length": len(body),
               "success": True, "http_status": 200, "elapsed_ms": 0, "error": None}
    event = c.build_raw_article_event(article, fetched, now)
    kwargs = {"delivery_event": event} if VARIANT == "after" else {}
    accepted, reason = db.reserve_delivery(article, fetched["title"], body, now, 0, **kwargs)
    if accepted and VARIANT == "before":
        producer.publish(event)
    db.save_article(article, fetched, now)
    db.complete_pending(article)
marker("staged", {"count": COUNT, "variant": VARIANT})
if os.getenv("LAB_GATE") == "true":
    while not (STATE / "release").exists():
        time.sleep(0.1)

poll = float(os.getenv("LAB_POLL_SECONDS", "600"))
while True:
    start = time.monotonic()
    result = c.deliver_outbox(db, producer) if VARIANT == "after" else producer.flush_round()
    if VARIANT == "after":
        db.set_meta("last_completed_round_at", c.iso(c.utc_now()))
    details = {"result": result, "variant": VARIANT}
    if VARIANT == "after":
        details["outbox"] = db.delivery_snapshot()
    marker("delivery", details)
    print("DELIVERY", json.dumps(details), flush=True)
    time.sleep(max(0.1, poll - (time.monotonic() - start)))
