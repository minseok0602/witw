import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from test_collector import collector, make_article, make_source
from collector_metrics import render_metrics


class Broker:
    topic = "news.raw.v1"
    def __init__(self, error=None):
        self.error = error
        self.callbacks = []
        self.events = []

    def publish(self, event, on_delivery):
        self.events.append(event)
        self.callbacks.append(on_delivery)
        return True

    def flush_round(self):
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback(self.error)
        return {"delivered": len(callbacks) if self.error is None else 0,
                "failed": len(callbacks) if self.error else 0, "remaining": 0}


class DurableDeliveryTest(unittest.TestCase):
    def test_broker_failure_survives_restart_and_retries_without_refetching(self):
        with patch.object(sys, "argv", ["collector"]):
            args = collector.parse_args()
        article = make_article()
        article.published_at = collector.iso(collector.utc_now())
        source = make_source()
        body = "A detailed article sentence with meaningful reporting. " * 12
        fetched = {"success": True, "http_status": 200, "title": "Actual headline",
                   "body": body, "body_length": len(body), "elapsed_ms": 20, "error": None}
        poll = {"source": source, "articles": [], "rss_status": "HTTP_304",
                "sitemap_status": "", "latest_status": "", "elapsed_ms": 10, "error": ""}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "collector.db"
            db = collector.DB(path)
            try:
                db.enqueue_article(article)
                with patch.object(collector, "discover_source", return_value=poll), patch.object(
                    collector, "fetch_article", return_value=fetched
                ), contextlib.redirect_stdout(io.StringIO()):
                    collector.poll_round(1, [source], db, args, collector.utc_now(), Broker("unavailable"))
                self.assertEqual(db.delivery_snapshot()["pending"], 1)
                self.assertEqual(db.delivery_snapshot()["failures"], 1)
                self.assertEqual(db.delivery_snapshot()["acknowledged"], 0)
                self.assertIn("witw_collector_outbox_pending 1", render_metrics(db.delivery_snapshot()))
            finally:
                db.close()
            db = collector.DB(path)
            try:
                broker = Broker()
                collector.deliver_outbox(db, broker)
                self.assertEqual(broker.events[0]["body"], body)
                self.assertEqual(db.delivery_snapshot()["pending"], 0)
                self.assertEqual(db.delivery_snapshot()["acknowledged"], 1)
                self.assertFalse(db.enqueue_article(article))
            finally:
                db.close()

    def test_quota_and_payload_are_committed_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            db = collector.DB(Path(directory) / "collector.db")
            try:
                article = make_article()
                db.enqueue_article(article)
                with self.assertRaises(ValueError):
                    db.reserve_delivery(article, "headline", "body" * 50,
                                        collector.iso(collector.utc_now()), 100,
                                        delivery_event={"bad": float("nan")})
                self.assertEqual(db.conn.execute("SELECT COUNT(*) FROM accepted_title_hashes").fetchone()[0], 0)
                self.assertEqual(db.pending_snapshot()["count"], 1)
                self.assertEqual(db.delivery_snapshot()["pending"], 0)
            finally:
                db.close()

    def test_queued_is_not_acknowledged_and_failed_payload_is_not_pruned(self):
        with tempfile.TemporaryDirectory() as directory:
            db = collector.DB(Path(directory) / "collector.db")
            try:
                article = make_article()
                event = {"article_id": "a1", "body": "retained body"}
                db.reserve_delivery(article, "headline", "body" * 50,
                                    "2026-01-01T00:00:00Z", 100, delivery_event=event)
                broker = Broker()
                broker.flush_round = lambda: {"delivered": 0, "failed": 0, "remaining": 1}
                collector.deliver_outbox(db, broker)
                self.assertEqual(db.delivery_snapshot()["pending"], 1)
                db.prune_pending_queue([make_source()], 1, 1)
                self.assertEqual(json.loads(db.outbox_events()[0][1]), event)
                broker.callbacks[0](None)
                broker.callbacks[0](None)  # Duplicate callbacks cannot double count.
                self.assertEqual(db.delivery_snapshot()["acknowledged"], 1)
            finally:
                db.close()

    def test_producer_waits_for_ack_and_reports_immediate_errors(self):
        client = Mock()
        with tempfile.TemporaryDirectory() as directory, patch.object(collector, "Producer", return_value=client):
            producer = collector.KafkaRawProducer("kafka:9092", "news.raw.v1", Path(directory) / "failed.jsonl")
            done = Mock()
            self.assertTrue(producer.publish({"article_id": "a1"}, on_delivery=done))
            done.assert_not_called()
            self.assertFalse(producer.publish({"article_id": "a1"}, on_delivery=done))
            callback = client.produce.call_args.kwargs["callback"]
            callback(None, Mock())
            done.assert_called_once_with(None)
            client.produce.side_effect = RuntimeError("queue failed")
            done.reset_mock()
            self.assertFalse(producer.publish({"article_id": "a2"}, on_delivery=done))
            self.assertIsInstance(done.call_args.args[0], RuntimeError)

    def test_successful_delivery_drains_more_than_one_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            db = collector.DB(Path(directory) / "collector.db")
            try:
                db.conn.executemany("INSERT INTO kafka_outbox VALUES (?, ?, ?, NULL)", [
                    (f"url-{i}", json.dumps({"article_id": f"article-{i}"}),
                     collector.iso(collector.utc_now())) for i in range(1201)
                ])
                db.conn.commit()
                result = collector.deliver_outbox(db, Broker())
                self.assertEqual(result["delivered"], 1201)
                self.assertEqual(db.delivery_snapshot()["pending"], 0)
                self.assertEqual(db.delivery_snapshot()["acknowledged"], 1201)
            finally:
                db.close()
