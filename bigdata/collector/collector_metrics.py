"""Read-only Prometheus endpoint for the durable delivery queue."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import time


def render_metrics(snapshot, started_at=0):
    metrics = {
        "outbox_pending": ("gauge", snapshot["pending"]),
        "outbox_oldest_age_seconds": ("gauge", snapshot["oldest_age_seconds"]),
        "delivery_acknowledged_total": ("counter", snapshot["acknowledged"]),
        "delivery_failures_total": ("counter", snapshot["failures"]),
        "last_round_timestamp_seconds": ("gauge", snapshot["last_round_timestamp"]),
        "started_timestamp_seconds": ("gauge", started_at),
    }
    return "".join(f"# TYPE witw_collector_{name} {kind}\nwitw_collector_{name} {value}\n"
                   for name, (kind, value) in metrics.items())


def start_metrics_server(db, port):
    if port == 0:
        return None
    started_at = time.time()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            try:
                body = render_metrics(db.delivery_snapshot(), started_at).encode("utf-8")
            except Exception:
                self.send_error(503, "Collector state unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    Thread(target=server.serve_forever, daemon=True).start()
    return server
