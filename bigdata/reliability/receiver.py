"""Isolated local notification receiver; never forwards to external services."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import threading
import time

PATH = Path("/state/notifications.jsonl")
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        record = {"received_epoch_ns": time.time_ns(), "payload": data}
        with LOCK, PATH.open("a", encoding="utf-8") as output:
            output.write(json.dumps(record) + "\n")
            output.flush()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_GET(self):
        with LOCK:
            body = PATH.read_bytes() if PATH.exists() else b""
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
