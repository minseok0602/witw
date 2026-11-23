"""Control endpoints; defaults preserve the original single-host experiment."""
import os

PROJECT = os.getenv("LAB_PROJECT", "witw-reliability-live")
NLP_GROUP = os.getenv("LAB_NLP_GROUP", "nlp-worker-live")
KAFKA_BOOTSTRAP = os.getenv("LAB_KAFKA_BOOTSTRAP", "127.0.0.1:39992")
DB_HOST = os.getenv("LAB_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("LAB_DB_PORT", "39932"))
DB_PASSWORD = os.getenv("LAB_DB_PASSWORD", "reliability_only")
RECEIVER_URL = os.getenv("LAB_RECEIVER_URL", "http://127.0.0.1:39904")
PROXY_URL = os.getenv("LAB_PROXY_URL", "http://127.0.0.1:38474")
PROMETHEUS_URL = os.getenv("LAB_PROMETHEUS_URL", "http://127.0.0.1:39900")
