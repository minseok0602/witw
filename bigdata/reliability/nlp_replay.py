"""Fixed inference outputs, production Kafka consumer/producer/archive functions."""
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, "/nlp")
from consumer import create_consumer, consume_batch, commit_batch
from producer import create_producer, send_enriched_article, flush_producer
from storage import raw_archive
from fixtures import embedding

raw_archive.RAW_DATA_DIR = Path("/state/nlp-archive")
consumer = create_consumer()
producer = create_producer()
while True:
    batch = consume_batch(consumer)
    if not batch:
        time.sleep(0.1)
        continue
    raw_archive.archive_batch(batch)
    for item in batch:
        article = item["article"]
        theme_id = 900001
        # Separate controlled burst catalog; existing fault fixtures are unchanged.
        source_url = article["article_url"]
        if source_url.split("/")[-2].startswith("soak-"):
            from load_fixtures import article_embedding, THEMES, THEME_FIRST
            theme = int(source_url.rsplit("/", 1)[-1]) % THEMES
            theme_id = THEME_FIRST + theme
            vector = article_embedding(article["article_id"], theme)
        else:
            vector = embedding(article["article_id"])
        article["nlp_worker_data"] = {
            "country_code": "US", "language_code": "en", "translated_title": article["title"],
            "field_id": 900001, "theme_id": theme_id, "theme_relevance_score": 1.0,
            "article_embedding": vector, "summary_embedding": vector,
            "summary": article["body"][:300], "sentiment": {"score": 50.0}, "mentioned_countries": [],
            "processed_at": article["discovered_at"], "version": "fixed-fixture",
        }
        send_enriched_article(producer, article)
    flush_producer(producer)
    commit_batch(consumer)
    print("COMMITTED", len(batch), flush=True)
