"""
consumer.py - ONE consumer for all five Media/OTT Kafka topics.

    python consumer.py

Reads playback-events, content-metadata, subscription-events, cdn-performance
and social-mentions, normalises every message into one common document shape
(normalize_event, below), and stores all of them in a single MongoDB Atlas
collection - that single collection is the "consolidated data source" the
Atlas Charts dashboard is built on.

Credentials come from a .env file next to this script:
    MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>/...
    MONGODB_DATABASE=ott_streaming        (optional)
    MONGODB_COLLECTION=all_events         (optional)
    EVENT_TTL_HOURS=12                    (optional, auto-delete old events; 0 = never)
    KAFKA_BROKER=localhost:9092           (optional)
Never hardcode a connection string in this file.
"""
import json
import os
import time
from datetime import datetime, timezone

import certifi
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.server_api import ServerApi

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "ott_streaming")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "all_events")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "localhost:9092")
# Free-tier (M0) Atlas clusters cap at 512 MB; at ~9 events/sec the producer writes
# roughly 300+ MB/day. Documents older than this are deleted automatically. 0 = keep forever.
EVENT_TTL_HOURS = float(os.getenv("EVENT_TTL_HOURS", "12"))

TOPICS = [
    "playback-events",
    "content-metadata",
    "subscription-events",
    "cdn-performance",
    "social-mentions",
]

SOURCE_MAP = {
    "playback-events": "playback",
    "content-metadata": "metadata",
    "subscription-events": "subscription",
    "cdn-performance": "cdn",
    "social-mentions": "social",
}

# The unified schema. Every document carries the "common" block; the rest are
# filled only by the source that owns them (missing fields are simply left out,
# which is how MongoDB and Atlas Charts expect sparse data to look).
SCHEMA = {
    "common": ["event_id", "source", "kafka_topic", "timestamp", "ingested_at"],
    "user_session": ["user_id", "session_id", "plan", "region", "cdn_node"],
    "content": ["title_id", "title", "genre", "language", "content_rating",
                "licensing_window_end", "license_status", "update_type"],
    "playback": ["event_type", "device", "watch_position_sec", "content_duration_sec",
                 "watch_completion_pct", "session_quality"],
    "cdn_qos": ["latency_ms", "bitrate_mbps", "buffering_rate_pct", "error_code"],
    "subscription": ["subscription_action", "plan_price_inr"],
    "social": ["platform", "text", "sentiment"],
}
DATE_FIELDS = {"timestamp", "licensing_window_end"}


def parse_timestamp(value):
    """ISO string -> real datetime, so MongoDB/Atlas Charts treat it as a Date, not text."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def normalize_event(topic, event):
    """One document shape for every source, so the five streams can be queried,
    filtered and charted together instead of as five separate collections."""
    doc = {"source": SOURCE_MAP.get(topic, topic), "kafka_topic": topic}
    for group, fields in SCHEMA.items():
        for field in fields:
            if field in doc or field == "ingested_at":
                continue
            value = event.get(field)
            if field in DATE_FIELDS:
                value = parse_timestamp(value)
            if value is not None:
                doc[field] = value
    doc.setdefault("timestamp", datetime.now(timezone.utc))
    doc["ingested_at"] = datetime.now(timezone.utc)   # producer->Atlas lag = ingested_at - timestamp
    return doc


def ensure_indexes(collection):
    """event_id unique -> replaying Kafka offsets can't create duplicates.
    timestamp / source+timestamp -> the 'last N minutes' dashboard queries stay fast."""
    try:
        collection.create_index([("event_id", ASCENDING)], unique=True, name="uniq_event_id")
    except Exception as e:
        raise SystemExit(
            f"\nERROR: could not create the unique event_id index ({e}).\n"
            "The collection probably holds duplicates from the old consumer.\n"
            "Fix: Atlas -> Browse Collections -> ott_streaming -> all_events -> Drop collection,\n"
            "then run consumer.py again.\n")
    collection.create_index([("timestamp", DESCENDING)], name="ts_desc")
    collection.create_index([("source", ASCENDING), ("timestamp", DESCENDING)], name="source_ts")
    if EVENT_TTL_HOURS > 0:
        try:
            collection.create_index([("ingested_at", ASCENDING)], name="ttl_ingested",
                                    expireAfterSeconds=int(EVENT_TTL_HOURS * 3600))
        except Exception as e:   # e.g. TTL value changed since the index was first created
            print(f"[warn] TTL index not (re)created: {e}. Drop 'ttl_ingested' in Atlas to change it.")


def main():
    if not MONGODB_URI:
        raise SystemExit("ERROR: MONGODB_URI is missing. Copy .env.example to .env and fill it in.")

    mongo_client = MongoClient(MONGODB_URI, server_api=ServerApi("1"), tlsCAFile=certifi.where())
    mongo_client.admin.command("ping")   # fails fast if the URI/password/IP allowlist is wrong
    collection = mongo_client[MONGODB_DATABASE][MONGODB_COLLECTION]
    ensure_indexes(collection)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="ott-vertical-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,   # we commit ourselves, only AFTER the write to Atlas succeeds
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print()
    print("=" * 70)
    print("MEDIA / OTT - CONSOLIDATED CONSUMER")
    print("=" * 70)
    print(f"Kafka:       {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"MongoDB:     Atlas -> {MONGODB_DATABASE}.{MONGODB_COLLECTION}")
    print("Topics:      " + ", ".join(TOPICS))
    print("=" * 70)
    print()

    stored, dupes = 0, 0
    per_source = {s: 0 for s in SOURCE_MAP.values()}
    try:
        for message in consumer:
            doc = normalize_event(message.topic, message.value)
            if write_with_retry(collection, doc):
                stored += 1
                per_source[doc["source"]] += 1
            else:
                dupes += 1
            # Commit the Kafka offset only after the document is safely in Atlas.
            # If we crash before this line, Kafka re-delivers the message and the
            # unique event_id index turns the second write into a harmless duplicate.
            if (stored + dupes) % 25 == 0:
                commit_offsets(consumer)
                counts = "  ".join(f"{k}={v}" for k, v in per_source.items())
                print(f"[stored {stored}] {counts}")
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    finally:
        commit_offsets(consumer)
        consumer.close()
        mongo_client.close()
        print(f"Closed. {stored} documents stored this run, {dupes} duplicates skipped.")


def short_reason(error):
    text = str(error)
    for key, reason in [("getaddrinfo", "no internet / DNS lookup failed"),
                        ("TLSV1_ALERT_INTERNAL_ERROR", "Atlas refused this IP - add it in Atlas > Network Access"),
                        ("10054", "connection reset (network dropped)"),
                        ("timed out", "timed out"),
                        ("bad auth", "wrong username/password in .env")]:
        if key in text:
            return reason
    return text.split("\n")[0][:120]


def write_with_retry(collection, doc):
    """Insert one document. Returns True if stored, False if it was a duplicate.
    If Atlas is unreachable, WAIT and retry instead of skipping - no event is lost."""
    wait = 5
    while True:
        try:
            collection.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False
        except Exception as error:
            print(f"[WAITING] Atlas unreachable: {short_reason(error)}. "
                  f"Retrying in {wait}s (nothing is lost - Kafka keeps the events).")
            time.sleep(wait)
            wait = min(wait * 2, 60)
            doc.pop("_id", None)   # insert_one adds _id even on failure; let Mongo assign a fresh one


def commit_offsets(consumer):
    try:
        consumer.commit()
    except Exception as error:   # e.g. after a long outage Kafka re-assigns partitions; it re-delivers instead
        print(f"[note] offset commit skipped ({type(error).__name__}); Kafka will re-deliver, duplicates are ignored")


if __name__ == "__main__":
    main()
