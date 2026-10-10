"""
producer.py - ONE producer for all five Media/OTT data sources.

    python producer.py            # runs all 5 generators together (use this for the demo)
    python producer.py playback   # runs just one, for a one-source demo
    python producer.py metadata | subscription | cdn | social

LIVE INCIDENT CONTROL (all-5 mode only)
    While it runs, type a CDN node name and press Enter to knock that edge
    node over for ~3 minutes, e.g.

        edge-mum-01

    Type `status` to see node health, `clear` to end the incident early.
    Ctrl+C stops everything.

    PlaybackGenerator      -> playback-events
    MetadataGenerator      -> content-metadata
    SubscriptionGenerator  -> subscription-events
    CdnGenerator           -> cdn-performance
    SocialGenerator        -> social-mentions

WHY THE STREAMS ARE LINKED
    Every region is served by one CDN edge node (REGION_NODE). When a node
    degrades, the knock-on effects show up in the other four streams the way
    they would on a real platform:
        cdn          -> latency / buffering / error codes spike on that node
        playback     -> sessions in that region buffer and get abandoned
        social       -> complaints about buffering rise in that region
        subscription -> cancellations rise in that region
    CDN pings are attached to real, currently-active playback sessions
    (same session_id, user_id, region), so the sources can be joined.
"""
import json
import os
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

TOPICS = {
    "playback": "playback-events",
    "metadata": "content-metadata",
    "subscription": "subscription-events",
    "cdn": "cdn-performance",
    "social": "social-mentions",
}

# Seconds between events per source. Playback dominates real OTT traffic
# (heartbeats every few seconds per viewer); catalogue changes are rare.
# Total ~ 5 + 2 + 1 + 0.7 + 0.2 = ~8.9 events/sec (~770k events/day).
INTERVALS = {
    "playback": 0.2,
    "cdn": 0.5,
    "social": 1.5,
    "subscription": 1.0,
    "metadata": 5.0,
}

INCIDENT_SECONDS = 180          # how long a manually triggered incident lasts
RANDOM_INCIDENT_PROB = 0.002    # per CDN event; ~1 unplanned incident every few minutes

# ======================================================================
# SHARED CATALOGUE - same titles, users, regions and nodes in every source
# ======================================================================
SEED = 42
_rng = random.Random(SEED)

GENRES = ["Drama", "Thriller", "Comedy", "Sports", "Documentary", "Kids", "Reality"]
LANGUAGES = ["English", "Hindi", "Tamil", "Telugu", "Bengali"]
RATINGS = ["U", "U/A 7+", "U/A 13+", "U/A 16+", "A"]

_TITLES = [
    "Monsoon Letters", "Saffron Street", "Midnight Signal", "Code Red Bandra",
    "Hostel Diaries", "Uncle in Charge", "Little Astronauts", "Jungle Jugaad",
    "Rivers of India", "The Spice Route", "Big House Live", "Kitchen Masters",
    "Love in Lockdown", "Two Tickets to Goa", "Derby Night Live", "The Last Innings",
    "Paper Boats", "Glass Door", "Office Chai Break", "Startup Nation",
]
_START = datetime.now(timezone.utc)
CATALOG = [{
    "title_id": f"T{2000 + i}",
    "title": t,
    "genre": _rng.choice(GENRES),
    "language": _rng.choice(LANGUAGES),
    "content_rating": _rng.choice(RATINGS),
    # most titles renew well in advance; the first three are deliberately
    # already expired / about to expire so the licensing alert has something to catch
    "licensing_window_end": (_START + timedelta(
        hours=_rng.choice([-6, 4, 18] if i < 3 else [720, 1500, 3000, 6000]))).isoformat(),
} for i, t in enumerate(_TITLES)]
TITLE_BY_ID = {t["title_id"]: t for t in CATALOG}

REGIONS = ["IN-North", "IN-South", "IN-West", "IN-East", "UAE", "UK", "US"]
CDN_NODES = ["edge-del-01", "edge-blr-01", "edge-mum-01", "edge-dxb-01", "edge-lon-01", "edge-nyc-01"]
REGION_NODE = {
    "IN-North": "edge-del-01", "IN-East": "edge-del-01", "IN-South": "edge-blr-01",
    "IN-West": "edge-mum-01", "UAE": "edge-dxb-01", "UK": "edge-lon-01", "US": "edge-nyc-01",
}
DEVICES = ["smart_tv", "mobile_android", "mobile_ios", "web_browser", "tablet", "streaming_stick"]
PLANS = ["Basic", "Standard", "Premium"]
PLAN_PRICE_INR = {"Basic": 199, "Standard": 499, "Premium": 649}   # monthly, illustrative
PLATFORMS = ["Twitter/X", "Instagram", "Reddit", "Facebook"]

# Weighted so India is most of the base, like an Indian OTT with a diaspora audience
_REGION_WEIGHTS = [24, 22, 22, 12, 8, 6, 6]
USERS = [{
    "user_id": f"U{5000 + i}",
    "region": _rng.choices(REGIONS, weights=_REGION_WEIGHTS)[0],
    "plan": _rng.choices(PLANS, weights=[45, 35, 20])[0],
} for i in range(300)]

SOCIAL_GOOD = ["loved the finale", "binged this all weekend", "can't stop recommending it",
               "best thing on the platform right now", "the cinematography is stunning"]
SOCIAL_BAD = ["why does this keep logging me out", "subtitles are missing again",
              "audio is out of sync", "too many ads on the free tier"]
SOCIAL_BUFFERING = ["keeps buffering on my wifi", "stuck on the loading wheel again",
                    "picture keeps dropping to 240p", "app crashed twice tonight",
                    "cancelled my subscription over this buffering"]


def new_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def license_status(title):
    hours_left = (datetime.fromisoformat(title["licensing_window_end"])
                  - datetime.now(timezone.utc)).total_seconds() / 3600
    if hours_left <= 0:
        return "expired"
    if hours_left <= 24:
        return "expiring_24h"
    return "active"


# ======================================================================
# SHARED LIVE STATE - lets the five generator threads affect each other
# ======================================================================
class PlatformState:
    def __init__(self):
        self.lock = threading.Lock()
        self.degraded = {}          # node -> unix time the incident ends
        self.active_sessions = {}   # session_id -> {user_id, region, node, title_id}

    # --- incidents ---------------------------------------------------
    def degrade(self, node, seconds=INCIDENT_SECONDS):
        with self.lock:
            self.degraded[node] = time.time() + seconds

    def clear(self):
        with self.lock:
            self.degraded.clear()

    def is_degraded(self, node):
        with self.lock:
            end = self.degraded.get(node)
            if end and time.time() > end:
                del self.degraded[node]
                return False
            return bool(end)

    def region_degraded(self, region):
        return self.is_degraded(REGION_NODE[region])

    def any_degraded(self):
        return any(self.is_degraded(n) for n in list(self.degraded))

    # --- sessions ----------------------------------------------------
    def add_session(self, sid, info):
        with self.lock:
            self.active_sessions[sid] = info

    def end_session(self, sid):
        with self.lock:
            self.active_sessions.pop(sid, None)

    def random_session(self, rng):
        with self.lock:
            if not self.active_sessions:
                return None, None
            sid = rng.choice(list(self.active_sessions))
            return sid, dict(self.active_sessions[sid])


STATE = PlatformState()


# ======================================================================
# PLAYBACK  (Viewer / Player App -> playback-events)
# ======================================================================
class PlaybackGenerator:
    """Viewers watching: start, progress, pause, buffering, abandon, complete."""

    MAX_SESSIONS = 40

    def __init__(self, seed=None, state=STATE):
        self.rng = random.Random(seed)
        self.state = state
        self.sessions = {}

    def _start_session(self):
        user = self.rng.choice(USERS)
        title = self.rng.choice(CATALOG)
        sid = new_id("SES")
        self.sessions[sid] = {
            "user_id": user["user_id"], "region": user["region"], "plan": user["plan"],
            "cdn_node": REGION_NODE[user["region"]],
            "title_id": title["title_id"],
            "device": self.rng.choice(DEVICES),
            "duration": self.rng.randint(1200, 7200),
            "position": 0,
        }
        self.state.add_session(sid, {k: self.sessions[sid][k]
                                     for k in ("user_id", "region", "cdn_node", "title_id")})
        return sid

    def next_event(self):
        if len(self.sessions) < self.MAX_SESSIONS and (not self.sessions or self.rng.random() < 0.3):
            sid = self._start_session()
            s = self.sessions[sid]
            event_type = "play_start"
        else:
            sid = self.rng.choice(list(self.sessions))
            s = self.sessions[sid]
            degraded = self.state.is_degraded(s["cdn_node"])
            if degraded:
                s["position"] = min(s["duration"], s["position"] + self.rng.randint(0, 20))
                event_type = self.rng.choices(
                    ["buffering", "progress", "abandon"], weights=[55, 20, 25])[0]
            else:
                s["position"] = min(s["duration"], s["position"] + self.rng.randint(60, 300))
                event_type = self.rng.choices(
                    ["progress", "pause", "resume", "seek", "abandon"], weights=[71, 10, 10, 8, 1])[0]
            if event_type != "abandon" and s["position"] >= s["duration"] * 0.95:
                event_type = "complete"

        degraded = self.state.is_degraded(s["cdn_node"])
        quality = (self.rng.choices(["fair", "poor"], weights=[30, 70])[0] if degraded else
                   self.rng.choices(["excellent", "good", "fair"], weights=[60, 32, 8])[0])
        title = TITLE_BY_ID[s["title_id"]]
        event = {
            "event_id": new_id("PB"),
            "timestamp": now_iso(),
            "user_id": s["user_id"],
            "session_id": sid,
            "title_id": title["title_id"],
            "title": title["title"],
            "genre": title["genre"],
            "language": title["language"],
            "license_status": license_status(title),
            "event_type": event_type,
            "device": s["device"],
            "watch_position_sec": s["position"],
            "content_duration_sec": s["duration"],
            "watch_completion_pct": round(100 * s["position"] / s["duration"], 1),
            "session_quality": quality,
            "region": s["region"],
            "cdn_node": s["cdn_node"],
            "plan": s["plan"],
        }
        if event_type in ("complete", "abandon"):
            del self.sessions[sid]
            self.state.end_session(sid)
        return sid, event


# ======================================================================
# CONTENT METADATA  (Content Catalog & Licensing System -> content-metadata)
# ======================================================================
class MetadataGenerator:
    """Catalogue changes: new releases, metadata edits, licences expiring."""

    def __init__(self, seed=None, state=STATE):
        self.rng = random.Random(seed)

    def next_event(self):
        title = self.rng.choice(CATALOG)
        status = license_status(title)
        if status == "expired":
            update_type = "license_expired"
        elif status == "expiring_24h":
            update_type = "license_expiring_soon"
        else:
            update_type = self.rng.choices(
                ["metadata_update", "new_release", "availability_check"], weights=[50, 10, 40])[0]
        event = {
            "event_id": new_id("MD"),
            "timestamp": now_iso(),
            "title_id": title["title_id"],
            "title": title["title"],
            "genre": title["genre"],
            "language": title["language"],
            "content_rating": title["content_rating"],
            "licensing_window_end": title["licensing_window_end"],
            "license_status": status,
            "update_type": update_type,
        }
        return title["title_id"], event


# ======================================================================
# SUBSCRIPTION  (Billing & Subscription Gateway -> subscription-events)
# ======================================================================
class SubscriptionGenerator:
    """Billing: renewals, plan changes, payment failures, cancellations, win-backs."""

    def __init__(self, seed=None, state=STATE):
        self.rng = random.Random(seed)
        self.state = state
        self.status = {u["user_id"]: "active" for u in USERS}
        self.plan = {u["user_id"]: u["plan"] for u in USERS}

    def next_event(self):
        user = self.rng.choice(USERS)
        uid, region = user["user_id"], user["region"]
        status = self.status[uid]
        degraded = self.state.region_degraded(region)

        if status == "cancelled":
            action = "reactivation"
            self.status[uid] = "active"
        elif status == "past_due":
            action = self.rng.choices(["payment_retry_success", "payment_failed"], weights=[70, 30])[0]
            if action == "payment_retry_success":
                self.status[uid] = "active"
        else:
            cancel_w = 60 if degraded else 6
            action = self.rng.choices(
                ["renewal", "plan_upgrade", "plan_downgrade", "payment_failed", "cancellation"],
                weights=[70, 8, 6, 8, cancel_w])[0]
            if action == "payment_failed":
                self.status[uid] = "past_due"
            elif action == "cancellation":
                self.status[uid] = "cancelled"
            elif action == "plan_upgrade" and self.plan[uid] != "Premium":
                self.plan[uid] = PLANS[PLANS.index(self.plan[uid]) + 1]
            elif action == "plan_downgrade" and self.plan[uid] != "Basic":
                self.plan[uid] = PLANS[PLANS.index(self.plan[uid]) - 1]
            elif action in ("plan_upgrade", "plan_downgrade"):
                action = "renewal"

        plan = self.plan[uid]
        event = {
            "event_id": new_id("SUB"),
            "timestamp": now_iso(),
            "user_id": uid,
            "subscription_action": action,
            "plan": plan,
            "plan_price_inr": PLAN_PRICE_INR[plan],
            "region": region,
            "cdn_node": REGION_NODE[region],
        }
        return uid, event


# ======================================================================
# CDN / QoS  (CDN / Player Telemetry -> cdn-performance)
# ======================================================================
class CdnGenerator:
    """Per-session streaming-quality pings: latency, bitrate, buffering, errors."""

    ERROR_CODES = ["DRM_LICENSE_ERROR", "MANIFEST_NOT_FOUND", "SEGMENT_TIMEOUT"]

    def __init__(self, seed=None, state=STATE):
        self.rng = random.Random(seed)
        self.state = state

    def next_event(self):
        if not self.state.any_degraded() and self.rng.random() < RANDOM_INCIDENT_PROB:
            self.state.degrade(self.rng.choice(CDN_NODES), self.rng.uniform(60, 120))

        sid, sess = self.state.random_session(self.rng)
        if sess is None:                      # nobody watching yet
            region = self.rng.choice(REGIONS)
            sess = {"user_id": None, "region": region, "cdn_node": REGION_NODE[region], "title_id": None}
            sid = None
        node = sess["cdn_node"]
        degraded = self.state.is_degraded(node)
        r = self.rng
        event = {
            "event_id": new_id("CDN"),
            "timestamp": now_iso(),
            "session_id": sid,
            "user_id": sess["user_id"],
            "title_id": sess["title_id"],
            "cdn_node": node,
            "region": sess["region"],
            "latency_ms": r.randint(300, 900) if degraded else r.randint(20, 150),
            "bitrate_mbps": round(r.uniform(0.5, 2.0) if degraded else r.uniform(3.0, 12.0), 2),
            "buffering_rate_pct": round(r.uniform(15, 40) if degraded else r.uniform(0, 3), 2),
            "error_code": (r.choice(self.ERROR_CODES) if degraded and r.random() < 0.3 else
                           r.choice(self.ERROR_CODES) if r.random() < 0.01 else None),
        }
        return node, event


# ======================================================================
# SOCIAL  (Social Listening -> social-mentions)
# ======================================================================
class SocialGenerator:
    """Social posts about a title - mostly praise; buffering complaints spike during incidents."""

    def __init__(self, seed=None, state=STATE):
        self.rng = random.Random(seed)
        self.state = state

    def next_event(self):
        title = self.rng.choice(CATALOG)
        region = self.rng.choices(REGIONS, weights=_REGION_WEIGHTS)[0]
        if self.state.region_degraded(region):
            sentiment, text = "negative", self.rng.choice(SOCIAL_BUFFERING)
        elif self.rng.random() < 0.82:
            sentiment, text = "positive", self.rng.choice(SOCIAL_GOOD)
        else:
            sentiment, text = "negative", self.rng.choice(SOCIAL_BAD)
        event = {
            "event_id": new_id("SOC"),
            "timestamp": now_iso(),
            "title_id": title["title_id"],
            "title": title["title"],
            "genre": title["genre"],
            "platform": self.rng.choice(PLATFORMS),
            "text": f"{title['title']} - {text}",
            "sentiment": sentiment,
            "region": region,
            "cdn_node": REGION_NODE[region],
        }
        return title["title_id"], event


GENERATORS = {
    "playback": (PlaybackGenerator, TOPICS["playback"]),
    "metadata": (MetadataGenerator, TOPICS["metadata"]),
    "subscription": (SubscriptionGenerator, TOPICS["subscription"]),
    "cdn": (CdnGenerator, TOPICS["cdn"]),
    "social": (SocialGenerator, TOPICS["social"]),
}


# ======================================================================
# SENDING
# ======================================================================
def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k or "none").encode("utf-8"),
        acks="all",
        linger_ms=20,
    )


def run_one(name, kafka, stop_event):
    cls, topic = GENERATORS[name]
    gen = cls()
    interval = INTERVALS[name]
    sent = 0
    print(f"[{name}] -> topic '{topic}' every {interval}s")
    while not stop_event.is_set():
        key, event = gen.next_event()
        try:
            kafka.send(topic, key=key, value=event)
            sent += 1
        except Exception as e:
            print(f"[{name}] send failed: {e}")
        if sent % 50 == 0 and sent:
            label = (event.get("event_type") or event.get("update_type")
                     or event.get("subscription_action") or event.get("sentiment") or "event")
            print(f"[{name}] sent {sent} | last: {label}")
        stop_event.wait(interval)
    print(f"[{name}] stopped after {sent} events")


def incident_console(stop_event):
    """Reads commands from the terminal so you can trigger an incident live."""
    print("Incident control: type a node name to degrade it "
          f"({', '.join(CDN_NODES)}), 'status', or 'clear'.\n")
    while not stop_event.is_set():
        try:
            cmd = input().strip().lower()
        except EOFError:
            return
        if cmd in CDN_NODES:
            STATE.degrade(cmd)
            regions = [r for r, n in REGION_NODE.items() if n == cmd]
            print(f"\n*** INCIDENT: {cmd} degraded for {INCIDENT_SECONDS}s "
                  f"(affects {', '.join(regions)}) ***\n")
        elif cmd == "clear":
            STATE.clear()
            print("\n*** all nodes healthy ***\n")
        elif cmd == "status":
            bad = [n for n in CDN_NODES if STATE.is_degraded(n)]
            print(f"degraded: {bad or 'none'} | active sessions: {len(STATE.active_sessions)}")
        elif cmd:
            print(f"unknown command '{cmd}'")


def main():
    try:
        kafka = make_producer()
    except Exception as e:
        raise SystemExit(f"\n[producer] Cannot reach Kafka at {KAFKA_BROKER} ({e}).\n"
                         "           Is Docker running? Try:  docker compose up -d\n")

    stop = threading.Event()

    if len(sys.argv) == 2:
        name = sys.argv[1].lower()
        if name not in GENERATORS:
            print(f"Usage: python producer.py [{'|'.join(GENERATORS)}]  (or no argument for all 5)")
            sys.exit(1)
        try:
            run_one(name, kafka, stop)
        except KeyboardInterrupt:
            pass
        finally:
            kafka.flush()
            kafka.close()
        return

    threads = [threading.Thread(target=run_one, args=(name, kafka, stop), name=name, daemon=True)
               for name in GENERATORS]
    for t in threads:
        t.start()

    print("\n=== 5 producers running (playback, metadata, subscription, cdn, social). "
          "Ctrl+C to stop. ===")
    try:
        incident_console(stop)
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        print("\nStopping producers...")
        stop.set()
        for t in threads:
            t.join(timeout=5)
        kafka.flush()
        kafka.close()


if __name__ == "__main__":
    main()
