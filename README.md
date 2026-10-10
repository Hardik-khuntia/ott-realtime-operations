# Media / OTT Real-Time Operations — Final Project (v2)

| File | What it is |
|---|---|
| `producer.py` | 5 linked data generators → 5 Kafka topics, with live incident control |
| `consumer.py` | reads all 5 topics → one Atlas collection (idempotent, indexed, 12 h TTL) |
| `.env.example` | copy to `.env` and fill in your Atlas URI |
| `dashboards/OTT_Media_Final_Dashboard.charts` | importable dashboard, 25 tiles |
| `atlas_charts_queries.js` | the one tile you add by hand (session join) |
| `ANSWERS.md` | corrected rubric answers, all numbers match this code |
| `tools/build_dashboard.py` | regenerates the `.charts` file |
| `tools/validate_dashboard.py` | tests every tile against 40 simulated minutes (no Kafka/Atlas needed) |

## Setup (done once, on Windows)

Already completed:
- Atlas password rotated.
- `.env` points at cluster **SDA-Streaming-Analytics**, database **`ott_final`**. Assignment 3's data is untouched.
- Kafka runs as the Docker container **`ott-kafka`** (apache/kafka, KRaft mode, no ZooKeeper), with the 5 topics created.
- Dashboard imported and mapped to `ott_final.all_events`.

The old `sda-kafka-1` / `sda-zookeeper-1` containers are not used. Leave them alone.

## Starting everything (every session, including demo day)

Open Command Prompt in the project folder: in File Explorer, click the address bar, type `cmd`, press Enter.

1. Start Docker Desktop and wait for "Engine running".
2. Start Kafka:
   ```
   docker start ott-kafka
   ```
3. Window 1:
   ```
   python consumer.py
   ```
4. Open a second Command Prompt the same way. Window 2:
   ```
   python producer.py
   ```
5. Stop with **Ctrl + C** in both windows when you're done. Don't leave them running overnight: the free tier has 512 MB, shared with Assignment 3.

## If the consumer prints `[WAITING] Atlas unreachable`

- **"Atlas refused this IP":** your laptop's IP changed (new Wi-Fi, reconnect, hotspot). In Atlas, go to Network Access → Add IP Address → Add Current IP Address, wait until it shows Active. The consumer resumes by itself; don't restart it.
- **"no internet":** reconnect Wi-Fi. It resumes by itself.
- **Before the demo,** add the IP of the network you'll present on (classroom Wi-Fi or hotspot).

## Demo day

**Start the producer and consumer at least 45 minutes before presenting** so the
dashboard has history. Do not leave them running overnight (512 MB free tier).

**Refresh, important:** on the free M0 tier, dashboards auto-refresh only every 4 hours.
- To update during the demo, click the dashboard **refresh icon**.
- If tiles still look stale, change any dashboard filter (e.g. Region → IN-West → back to all).
  Filters always re-query.
- Rehearse this once.

**Live demo script (~4 min)**
1. Show both Command Prompt windows side by side, then the healthy dashboard.
2. In the producer window (Window 2), type `edge-mum-01` and press Enter
   (`*** INCIDENT ... affects IN-West ***`).
3. Explain the architecture slide for about 90 seconds.
4. Refresh the dashboard. Point out:
   - the hot band on edge-mum-01 in the heatmap;
   - IN-West at the top of all four region charts;
   - Revenue at Risk climbing;
   - Pipeline Lag still low, as proof it's live.
5. Type `clear` and say: "In production, this signal would trigger automatic failover."

Backup: record a 2-minute screen video of steps 2–4 the day before, in case
Wi-Fi or Docker fails on stage.

## Faculty Q&A (likely from an engineering leader)

| Question | Answer |
|---|---|
| Consumer crashes mid-demo? | Offsets are committed only after the Atlas write succeeds, so Kafka re-delivers anything unconfirmed. The unique `event_id` index makes the re-write harmless: no duplicates, no loss. |
| Ordering? | Guaranteed per key, not globally. Playback is keyed by `session_id`, so one session's events stay in order in one partition. |
| Scale to 1M viewers? | ~33k events/sec at one heartbeat per 30 s. Add partitions; run more consumers with the same `group_id` (Kafka rebalances partitions); batch inserts. Parallelism is capped by partition count. |
| Atlas goes down? | The consumer waits and retries with back-off while Kafka holds the backlog, then catches up. Delayed, not lost. We actually hit this when Wi-Fi reconnected with a new IP. |
| Why one collection, not five? | Cross-source questions become one query instead of an ETL job. Trade-off: sparse documents, which MongoDB handles natively. |
| Why MongoDB? | Native dashboarding at coursework scale. In production, keep Kafka and put a columnar/time-series store behind it. |
| How do you know it's real-time? | The Pipeline Lag tile measures `ingested_at − timestamp` live. |
| Security? | Secrets in `.env`, never in code. An early version leaked a password; we rotated it. In a real repo, `.gitignore` plus GitHub secret scanning would catch it. |
| Is the data real? | Simulated, but causally linked (node failure → buffering → abandonment → churn), not independent noise. |
