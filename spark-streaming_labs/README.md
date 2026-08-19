# Kafka + Spark Streaming learning stack (weatherapi.com)

A local sandbox to learn Spark Structured Streaming with real data:
`weatherapi.com -> producer -> Kafka -> Spark Structured Streaming`.

## What changed from your original two files

- Both files used port **8080** (Kafka UI and Spark Master UI) — that's a
  conflict on your host machine. Spark Master UI is now mapped to
  **9090** instead (the container still listens on 8080 internally).
- The two compose files were on separate default networks, so Spark could
  never have reached Kafka. Everything now shares one `streaming-net`
  bridge network.
- Added a `weather-producer` service that polls weatherapi.com and writes
  JSON events to a `weather-data` Kafka topic.
- Added `apps/streaming_consumer.py`, a ready-to-run Spark Structured
  Streaming job that reads that topic and computes a windowed rolling
  average temperature per city.

## 1. Get a free API key

Sign up at https://www.weatherapi.com/ and grab your API key.

## 2. Configure

```bash
cp .env.example .env
# edit .env and paste your WEATHERAPI_KEY
```

## 3. Start everything

```bash
docker compose up -d
```

Services and where to find them:

| Service          | URL / Port              |
|-------------------|-------------------------|
| Kafka broker       | localhost:9092          |
| Kafka UI           | http://localhost:8080   |
| Spark Master UI    | http://localhost:9090   |
| Spark Worker 1 UI  | http://localhost:8081   |
| Spark Worker 2 UI  | http://localhost:8082   |

Check `weather-producer` logs to confirm it's fetching and sending data:

```bash
docker logs -f weather-producer
```

Open Kafka UI (http://localhost:8080) → Topics → `weather-data` to browse
the raw JSON messages arriving.

## 4. Run the Spark Structured Streaming job

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/streaming_consumer.py
```

The first run downloads the Kafka connector jar (needs internet access
from inside the container) — subsequent runs reuse the cached jar. You
should start seeing windowed average-temperature tables print to the
console every micro-batch, e.g.:

```
+------------------------------------------+-------+----------+------------+
|window                                      |city   |avg_temp_c|max_humidity|
+------------------------------------------+-------+----------+------------+
|{2026-08-19 10:00:00, 2026-08-19 10:01:00}|London |18.4      |72          |
+------------------------------------------+-------+----------+------------+
```

## 5. Things to try next as you learn

- Change `outputMode` from `complete` to `update` and see the difference.
- Write results to Kafka instead of the console (a second topic), or to
  a Parquet sink on `./data` using `foreachBatch`.
- Parse `fetched_at` with a proper timezone/format via `to_timestamp` and
  compare event-time vs. processing-time windowing.
- Add a second producer city list and watch multiple partitions being
  consumed by both Spark workers.
- Scale workers: `docker compose up -d --scale spark-worker-1=2`.

## 6. Shut down

```bash
docker compose down          # keep the kafka_data volume
docker compose down -v       # also wipe Kafka's stored data
```