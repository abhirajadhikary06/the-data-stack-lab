# Databricks for Data Engineers — Reference Guide

> A practical, code-first reference to the Databricks Lakehouse Platform: architecture, Delta Lake, ingestion, streaming, orchestration, governance, and performance tuning. Interview-style Q&A and system-design talking points are woven directly into each section below for quick lookup.

---

## Contents

1. [What Is Databricks & the Lakehouse Architecture](#1-what-is-databricks--the-lakehouse-architecture)
2. [Platform Architecture Deep Dive](#2-platform-architecture-deep-dive)
3. [Delta Lake](#3-delta-lake--the-core-of-data-engineering-on-databricks)
4. [Medallion Architecture (Bronze / Silver / Gold)](#4-medallion-architecture-bronze--silver--gold)
5. [Ingestion Patterns — Auto Loader & COPY INTO](#5-ingestion-patterns--auto-loader--copy-into)
6. [Structured Streaming](#6-structured-streaming-on-databricks)
7. [Delta Live Tables (DLT)](#7-delta-live-tables-dlt)
8. [Orchestration — Databricks Workflows](#8-orchestration--databricks-workflows)
9. [Unity Catalog & Governance](#9-unity-catalog--governance)
10. [Performance Tuning & Optimization](#10-performance-tuning--optimization)
11. [PySpark Practical Cheat Sheet](#11-pyspark-practical-cheat-sheet)
12. [Databricks SQL Essentials](#12-databricks-sql-essentials)
13. [Quick-Reference Command Cheat Sheet](#13-quick-reference-command-cheat-sheet)

---

## 1. What Is Databricks & the Lakehouse Architecture

Databricks is a unified, cloud-based data platform built by the original creators of Apache Spark. It runs on AWS, Azure, or GCP and combines the low-cost, flexible storage of a data lake with the reliability, performance, and governance of a data warehouse — an architecture Databricks calls the **Lakehouse**.

### Interview framing to memorize
> "A Lakehouse stores data once, in open formats like Delta/Parquet on cheap cloud object storage, and layers ACID transactions, schema enforcement, and fine-grained governance on top — so the same data serves BI, data engineering, and ML workloads without duplicating it into a separate warehouse."

### Architecture comparison

| Architecture | Storage | Strength | Weakness |
|---|---|---|---|
| **Data Warehouse** | Proprietary, structured | Strong ACID, governance, BI performance | Expensive, rigid schema, poor for unstructured/ML data |
| **Data Lake** | Open files (Parquet/CSV/JSON) on object storage | Cheap, scalable, stores any data type | No ACID guarantees, easy to corrupt ("data swamp"), slow BI queries |
| **Lakehouse (Databricks)** | Open format (Delta Lake) on object storage | ACID + schema enforcement + BI speed, on cheap storage, one copy of data | Newer paradigm; team needs to learn Delta-specific operations |

### Core building blocks of the platform

- **Workspace** — the collaborative UI containing notebooks, repos, dashboards, and jobs.
- **Clusters** — managed Spark compute (driver + executors) that Databricks provisions and tears down for you.
- **Delta Lake** — the open-source storage layer that adds ACID transactions on top of Parquet files.
- **Unity Catalog** — centralized governance: permissions, lineage, and a 3-level namespace (`catalog.schema.table`) across all workspaces.
- **Workflows (Jobs)** — native orchestration for scheduling notebooks, SQL, DLT pipelines, and JARs.
- **Delta Live Tables (DLT)** — a declarative framework for building reliable ETL pipelines.
- **Photon** — a native, vectorized query engine (written in C++) that accelerates SQL and DataFrame workloads transparently.

> **Q: What is Photon?**
> A native, vectorized query engine written in C++ that transparently accelerates SQL and DataFrame workloads on Databricks Runtime, with no code changes required.

---

## 2. Platform Architecture Deep Dive

### 2.1 Control Plane vs. Data Plane

One of the most frequently tested architecture concepts, especially for roles touching security or platform setup.

| | Control Plane | Data Plane |
|---|---|---|
| **Owned by** | Databricks | Your cloud account (AWS/Azure/GCP) |
| **Contains** | Web UI, notebooks (source), job scheduler, cluster manager, Unity Catalog metadata | Your actual clusters (VMs), and your data in cloud object storage (S3/ADLS/GCS) |
| **Key point** | No customer data is stored here by default | Your data never leaves your cloud account; Databricks only orchestrates compute against it |

> **Q: What is the difference between the control plane and the data plane?**
> The control plane (managed by Databricks) hosts the UI, job scheduler, and cluster manager; the data plane (in your cloud account) runs the actual compute and stores your data. Your data never leaves your cloud account by default.

### 2.2 Driver, Executors & Clusters

Every Databricks cluster is a Spark cluster under the hood: one **driver** node coordinates the job, plans stages, and collects results; multiple **worker/executor** nodes run tasks in parallel on partitions of data. Databricks manages provisioning, networking, and version compatibility (the Databricks Runtime, or DBR) for you.

| Cluster type | When to use | Notes |
|---|---|---|
| **All-purpose cluster** | Interactive development, ad-hoc notebooks, exploration | Can be shared by multiple users; billed while running/idle-timeout |
| **Job cluster** | Production pipelines triggered by a Workflow | Spins up for the job and terminates immediately after — cheaper, isolated, reproducible |
| **SQL Warehouse** | BI/SQL queries via Databricks SQL, dashboards, JDBC/ODBC | Serverless or classic; optimized specifically for SQL, not general Spark code |
| **Single-node cluster** | Light ETL, small data, ML training with pandas/sklearn | Driver only, no executors — avoids distributed overhead for small workloads |

### 2.3 Autoscaling & Cluster Policies

- **Autoscaling** lets a cluster grow/shrink the number of worker nodes between a min and max based on load, reducing cost on bursty workloads.
- **Spot/preemptible instances** can be mixed in for workers to cut cost, with on-demand instances reserved for the driver.
- **Cluster policies** let admins restrict instance types, DBR versions, and max size so engineers cannot accidentally launch an oversized/expensive cluster.
- **Photon** can be enabled on a cluster to accelerate SQL/DataFrame execution without code changes — it rewrites physical execution, not your query.

### 2.4 DBFS & Storage Access

DBFS (Databricks File System) is a distributed file system abstraction mounted on every cluster, backed by your cloud object storage. Modern best practice is to **avoid storing production data directly in the DBFS root** and instead use Unity Catalog **external locations** and **volumes** pointing at governed cloud storage paths, with credentials managed centrally via **storage credentials**.

---

## 3. Delta Lake — the Core of Data Engineering on Databricks

Delta Lake is an open-source storage layer that sits on top of Parquet files and adds a transaction log.

> **The one thing to remember:** Delta table = Parquet data files + a `_delta_log` folder of JSON/checkpoint files that record every change as an ordered, atomic transaction. That log is what gives Delta ACID guarantees, time travel, and safe concurrent writes on top of plain object storage.

### 3.1 Creating and writing Delta tables

```python
# PySpark
df = spark.read.json("/mnt/raw/orders/")
(df.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("order_date")
 .saveAsTable("main.sales.orders_bronze"))
```
*Write a DataFrame as a managed Delta table using Unity Catalog's 3-level namespace.*

```sql
-- SQL
CREATE TABLE main.sales.orders_bronze (
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  amount DECIMAL(10,2)
)
USING DELTA
PARTITIONED BY (order_date);
```
*Delta tables can also be defined declaratively in SQL.*

### 3.2 ACID transactions & the transaction log

- Every write (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) creates a new, atomic, numbered JSON commit in `_delta_log`.
- Readers always see a consistent snapshot — they never see a partial write, even during concurrent jobs.
- Delta uses **optimistic concurrency control**: writers assume no conflict, then check at commit time and retry/fail if another writer changed the same files.
- Periodically, Delta writes a **checkpoint** (Parquet snapshot of the log) so readers don't have to replay thousands of JSON files from scratch.

> **Q: How does Delta Lake provide ACID transactions on top of object storage?**
> Every write commits an ordered, atomic entry to the `_delta_log` transaction log; readers always see a consistent snapshot, and concurrent writers use optimistic concurrency control to detect and resolve conflicts at commit time.

### 3.3 Time travel

Because every version is recorded, you can query a Delta table as of a previous version or timestamp — extremely useful for debugging bad pipeline runs or reproducing a report.

```sql
-- Query a prior version
SELECT * FROM main.sales.orders_bronze VERSION AS OF 12;
SELECT * FROM main.sales.orders_bronze TIMESTAMP AS OF '2026-08-01T00:00:00Z';

-- Roll back a table to a previous version
RESTORE TABLE main.sales.orders_bronze TO VERSION AS OF 12;

-- Inspect the full history
DESCRIBE HISTORY main.sales.orders_bronze;
```

Time travel works identically in PySpark via `.option('versionAsOf', 12)` or `'timestampAsOf'`.

### 3.4 Schema enforcement & schema evolution

- By default, Delta **rejects writes** whose schema doesn't match the target table (schema enforcement) — this is what prevents a data swamp.
- When schema drift is expected (e.g., a new column from an upstream API), opt in explicitly with `mergeSchema`.

```python
(df.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .saveAsTable("main.sales.orders_bronze"))
```

> **Q: What's the difference between schema enforcement and schema evolution?**
> Enforcement is Delta's default behavior of rejecting writes that don't match the table's schema, preventing bad data from corrupting the table. Evolution is opting in explicitly (`mergeSchema=true`, or Auto Loader's schema evolution modes) to allow the schema to grow, e.g., when a new column appears upstream.

### 3.5 Upserts with MERGE INTO

`MERGE INTO` is the single most-tested Delta operation in interviews because it's how you implement **upserts / CDC (change-data-capture) / SCD Type 1 & 2** logic.

```sql
MERGE INTO main.sales.customers AS target
USING staged_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.op = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  target.name = source.name,
  target.email = source.email,
  target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (customer_id, name, email, updated_at)
  VALUES (source.customer_id, source.name, source.email, source.updated_at);
```
*Classic CDC upsert pattern: update matches, delete tombstones, insert new rows.*

```python
# Same logic in PySpark using DeltaTable API
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "main.sales.customers")
(target.alias("t")
 .merge(source_df.alias("s"), "t.customer_id = s.customer_id")
 .whenMatchedDelete(condition="s.op = 'DELETE'")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
```

> **Q: How would you implement an upsert (CDC) pipeline in Delta Lake?**
> Use `MERGE INTO`: match target and source on a primary/business key, `UPDATE` matched rows, `INSERT` unmatched rows, and optionally `DELETE` rows flagged as deletes in the source CDC feed.

#### System design — Kafka CDC into an always-current `customers` table
- **Bronze:** Structured Streaming reads Kafka directly (`readStream.format('kafka')`) and lands raw events as-is into an append-only Delta bronze table, preserving offsets for replay.
- **Silver:** a streaming (or DLT) job parses the CDC payload (op type + before/after image), dedupes on `(key, offset)`, and applies `MERGE INTO` against the target table — `UPDATE` on `'update'`, `DELETE` on `'delete'`, `INSERT` on `'insert'/'create'`.
- Use `foreachBatch` on the streaming DataFrame to run the `MERGE` per micro-batch, since `MERGE` isn't natively a streaming sink operation.
- Checkpoint the streaming query so restarts resume exactly where they left off (exactly-once into the Delta sink).

### 3.6 OPTIMIZE, Z-ORDER & Liquid Clustering

Streaming and micro-batch writes create many small files, which hurts read performance (the "small file problem"). `OPTIMIZE` compacts small files into larger ones; `ZORDER BY` co-locates related data within those files so predicate filters skip more data.

```sql
-- Compact small files
OPTIMIZE main.sales.orders_bronze;

-- Compact AND co-locate rows by customer_id for faster point lookups/filters
OPTIMIZE main.sales.orders_bronze ZORDER BY (customer_id);

-- Newer alternative: Liquid Clustering (no manual ZORDER key tuning, incremental)
ALTER TABLE main.sales.orders_bronze CLUSTER BY (customer_id);
```
*Liquid Clustering is Databricks' newer replacement for partitioning + Z-order: it re-clusters incrementally and avoids the fixed-partition rigidity problem.*

> **Q: What is the difference between OPTIMIZE and VACUUM?**
> `OPTIMIZE` compacts small files into larger ones (optionally Z-ORDERing by a column) to speed up reads. `VACUUM` permanently deletes old, unreferenced data files past a retention window to reclaim storage — they solve different problems and are usually run together after heavy write/streaming workloads.

### 3.7 VACUUM

`OPTIMIZE` and `MERGE` leave behind old, no-longer-referenced data files (needed for time travel). `VACUUM` permanently deletes files older than a retention threshold (default 7 days) to reclaim storage.

```sql
-- Remove files no longer referenced and older than the retention window
VACUUM main.sales.orders_bronze RETAIN 168 HOURS; -- 168h = 7 days (default/safe minimum)
```

> **Gotcha:** lowering retention below 7 days (or using `RETAIN 0 HOURS`) can break time travel and even corrupt concurrent long-running readers, because it deletes files a reader might still need. Interviewers listen for you knowing this trade-off, not just the syntax.

> **Q: Why shouldn't you set VACUUM's retention period too low?**
> Time travel and any long-running or concurrent reader depend on old files still existing; vacuuming too aggressively (e.g., `RETAIN 0 HOURS`) can delete files a reader is actively using, causing failures or breaking the ability to query prior versions.

### 3.8 Deletion vectors

> **Q: What are deletion vectors?**
> A performance optimization where `UPDATE`/`DELETE`/`MERGE` mark rows as deleted in a lightweight side file instead of immediately rewriting whole Parquet files; a later `OPTIMIZE` physically compacts the data, trading a little read overhead for much faster write operations.

---

## 4. Medallion Architecture (Bronze / Silver / Gold)

The Medallion architecture is the default design pattern for organizing pipelines on the Lakehouse. Data flows through progressively cleaner layers, each stored as its own Delta table so you can reprocess or audit any stage independently.

| Layer | Purpose | Typical transformations |
|---|---|---|
| **Bronze** | Raw, immutable landing zone | Append-only ingestion, minimal parsing, keep source schema + ingestion metadata (source file, load time) |
| **Silver** | Cleaned, validated, conformed | Dedup, null/type handling, join reference data, enforce schema, flatten nested structures |
| **Gold** | Business-level aggregates | Star schemas, KPIs, aggregations consumed directly by BI dashboards / ML feature tables |

### End-to-end example

```python
# --- BRONZE: land raw JSON as-is, plus lineage metadata ---
bronze_df = (spark.read.json("/mnt/raw/events/")
 .withColumn("_ingested_at", current_timestamp())
 .withColumn("_source_file", input_file_name()))

bronze_df.write.format("delta").mode("append") \
 .saveAsTable("main.events.bronze_events")

# --- SILVER: clean, dedupe, enforce types ---
silver_df = (spark.table("main.events.bronze_events")
 .dropDuplicates(["event_id"])
 .withColumn("event_ts", to_timestamp("event_ts"))
 .filter(col("event_ts").isNotNull())
 .select("event_id", "user_id", "event_type", "event_ts"))

silver_df.write.format("delta").mode("overwrite") \
 .saveAsTable("main.events.silver_events")

# --- GOLD: business aggregate for a dashboard ---
gold_df = (spark.table("main.events.silver_events")
 .groupBy(window("event_ts", "1 hour"), "event_type")
 .count())

gold_df.write.format("delta").mode("overwrite") \
 .saveAsTable("main.events.gold_hourly_event_counts")
```
*A minimal but realistic bronze → silver → gold pipeline in PySpark.*

> **Interview talking point:** emphasize that bronze is append-only and never deleted (it's your replay/audit source of truth), silver enforces data quality contracts, and gold is denormalized specifically for the consumption pattern (dashboard, ML feature store, reverse-ETL) rather than for storage efficiency.

---

## 5. Ingestion Patterns — Auto Loader & COPY INTO

### 5.1 Auto Loader (`cloudFiles`)

Auto Loader incrementally and efficiently ingests new files as they land in cloud storage, without you having to list an entire directory on every run. It tracks which files have already been processed using scalable checkpointed state (RocksDB-backed), and can also auto-detect and evolve schema.

```python
df = (spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", "/mnt/checkpoints/events/schema")
 .option("cloudFiles.inferColumnTypes", "true")
 .load("/mnt/raw/events/"))

(df.writeStream
 .format("delta")
 .option("checkpointLocation", "/mnt/checkpoints/events/bronze")
 .trigger(availableNow=True)  # process what's new, then stop (batch-like)
 .toTable("main.events.bronze_events"))
```
*Auto Loader with `trigger(availableNow=True)` behaves like an efficient incremental batch job.*

- **File discovery modes:** directory listing (default, simple) vs. file notification (subscribes to cloud events like S3 SQS/Azure Event Grid) — use notification mode for very high file-arrival-rate directories.
- **Schema evolution modes:** `addNewColumns` (default), `rescue` (unexpected data goes to a `_rescued_data` column instead of failing the job), `failOnNewColumns`, `none`.
- Auto Loader is the recommended replacement for older `COPY INTO`-only or manual directory-listing ingestion at scale.

> **Q: How does Auto Loader avoid re-listing an entire directory on every run?**
> It maintains scalable, checkpointed state (backed by RocksDB) of which files have already been discovered/processed, and can optionally use cloud-native file-notification services (e.g., S3 event notifications via SQS) instead of directory listing for very high file volumes.

### 5.2 COPY INTO

`COPY INTO` is a simpler, SQL-native, idempotent, re-runnable bulk-load command — good for smaller numbers of files or ad-hoc/one-off loads where full Auto Loader streaming infrastructure is overkill.

```sql
COPY INTO main.sales.orders_bronze
FROM '/mnt/raw/orders/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');
```
*Re-running `COPY INTO` is safe: it only loads files it hasn't already loaded.*

> **Q: Auto Loader vs COPY INTO?**
> `COPY INTO` tracks state via table history and is best for thousands of files loaded periodically; Auto Loader is a streaming source built for millions of files / continuous arrival and scales file discovery independent of load size.

#### System design — detecting a broken upstream schema before it corrupts silver/gold
- Rely on Delta's default schema enforcement at the bronze-to-silver boundary so unexpected changes fail loudly instead of silently corrupting data.
- Use Auto Loader's `rescue` schema evolution mode so genuinely new/unexpected fields land in a `_rescued_data` column for inspection instead of breaking the job outright.
- Add DLT `expect_or_fail` (or equivalent assertions in a plain job) on critical columns so a job fails fast with a clear data-quality signal, and wire job failure to an alert.

---

## 6. Structured Streaming on Databricks

Structured Streaming treats a stream as an unbounded table that is continuously appended to. The same DataFrame API you use for batch works for streaming — only the read/write ends differ (`readStream` / `writeStream`).

### 6.1 Triggers

| Trigger | Behavior |
|---|---|
| Default (no trigger set) | Micro-batches processed as fast as possible, back-to-back |
| `processingTime='1 minute'` | Fixed micro-batch interval |
| `availableNow=True` | Processes all currently available data in batches, then stops — ideal for scheduled jobs replacing 24/7 clusters |
| `continuous='1 second'` | Experimental low-latency continuous processing (limited operator support) |

> **Q: What does `trigger(availableNow=True)` do and why is it useful?**
> It processes all data currently available as a series of micro-batches and then stops the stream, giving you the exactly-once/incremental semantics of streaming with the cost and operational model of a scheduled batch job.

### 6.2 Checkpointing & exactly-once semantics

- Every streaming query needs a `checkpointLocation` — it stores offsets processed and state, enabling exactly-once processing into Delta sinks after failures/restarts.
- Never share one checkpoint directory across two different streaming queries.
- Changing the query logic significantly (e.g., adding a stateful aggregation) after a checkpoint exists can break restart compatibility.

### 6.3 Watermarking for late data

```python
from pyspark.sql.functions import window, col

agg_df = (events_df
 .withWatermark("event_ts", "10 minutes")  # tolerate up to 10 min of lateness
 .groupBy(window("event_ts", "5 minutes"), "event_type")
 .count())

(agg_df.writeStream
 .format("delta")
 .outputMode("append")  # append works once watermark closes a window
 .option("checkpointLocation", "/mnt/checkpoints/agg")
 .toTable("main.events.gold_5min_counts"))
```
*Watermarking bounds how long Spark keeps state for a window before discarding/emitting it.*

> **Q: Why does a streaming aggregation need a watermark before you can use append output mode?**
> Without a watermark, Spark doesn't know when a window is "done" and would have to keep all state forever; a watermark tells Spark how long to wait for late data before finalizing and emitting a window's result, which append mode requires.

#### System design — handling late-arriving data in an hourly sales aggregation
- Add a watermark (e.g., `withWatermark('event_ts', '2 hours')`) sized to your realistic lateness tail, so Spark keeps state for in-flight windows long enough to catch most late events before finalizing them.
- For events arriving after the watermark closes the window, either accept the small under-count (documented business trade-off) or run a periodic backfill/reconciliation job that recomputes recent windows from silver on a schedule.
- Store **event time** (not ingestion time) as the windowing column so late-arriving-but-old events land in the correct historical window rather than today's window.

### 6.4 Output modes

| Mode | Use case |
|---|---|
| `append` | Only new rows written since last trigger (most common; required for most joins/windows with watermark) |
| `update` | Only rows that changed since last trigger (aggregations without needing full watermark append semantics) |
| `complete` | Entire result table rewritten every trigger (small aggregations/dashboards only) |

### 6.5 Stream-static and stream-stream joins

```python
# Stream-static join: enrich streaming orders with a (slowly changing) static dimension table
customers = spark.table("main.sales.customers")  # static/batch DataFrame
enriched = (orders_stream
 .join(customers, "customer_id", "left"))

# Stream-stream join needs watermarks on both sides
join_cond = "order_id = payment_id AND payment_ts BETWEEN order_ts AND order_ts + interval 30 minutes"
joined = (orders_stream.withWatermark("order_ts", "10 minutes")
 .join(payments_stream.withWatermark("payment_ts", "10 minutes"),
 expr(join_cond), "inner"))
```

> **Q: What's the difference between stream-static and stream-stream joins?**
> A stream-static join enriches a streaming DataFrame with a batch/reference table (no watermark needed on the static side). A stream-stream join requires watermarks on both streams so Spark can bound how much state it buffers while waiting for matching events.

#### System design — migrating a daily batch job to near-real-time
- Convert batch reads to Auto Loader streaming reads (`cloudFiles`) so the source side is already incremental.
- Keep the same transformation logic, but move it into a DLT pipeline or a `readStream`/`writeStream` job with a sensible trigger — start with `trigger(availableNow=True)` on a tighter schedule before committing to always-on continuous streaming, to de-risk the change.
- Validate output parity against the old batch job on historical data before cutting downstream consumers over.
- Add monitoring on checkpoint lag / batch duration to catch the pipeline falling behind.

---

## 7. Delta Live Tables (DLT)

DLT (now often surfaced in the product as part of "Lakeflow Declarative Pipelines") lets you declare *what* each table should contain; Databricks figures out *how* and in what order to execute it, handles orchestration, retries, and cluster management, and gives you built-in data quality enforcement and lineage — without you writing imperative Workflow DAGs by hand.

### 7.1 Declaring a pipeline

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Raw events landed via Auto Loader")
def bronze_events():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/raw/events/"))

@dlt.table(comment="Cleaned, deduplicated events")
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect("valid_ts", "event_ts IS NOT NULL")  # logged, not dropped
def silver_events():
    return (dlt.read_stream("bronze_events")
        .dropDuplicates(["event_id"]))

@dlt.table(comment="Hourly aggregate for dashboards")
def gold_hourly_counts():
    return (dlt.read("silver_events")
        .groupBy("event_type")
        .count())
```
*DLT infers the DAG automatically from `dlt.read()`/`dlt.read_stream()` dependencies between tables.*

### 7.2 Data quality with Expectations

| Expectation | Behavior on failing rows |
|---|---|
| `@dlt.expect(name, condition)` | Row is kept; failure is only logged/metriced |
| `@dlt.expect_or_drop(name, condition)` | Failing rows are silently dropped from the output |
| `@dlt.expect_or_fail(name, condition)` | Pipeline run fails immediately if any row violates the condition |

> **Q: What's the difference between `expect`, `expect_or_drop`, and `expect_or_fail` in DLT?**
> - `expect`: logs violations but keeps the row.
> - `expect_or_drop`: silently drops rows that violate the rule.
> - `expect_or_fail`: fails the entire pipeline run on any violation.

### 7.3 DLT vs. hand-rolled notebooks + Workflows

- DLT auto-manages incremental processing state for streaming tables — you don't write your own checkpoint/merge logic for basic incremental loads.
- Built-in data quality metrics surface in the pipeline UI (rows passed/dropped/failed per expectation) — no custom logging needed.
- Supports both **Triggered** (batch-like, runs once and stops) and **Continuous** pipeline modes.
- Handles cluster lifecycle, retries, and backfills automatically; still lets you drop to plain PySpark/SQL inside each table definition.

> **Q: What problem does DLT solve that plain notebooks + Workflows don't?**
> DLT lets you declare table definitions and dependencies, and it automatically infers the execution DAG, manages incremental/streaming state, handles retries and cluster lifecycle, and gives you built-in, queryable data-quality metrics via Expectations — all without hand-written orchestration code.

---

## 8. Orchestration — Databricks Workflows

Workflows (Jobs) is Databricks' native orchestrator: a job is a DAG of tasks, where each task can be a notebook, a Python script/wheel, a JAR, a SQL query/dashboard refresh, or a DLT pipeline trigger. Tasks declare dependencies ("run after task X succeeds"), and each task can run on its own job cluster.

- **Task dependencies** form a DAG — e.g., `bronze_ingest → silver_clean → [gold_sales, gold_inventory]` fanning out in parallel.
- **Job clusters per task** (vs. a shared all-purpose cluster) isolate failures and right-size compute/cost per task.
- **Retries & timeouts** can be set per task; failed tasks can trigger email/Slack/webhook alerts.
- **Parameters** can be passed into notebooks (widgets) or scripts, enabling one job definition to run for multiple environments/dates.
- **Triggers:** a job can run on a cron schedule, on file arrival in a storage location, on completion of another job, or via the REST API/external orchestrator (e.g., Airflow) call.
- **Repos / Databricks Asset Bundles (DABs)** let you define jobs, clusters, and pipelines as code (YAML) and deploy via CI/CD instead of clicking through the UI.

> **Q: When would you choose Databricks Workflows over Airflow, or vice versa?**
> Workflows is simplest when the pipeline is entirely within Databricks (notebooks, DLT, SQL) since it manages clusters natively with no extra infrastructure. Airflow (or similar) is a better fit when you need to orchestrate across many heterogeneous systems beyond Databricks; many teams use Airflow to trigger a Databricks job as one node in a larger DAG.

---

## 9. Unity Catalog & Governance

Unity Catalog (UC) is Databricks' centralized governance layer, spanning every workspace in an account. Before UC, permissions and metadata were siloed per-workspace (the legacy Hive metastore) — UC is now the default.

> **Q: What is Unity Catalog and what problem did it solve?**
> A centralized governance layer spanning every workspace in an account, providing a 3-level namespace (`catalog.schema.table`), fine-grained ANSI SQL access control, automatic column-level lineage, and governed access to files via Volumes — replacing the old per-workspace, siloed Hive metastore model.

### 9.1 Three-level namespace

```sql
catalog.schema.table
-- example:
main.sales.orders_bronze

SELECT * FROM main.sales.orders_bronze;

USE CATALOG main;
USE SCHEMA sales;
SELECT * FROM orders_bronze;  -- now resolves via current catalog/schema
```
*Catalog sits above schema (database), giving a natural boundary for environments (dev/staging/prod) or business units.*

### 9.2 Access control

```sql
GRANT SELECT ON TABLE main.sales.orders_bronze TO `data-analysts`;
GRANT MODIFY, SELECT ON SCHEMA main.sales TO `data-engineers`;
REVOKE SELECT ON TABLE main.sales.orders_bronze FROM `contractor-group`;

-- Row/column level security
CREATE VIEW main.sales.orders_masked AS
SELECT order_id, customer_id,
  CASE WHEN is_member('finance-team') THEN amount ELSE NULL END AS amount
FROM main.sales.orders_bronze;
```
*UC uses ANSI-SQL-standard GRANT/REVOKE; permissions are inherited catalog → schema → table.*

### 9.3 Other Unity Catalog capabilities

- **Data lineage** — automatic, column-level lineage graphs showing which notebooks/jobs read and wrote each table.
- **Volumes** — governed access to non-tabular files (e.g., raw files staged for Auto Loader, images, PDFs) without exposing raw cloud paths.
- **Delta Sharing** — open protocol to securely share live Delta tables with other organizations/tools without copying data.
- **System tables** — built-in Delta tables under the `system` catalog exposing audit logs, billing, and query history for governance/FinOps.
- **External locations & storage credentials** — centrally-managed, auditable mapping between cloud storage paths and the identities allowed to access them.

#### System design — multi-environment (dev/staging/prod) isolation
- Use separate Unity Catalog **catalogs** per environment (dev, staging, prod) rather than separate schemas, so permissions and lineage are cleanly isolated.
- Manage jobs, clusters, and DLT pipelines as code with **Databricks Asset Bundles (DABs)**, parameterizing the target catalog per environment and deploying via CI/CD.
- Use **cluster policies** to prevent dev/staging workloads from using production-sized/costly clusters.

---

## 10. Performance Tuning & Optimization

> This is where senior/practical interviews spend the most time — expect follow-up "why" questions on every technique below.

### 10.1 Partitioning

- Partition by a **low-cardinality** column that's frequently filtered on (e.g., `date`), not a high-cardinality one (e.g., `customer_id`) — over-partitioning creates the small-file problem.
- Rule of thumb: aim for partitions that hold at least **~1–10 GB** of data each; too many tiny partitions hurt more than they help.
- For high-cardinality access patterns, prefer **Z-ORDER / Liquid Clustering** over physical partitioning.

> **Q: Partition by date or by customer_id — which and why?**
> Partition by date (or another low-cardinality, frequently-filtered column); `customer_id` is high-cardinality and would create the small-file problem across thousands of tiny partitions — for high-cardinality filter/join columns, use Z-ORDER or Liquid Clustering instead of physical partitioning.

### 10.2 Joins & shuffles

```python
from pyspark.sql.functions import broadcast

# Force a broadcast join when one side is small (< ~10s of MB, tune via
# spark.sql.autoBroadcastJoinThreshold) to avoid an expensive shuffle
big_df.join(broadcast(small_dim_df), "customer_id")
```

- A **shuffle** (wide transformation like `groupBy`/`join`/`repartition`) moves data across the network between executors — the single biggest cost in most Spark jobs.
- **Broadcast joins** avoid a shuffle entirely by sending a small table to every executor's memory.
- **Data skew** (one key with far more rows than others) makes one task/partition a straggler; fixes include salting the skewed key, or enabling Adaptive Query Execution (AQE) skew join handling.
- **Adaptive Query Execution (AQE)** is on by default in modern DBR — it re-optimizes the physical plan at runtime (coalescing shuffle partitions, switching join strategies, handling skew) based on actual data statistics.

> **Q: A join is running slowly. What do you check first?**
> - Whether one side is small enough to broadcast (avoiding a shuffle entirely).
> - The Spark UI for data skew (one task taking far longer than others on the same stage).
> - Whether Adaptive Query Execution (AQE) is enabled to auto-handle skew and shuffle partition sizing.
> - Whether the join keys need repartitioning or salting if skew is severe and AQE alone isn't enough.

> **Q: What causes data skew and how do you fix it?**
> A small number of key values have disproportionately more rows than others (e.g., a default/null `customer_id` absorbing millions of rows), so one task/partition becomes a straggler. Fixes: enable/verify AQE skew join handling, salt the skewed key with a random suffix and explode the small side accordingly, or filter/handle the hot key separately.

### 10.3 Caching

```python
df.cache()  # or df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()  # action to materialize the cache
spark.sql("CACHE SELECT * FROM main.sales.orders_bronze")  # SQL cache
```
*Cache only DataFrames reused multiple times in the same session; caching a one-shot DataFrame wastes memory.*

### 10.4 File formats & layout

- Delta/Parquet are columnar and support predicate pushdown + column pruning — select only the columns you need.
- Run `OPTIMIZE` regularly on tables with frequent small writes (especially streaming sinks) to fix the small-file problem.
- **Deletion vectors** (modern Delta feature) let `UPDATE`/`DELETE`/`MERGE` mark rows as deleted without immediately rewriting entire Parquet files, dramatically speeding up those operations; a later `OPTIMIZE` physically compacts them.

> **Q: Why might a Delta table with lots of small files hurt performance, and how do you fix it?**
> Many small files mean more metadata overhead and more task-scheduling overhead relative to actual data processed, since each file typically maps to at least one task; run `OPTIMIZE` (with ZORDER or Liquid Clustering as needed) to compact them into fewer, larger files.

### 10.5 Cluster sizing & Photon

- Prefer more, smaller executors for high-parallelism I/O-bound work; fewer, larger executors for memory-heavy shuffles/joins.
- Enable **Photon** for SQL/DataFrame-heavy workloads — it's a drop-in vectorized engine that typically cuts cost and runtime with no code changes.
- Use **job clusters** (not always-on all-purpose clusters) for production pipelines to avoid paying for idle time.
- Watch the Spark UI's stage/task timeline for stragglers (skew) and spill metrics (undersized executor memory forcing disk spill).

> **Interview one-liner:** "I'd start by looking at the Spark UI for shuffle spill and task-time skew, check whether small-file counts are high (needs OPTIMIZE), confirm AQE and Photon are enabled, and make sure joins against small dimension tables are broadcast."

---

## 11. PySpark Practical Cheat Sheet

### 11.1 Core DataFrame operations

```python
from pyspark.sql import functions as F

df.select("a", "b").filter(F.col("amount") > 100)
df.withColumn("amount_usd", F.col("amount") * F.lit(1.1))
df.withColumnRenamed("old_name", "new_name")
df.drop("unused_col")
df.dropDuplicates(["order_id"])
df.na.fill({"amount": 0, "status": "unknown"})
df.groupBy("region").agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
df.orderBy(F.col("amount").desc())
df.join(other_df, on="customer_id", how="left")
df.union(other_df)          # schemas must match by position
df.unionByName(other_df)    # schemas matched by column name (safer)
```

### 11.2 Window functions

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

w = Window.partitionBy("customer_id").orderBy("order_date")

df.withColumn("row_num", F.row_number().over(w)) \
  .withColumn("running_total", F.sum("amount").over(w)) \
  .withColumn("prev_amount", F.lag("amount", 1).over(w)) \
  .withColumn("rank_by_amount",
      F.rank().over(Window.partitionBy("customer_id").orderBy(F.col("amount").desc())))
```
*Window functions are how you implement top-N-per-group, running totals, and SCD Type 2 versioning logic.*

### 11.3 UDFs — and why to avoid them

```python
# Standard Python UDF: works but is slow (row-by-row serialization, no Catalyst optimization)
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def normalize(s):
    return s.strip().lower() if s else None

df.withColumn("clean_name", normalize("name"))

# Prefer built-in functions when possible (they're Catalyst-optimized and vectorized):
df.withColumn("clean_name", F.lower(F.trim(F.col("name"))))

# When a UDF is unavoidable, use a Pandas UDF (vectorized, Arrow-based, much faster than row UDFs)
import pandas as pd

@F.pandas_udf(StringType())
def normalize_pd(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower()
```

### 11.4 Reading/writing common formats

```python
spark.read.format("delta").load("/path/or/table")
spark.read.option("header", True).option("inferSchema", True).csv("/path")
spark.read.schema(explicit_schema).json("/path")  # always prefer explicit schema over inferSchema in prod

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/path")
df.write.partitionBy("year", "month").parquet("/path")
```

### 11.5 Repartition vs. coalesce

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| **Shuffle?** | Yes (full shuffle) | No (merges existing partitions in place) |
| **Can increase partitions?** | Yes | No — only reduces |
| **Typical use** | Before a wide operation needing more parallelism, or to fix skew | Reducing partition count before a write, e.g., to avoid too many small output files |

---

## 12. Databricks SQL Essentials

Even in a "Data Engineering" interview, expect SQL questions — especially CTEs, window functions, and `MERGE`, since these are how most production transformations and dbt-on-Databricks models are written.

```sql
-- CTEs for readable multi-step logic
WITH recent_orders AS (
  SELECT * FROM main.sales.orders_bronze WHERE order_date >= current_date() - INTERVAL 30 DAYS
),
customer_totals AS (
  SELECT customer_id, SUM(amount) AS total_spent
  FROM recent_orders
  GROUP BY customer_id
)
SELECT c.customer_id, c.total_spent
FROM customer_totals c
WHERE c.total_spent > 1000
ORDER BY c.total_spent DESC;

-- Window function: top order per customer
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
  FROM main.sales.orders_bronze
) WHERE rn = 1;

-- SCD Type 2 pattern (close old record, insert new version) via MERGE
MERGE INTO main.sales.customers_scd2 AS t
USING staged_customers AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.email <> s.email OR t.address <> s.address) THEN
  UPDATE SET t.is_current = false, t.valid_to = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, address, valid_from, valid_to, is_current)
  VALUES (s.customer_id, s.email, s.address, current_timestamp(), NULL, true);
```
*Note: the SCD Type 2 example above only closes changed records — in production you'd run a second INSERT-only `MERGE` (or a follow-up statement) to add the new current row after closing the old one.*

#### System design — implementing SCD Type 2 for a slowly-changing dimension
- Add `valid_from`, `valid_to`, and `is_current` columns to the dimension table.
- On each load, `MERGE INTO` the target: where the business key matches AND `is_current = true` AND any tracked attribute differs, `UPDATE` to set `is_current = false` and `valid_to = now()`.
- In a second pass (or `WHEN NOT MATCHED` clause), `INSERT` the new current row with `valid_from = now()` and `valid_to = NULL`.
- Downstream fact tables join to the dimension on business key + a timestamp `BETWEEN valid_from AND valid_to` for point-in-time-correct historical joins.

> **Q: You need to implement SCD Type 2 for a dimension table that changes slowly. Walk through it.**
> See the four steps above — this is one of the most commonly asked walkthrough questions in Databricks SQL/DE interviews.

---

## 13. Quick-Reference Command Cheat Sheet

### Delta Lake
```sql
CREATE TABLE cat.schema.tbl (...) USING DELTA PARTITIONED BY (col);
MERGE INTO target USING source ON ... WHEN MATCHED ... WHEN NOT MATCHED ...;
OPTIMIZE cat.schema.tbl ZORDER BY (col);
ALTER TABLE cat.schema.tbl CLUSTER BY (col);        -- Liquid Clustering
VACUUM cat.schema.tbl RETAIN 168 HOURS;
SELECT * FROM tbl VERSION AS OF n;
SELECT * FROM tbl TIMESTAMP AS OF 'ts';
RESTORE TABLE tbl TO VERSION AS OF n;
DESCRIBE HISTORY tbl;
```

### Governance (Unity Catalog)
```sql
USE CATALOG main; USE SCHEMA sales;
GRANT SELECT ON TABLE cat.schema.tbl TO `group`;
REVOKE SELECT ON TABLE cat.schema.tbl FROM `group`;
```

### Ingestion
```python
spark.readStream.format("cloudFiles").option("cloudFiles.format", "json") \
  .option("cloudFiles.schemaLocation", "...").load("/path")
```
```sql
COPY INTO tbl FROM '/path' FILEFORMAT = JSON COPY_OPTIONS ('mergeSchema'='true');
```

### Streaming
```python
df.writeStream.format("delta").option("checkpointLocation", "...") \
  .trigger(availableNow=True).toTable("cat.schema.tbl")

df.withWatermark("event_ts", "10 minutes").groupBy(window("event_ts", "5 minutes")).count()
```

### Performance
```python
from pyspark.sql.functions import broadcast
big_df.join(broadcast(small_df), "key")

df.cache(); df.count()
df.repartition(200)   # shuffle, can increase partitions
df.coalesce(10)        # no shuffle, only decrease
```

---

## Mental Model Recap

| Layer | Key Concept |
|---|---|
| **Storage** | Delta Lake = Parquet + `_delta_log` (ACID, time travel) |
| **Ingestion** | Auto Loader (streaming, scale) vs. COPY INTO (simple, idempotent batch) |
| **Processing** | Structured Streaming (imperative) vs. DLT (declarative) |
| **Orchestration** | Databricks Workflows (native) vs. Airflow (cross-system) |
| **Governance** | Unity Catalog: `catalog.schema.table`, GRANT/REVOKE, lineage, Volumes |
| **Performance** | AQE + Photon + broadcast joins + OPTIMIZE/Z-ORDER + right-sized clusters |
