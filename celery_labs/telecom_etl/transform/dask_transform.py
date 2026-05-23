import os
import logging
from datetime import datetime

import dask.dataframe as dd
import pandas as pd
import pymonetdb
import duckdb
from celery import shared_task
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/telecom.db")
TARGET_TABLE = "transformed_telecom_usage"


# ─── Connection helpers ───────────────────────────────────────────────────────

def get_monetdb_conn():
    return pymonetdb.connect(
        hostname=os.getenv("MONETDB_HOST", "monetdb"),
        port=int(os.getenv("MONETDB_PORT", 50000)),
        database=os.getenv("MONETDB_DB", "telecom_db"),
        username=os.getenv("MONETDB_USER", "monetdb"),
        password=os.getenv("MONETDB_PASS", "monetdb_admin"),
        autocommit=False,
    )


# ─── TASK 1: Extract from MonetDB into Dask DataFrame ────────────────────────

@shared_task(bind=True, name="telecom_etl.transform.extract_from_monetdb")
def extract_from_monetdb(self):
    """Read raw_telecom_internet_usage from MonetDB into a Pandas df, wrap in Dask."""
    logger.info("Transform Step 1: Extracting from MonetDB...")
    try:
        conn = get_monetdb_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM raw_telecom_internet_usage")
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        pdf = pd.DataFrame(rows, columns=cols)
        logger.info(f"Extracted {len(pdf)} rows from MonetDB")

        # Save to parquet as intermediate — Dask works best with files
        os.makedirs("/app/data", exist_ok=True)
        pdf.to_parquet("/app/data/raw_extract.parquet", index=False)

        return {"status": "extracted", "rows": len(pdf), "path": "/app/data/raw_extract.parquet"}

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise self.retry(exc=e, countdown=30, max_retries=3)


# ─── TASK 2: Transform using Dask ────────────────────────────────────────────

@shared_task(bind=True, name="telecom_etl.transform.transform_data")
def transform_data(self, extract_result):
    """Apply all cleaning and transformation operations using Dask."""
    if extract_result.get("status") != "extracted":
        raise Exception(f"Extraction did not complete: {extract_result}")

    logger.info("Transform Step 2: Applying transformations with Dask...")
    try:
        ddf = dd.read_parquet(extract_result["path"])

        # ── 1. phone_number: remove rows where length > 10, cast to int ──────
        ddf = ddf[ddf["phone_number"].str.len() <= 10]
        ddf["phone_number"] = ddf["phone_number"].astype("int64")

        # ── 2. Uppercase string columns ──────────────────────────────────────
        for col in ["plan_type", "device_type", "customer_status", "region", "source_system"]:
            ddf[col] = ddf[col].str.upper().str.strip()

        # ── 3. Parse date/timestamp columns ──────────────────────────────────
        ddf["signup_date"] = dd.to_datetime(ddf["signup_date"], format="%Y-%m-%d", errors="coerce").dt.date
        ddf["session_start"] = dd.to_datetime(ddf["session_start"], errors="coerce")
        ddf["session_end"] = dd.to_datetime(ddf["session_end"], errors="coerce")

        # ── 4. Drop nulls ─────────────────────────────────────────────────────
        ddf = ddf.dropna()

        # ── 5. Remove exact duplicate rows ───────────────────────────────────
        ddf = ddf.drop_duplicates()

        # ── 6. Keep latest record per usage_id (by ingestion_timestamp) ──────
        # Dask doesn't support groupby+idxmax directly — compute to pandas for this
        pdf = ddf.compute()
        pdf["ingestion_timestamp"] = pd.to_datetime(pdf["ingestion_timestamp"])
        pdf = pdf.sort_values("ingestion_timestamp", ascending=False)
        pdf = pdf.drop_duplicates(subset=["usage_id"], keep="first")
        pdf = pdf.sort_values("usage_id").reset_index(drop=True)

        # Back to Dask
        ddf = dd.from_pandas(pdf, npartitions=2)

        # ── 7. Numeric range filters ──────────────────────────────────────────
        ddf = ddf[ddf["data_used_mb"] >= 0]
        ddf = ddf[(ddf["upload_speed_mbps"] >= 0) & (ddf["upload_speed_mbps"] <= 100)]
        ddf = ddf[(ddf["download_speed_mbps"] >= 0) & (ddf["download_speed_mbps"] <= 500)]
        ddf = ddf[(ddf["signal_strength"] >= -120) & (ddf["signal_strength"] <= -40)]

        # ── 8. session_duration_minutes ───────────────────────────────────────
        ddf["session_duration_minutes"] = (
            (ddf["session_end"] - ddf["session_start"]).dt.total_seconds() / 60
        ).round(2)

        # ── 9. data_usage_level ───────────────────────────────────────────────
        def classify_data_usage(mb):
            if mb < 500:
                return "LOW"
            elif mb <= 2000:
                return "MEDIUM"
            else:
                return "HIGH"

        ddf["data_usage_level"] = ddf["data_used_mb"].apply(
            classify_data_usage, meta=("data_usage_level", "str")
        )

        # ── 10. signal_strength_quality ───────────────────────────────────────
        def classify_signal(val):
            if val >= -60:
                return "HIGH"
            elif val >= -90:
                return "AVERAGE"
            else:
                return "LOW"

        ddf["signal_strength_quality"] = ddf["signal_strength"].apply(
            classify_signal, meta=("signal_strength_quality", "str")
        )

        # ── 11. upload_speed_quality ──────────────────────────────────────────
        def classify_upload(val):
            if val >= 50:
                return "HIGH"
            elif val >= 20:
                return "AVERAGE"
            else:
                return "LOW"

        ddf["upload_speed_quality"] = ddf["upload_speed_mbps"].apply(
            classify_upload, meta=("upload_speed_quality", "str")
        )

        # ── 12. download_speed_quality ────────────────────────────────────────
        def classify_download(val):
            if val >= 200:
                return "HIGH"
            elif val >= 50:
                return "AVERAGE"
            else:
                return "LOW"

        ddf["download_speed_quality"] = ddf["download_speed_mbps"].apply(
            classify_download, meta=("download_speed_quality", "str")
        )

        # ── 13. Year, month, week from signup_date ────────────────────────────
        signup_dt = dd.to_datetime(ddf["signup_date"].astype("str"), errors="coerce")
        ddf["signup_year"] = signup_dt.dt.year
        ddf["signup_month"] = signup_dt.dt.month
        ddf["signup_week"] = signup_dt.dt.isocalendar().week.astype("int32")

        # ── Compute final result ──────────────────────────────────────────────
        final_pdf = ddf.compute()
        logger.info(f"Transformation complete: {len(final_pdf)} rows after cleaning")

        out_path = "/app/data/transformed.parquet"
        final_pdf.to_parquet(out_path, index=False)

        return {"status": "transformed", "rows": len(final_pdf), "path": out_path}

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise self.retry(exc=e, countdown=30, max_retries=3)


# ─── TASK 3: Load into DuckDB ─────────────────────────────────────────────────

@shared_task(bind=True, name="telecom_etl.transform.load_to_duckdb")
def load_to_duckdb(self, transform_result):
    """Load transformed parquet into DuckDB as the analytics target."""
    if transform_result.get("status") != "transformed":
        raise Exception(f"Transform did not complete: {transform_result}")

    logger.info(f"Transform Step 3: Loading into DuckDB at {DUCKDB_PATH}...")
    try:
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
        parquet_path = transform_result["path"]

        conn = duckdb.connect(DUCKDB_PATH)

        # Drop and recreate for idempotency
        conn.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

        # Create table from parquet with usage_id as primary key
        conn.execute(f"""
            CREATE TABLE {TARGET_TABLE} AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)

        # Add primary key constraint via unique index (DuckDB style)
        conn.execute(f"""
            CREATE UNIQUE INDEX idx_usage_id ON {TARGET_TABLE} (usage_id)
        """)

        row_count = conn.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
        conn.close()

        logger.info(f"Loaded {row_count} rows into DuckDB table '{TARGET_TABLE}'")
        return {"status": "load_complete", "rows_in_duckdb": row_count}

    except Exception as e:
        logger.error(f"DuckDB load failed: {e}")
        raise self.retry(exc=e, countdown=30, max_retries=3)


# ─── Entry point ──────────────────────────────────────────────────────────────

@shared_task(bind=True, name="telecom_etl.transform.run_transform_pipeline")
def run_transform_pipeline(self):

    from celery import chain
    logger.info("Starting Transform Pipeline: MonetDB → Dask → DuckDB")
    job = chain(
        extract_from_monetdb.s(),
        transform_data.s(),
        load_to_duckdb.s(),
    )
    return job.apply_async()
