import os
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
import pymonetdb
from celery import shared_task, chain
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000  # rows per batch
TABLE_NAME = "raw_telecom_internet_usage"

# ─── Connection helpers ───────────────────────────────────────────────────────

def get_neon_conn():
    conn_str = os.getenv("NEON_DB_KEY")
    if not conn_str:
        raise ValueError("NEON_DB_KEY not set in environment")
    return psycopg2.connect(conn_str, connect_timeout=30)


def get_monetdb_conn():
    return pymonetdb.connect(
        hostname=os.getenv("MONETDB_HOST", "monetdb"),
        port=int(os.getenv("MONETDB_PORT", 50000)),
        database=os.getenv("MONETDB_DB", "telecom_db"),
        username=os.getenv("MONETDB_USER", "monetdb"),
        password=os.getenv("MONETDB_PASS", "monetdb_admin"),
        autocommit=False,
    )


# ─── DDL ─────────────────────────────────────────────────────────────────────

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw_telecom_internet_usage (
    usage_id             BIGINT,
    customer_id          BIGINT,
    phone_number         VARCHAR(20),
    region               VARCHAR(50),
    plan_type            VARCHAR(30),
    device_type          VARCHAR(30),
    customer_status      VARCHAR(30),
    signup_date          VARCHAR(30),
    tower_id             VARCHAR(20),
    session_start        VARCHAR(50),
    session_end          VARCHAR(50),
    data_used_mb         DECIMAL(10,2),
    network_type         VARCHAR(20),
    signal_strength      INT,
    upload_speed_mbps    DECIMAL(8,2),
    download_speed_mbps  DECIMAL(8,2),
    source_system        VARCHAR(50),
    ingestion_timestamp  TIMESTAMP
)
"""

MIGRATION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS migration_log (
    table_name      VARCHAR(100),
    status          VARCHAR(20),
    rows_in_source  BIGINT,
    rows_loaded     BIGINT,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP
)
"""


# ─── PHASE 1: Inspect & Prepare ──────────────────────────────────────────────

@shared_task(bind=True, name="telecom_etl.migration.inspect_neon_schema")
def inspect_neon_schema(self):
    """Connect to Neon, get row count and column info."""
    logger.info("Phase 1: Inspecting Neon schema...")
    try:
        conn = get_neon_conn()
        cur = conn.cursor()

        cur.execute(f"SELECT COUNT(*) FROM public.{TABLE_NAME}")
        row_count = cur.fetchone()[0]
        logger.info(f"Neon '{TABLE_NAME}' has {row_count} rows")

        cur.execute("""
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (TABLE_NAME,))

        columns = [
            {"name": r[0], "data_type": r[1], "max_length": r[2],
             "numeric_precision": r[3], "numeric_scale": r[4], "nullable": r[5]}
            for r in cur.fetchall()
        ]

        cur.close()
        conn.close()

        return {
            "table_name": TABLE_NAME,
            "row_count": row_count,
            "columns": columns,
            "inspected_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Phase 1 inspection failed: {e}")
        raise self.retry(exc=e, countdown=30, max_retries=3)


@shared_task(bind=True, name="telecom_etl.migration.prepare_monetdb")
def prepare_monetdb(self, manifest):
    """Create target table and migration_log in MonetDB. Idempotent."""
    logger.info("Phase 1: Preparing MonetDB...")
    try:
        conn = get_monetdb_conn()
        cur = conn.cursor()

        cur.execute(RAW_TABLE_DDL)
        cur.execute(MIGRATION_LOG_DDL)

        # Idempotency — only insert log entry if no PENDING/IN_PROGRESS exists
        cur.execute("""
            SELECT COUNT(*) FROM migration_log
            WHERE table_name = %s AND status IN ('PENDING', 'IN_PROGRESS', 'DONE')
        """, (manifest["table_name"],))
        existing = cur.fetchone()[0]

        if existing == 0:
            cur.execute("""
                INSERT INTO migration_log
                    (table_name, status, rows_in_source, rows_loaded, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (manifest["table_name"], "PENDING", manifest["row_count"], 0,
                  datetime.utcnow(), None))
            logger.info("Migration log entry created: PENDING")
        else:
            logger.info(f"Migration log already has {existing} entry — skipping insert")

        conn.commit()
        cur.close()
        conn.close()

        logger.info("Phase 1 complete: MonetDB ready")
        return {
            "status": "phase1_complete",
            "table_name": manifest["table_name"],
            "row_count": manifest["row_count"],
        }

    except Exception as e:
        logger.error(f"Phase 1 MonetDB prep failed: {e}")
        raise self.retry(exc=e, countdown=30, max_retries=3)


# ─── PHASE 2: Extract & Load ─────────────────────────────────────────────────

@shared_task(bind=True, name="telecom_etl.migration.extract_and_load")
def extract_and_load(self, phase1_result):
    """
    Extract from Neon in chunks, load into MonetDB.
    Updates migration_log status throughout.
    """
    if phase1_result.get("status") != "phase1_complete":
        raise Exception(f"Phase 1 did not complete: {phase1_result}")

    table_name = phase1_result["table_name"]
    total_rows = phase1_result["row_count"]
    logger.info(f"Phase 2: Starting extract & load for {table_name} ({total_rows} rows)")

    neon_conn = get_neon_conn()
    monet_conn = get_monetdb_conn()

    try:
        neon_cur = neon_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        monet_cur = monet_conn.cursor()

        # Mark as IN_PROGRESS
        monet_cur.execute("""
            UPDATE migration_log SET status = 'IN_PROGRESS'
            WHERE table_name = %s AND status = 'PENDING'
        """, (table_name,))
        monet_conn.commit()

        # Clear any partial data from previous failed runs
        monet_cur.execute(f"DELETE FROM {table_name}")
        monet_conn.commit()

        insert_sql = f"""
            INSERT INTO {table_name} (
                usage_id, customer_id, phone_number, region, plan_type,
                device_type, customer_status, signup_date, tower_id,
                session_start, session_end, data_used_mb, network_type,
                signal_strength, upload_speed_mbps, download_speed_mbps,
                source_system, ingestion_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
        """

        rows_loaded = 0
        offset = 0

        while True:
            neon_cur.execute(f"""
                SELECT * FROM public.{table_name}
                ORDER BY usage_id
                LIMIT %s OFFSET %s
            """, (CHUNK_SIZE, offset))

            chunk = neon_cur.fetchall()
            if not chunk:
                break

            batch = [
                (
                    row["usage_id"], row["customer_id"], row["phone_number"],
                    row["region"], row["plan_type"], row["device_type"],
                    row["customer_status"], row["signup_date"], row["tower_id"],
                    row["session_start"], row["session_end"], row["data_used_mb"],
                    row["network_type"], row["signal_strength"],
                    row["upload_speed_mbps"], row["download_speed_mbps"],
                    row["source_system"], row["ingestion_timestamp"],
                )
                for row in chunk
            ]

            monet_cur.executemany(insert_sql, batch)
            monet_conn.commit()

            rows_loaded += len(chunk)
            offset += CHUNK_SIZE
            logger.info(f"Loaded {rows_loaded}/{total_rows} rows...")

        # Mark as DONE
        monet_cur.execute("""
            UPDATE migration_log
            SET status = 'DONE', rows_loaded = %s, completed_at = %s
            WHERE table_name = %s AND status = 'IN_PROGRESS'
        """, (rows_loaded, datetime.utcnow(), table_name))
        monet_conn.commit()

        logger.info(f"Phase 2 complete: {rows_loaded} rows migrated to MonetDB")
        return {"status": "migration_complete", "rows_loaded": rows_loaded}

    except Exception as e:
        # Mark as FAILED
        try:
            monet_cur.execute("""
                UPDATE migration_log SET status = 'FAILED'
                WHERE table_name = %s AND status = 'IN_PROGRESS'
            """, (table_name,))
            monet_conn.commit()
        except Exception:
            pass
        logger.error(f"Phase 2 extract & load failed: {e}")
        raise self.retry(exc=e, countdown=60, max_retries=3)

    finally:
        neon_conn.close()
        monet_conn.close()


# ─── Entry point ─────────────────────────────────────────────────────────────

@shared_task(bind=True, name="telecom_etl.migration.migrate_neon_monet")
def migrate_neon_monet(self):

    logger.info("Starting Neon → MonetDB migration")
    job = chain(
        inspect_neon_schema.s(),
        prepare_monetdb.s(),
        extract_and_load.s(),
    )
    return job.apply_async()
