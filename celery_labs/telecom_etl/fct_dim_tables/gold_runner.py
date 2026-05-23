import os
import logging
import duckdb
from pathlib import Path
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/telecom.db")
SQL_DIR = Path(__file__).parent

# Execution order matters — dims before fact, fact before KPIs
SQL_FILES = [
    "dim_customer.sql",
    "dim_time.sql",
    "dim_network.sql",
    "fact_telecom_usage.sql",
    "gold_usage_kpi.sql",
]


def run_sql_file(conn: duckdb.DuckDBPyConnection, filename: str):
    """Read a SQL file and execute it against DuckDB."""
    sql_path = SQL_DIR / filename
    sql = sql_path.read_text()
    logger.info(f"Running: {filename}")
    conn.execute(sql)
    logger.info(f"Done:    {filename}")


# ─── Prefect tasks — one per SQL file ────────────────────────────────────────

@task(name="build_dim_customer", cache_policy=NO_CACHE)
def build_dim_customer(conn):
    run_sql_file(conn, "dim_customer.sql")
    count = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    logger.info(f"dim_customer: {count} rows")
    return count


@task(name="build_dim_time", cache_policy=NO_CACHE)
def build_dim_time(conn):
    run_sql_file(conn, "dim_time.sql")
    count = conn.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    logger.info(f"dim_time: {count} rows")
    return count


@task(name="build_dim_network", cache_policy=NO_CACHE)
def build_dim_network(conn):
    run_sql_file(conn, "dim_network.sql")
    count = conn.execute("SELECT COUNT(*) FROM dim_network").fetchone()[0]
    logger.info(f"dim_network: {count} rows")
    return count


@task(name="build_fact_telecom_usage", cache_policy=NO_CACHE)
def build_fact_telecom_usage(conn):
    run_sql_file(conn, "fact_telecom_usage.sql")
    count = conn.execute("SELECT COUNT(*) FROM fact_telecom_usage").fetchone()[0]
    logger.info(f"fact_telecom_usage: {count} rows")
    return count


@task(name="build_gold_usage_kpis")
def build_gold_usage_kpis(conn):
    run_sql_file(conn, "gold_usage_kpi.sql")
    count = conn.execute("SELECT COUNT(*) FROM gold_usage_kpis").fetchone()[0]
    logger.info(f"gold_usage_kpis: {count} rows")
    return count


# ─── Prefect flow — equivalent of dbt run ────────────────────────────────────

@flow(name="gold_layer_flow", log_prints=True)
def gold_layer_flow():
    """
    Runs all Gold Layer SQL files against DuckDB in dependency order.
    Equivalent of: dbt run --select dim_* fact_* gold_*

    Single DuckDB connection shared across all tasks to avoid file lock conflicts.
    """
    # Read path at runtime so CLI override via env var works
    duckdb_path = os.getenv("DUCKDB_PATH", "/app/data/telecom.db")
    logger.info(f"Connecting to DuckDB at {duckdb_path}")
    conn = duckdb.connect(duckdb_path)

    try:
        # Dims first — no dependencies
        build_dim_customer(conn)
        build_dim_time(conn)
        build_dim_network(conn)

        # Fact — depends on dim_network
        build_fact_telecom_usage(conn)

        # KPI mart — depends on fact + dim_customer
        build_gold_usage_kpis(conn)

        logger.info("Gold Layer build complete — all tables ready for Metabase")

    finally:
        conn.close()


# ─── Run directly (without Prefect server) ───────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        os.environ["DUCKDB_PATH"] = sys.argv[1]
    gold_layer_flow()
