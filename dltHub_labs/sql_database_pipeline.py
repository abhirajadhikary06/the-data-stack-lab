from os import write
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
load_dotenv()

def load_website_checks_log():
    # If you want to load the entire database
#   source = sql_database().with_resources("website_checks_log", "playing_with_neon")

    # Loading the database source using the credentials from secrets.toml
    source = sql_table(table="website_checks_log")
    # Merge the data into destination with the existing data, if any.(applicable for databases)
#   source.website_checks_log.apply_hints(write_disposition="merge", primary_key=["id"])

    # If there are no updates in rows for our table, and there is just incremental data, we use increment with append
    source.apply_hints(incremental=dlt.sources.incremental("checked_at"), write_disposition="append")

    # Bronze Level Transformation in dlt itself
    @dlt.transformer(data_from=source, name="transform_website_checks_log")
    def transform_website_checks_log(rows):

        for row in rows:

            # DELETE / FILTER INVALID ROWS
            # Equivalent to DELETE WHERE ... IS NULL

            if row.get("status_code") is None:
                continue

            if row.get("url") is None:
                continue

            if row.get("fix") is None:
                continue

            if row.get("response_time_ms") is None:
                continue

            if row.get("status_message") is None:
                continue

            # URL NORMALIZATION

            url = (
                row["url"]
                .strip()
                .lower()
                .replace("https://", "")
                .replace("http://", "")
                .rstrip("/")
            )

            row["url"] = url

            # WEBSITE NAME + DOMAIN EXTRACTION

            parsed = urlparse(f"https://{url}")

            hostname = parsed.hostname or ""

            row["website_name"] = (
                hostname.split(".")[0]
                if hostname else "unknown"
            )

            row["domain"] = (
                hostname.split(".")[-1]
                if hostname else "unknown"
            )

            # RESPONSE TIME CONVERSION

            response_time = float(
                row["response_time_ms"]
            )

            row["response_time_s"] = (
                response_time / 1000.0
            )

            # PERFORMANCE BUCKETS

            if response_time <= 200:
                row["performance_bucket"] = "fast"

            elif 201 <= response_time <= 800:
                row["performance_bucket"] = "moderate"

            elif 801 <= response_time <= 1000:
                row["performance_bucket"] = "slow"

            else:
                row["performance_bucket"] = "critical"

            # STATUS CODE GROUPING

            status_code = int(
                row["status_code"]
            )

            if 200 <= status_code <= 299:
                row["status_group"] = "success"

            elif 300 <= status_code <= 399:
                row["status_group"] = "redirection"

            elif 400 <= status_code <= 499:
                row["status_group"] = "client_error"

            elif 500 <= status_code <= 599:
                row["status_group"] = "server_error"

            else:
                row["status_group"] = "invalid_error"

            # BOOLEAN ERROR FLAGS

            row["is_error"] = (
                status_code >= 400
            )

            row["is_success"] = (
                200 <= status_code <= 299
            )

            row["is_redirect"] = (
                300 <= status_code <= 399
            )

            row["is_slow_response"] = (
                response_time > 1000
            )

            # INVALID STATUS MESSAGE HANDLING

            if status_code == 999:
                row["status_message"] = "Invalid"

            # LOWERCASE FIX COLUMN

            row["fix"] = (
                row["fix"]
                .strip()
                .lower()
            )

            # TIMESTAMP FEATURES

            checked_at = row["checked_at"]

            row["check_date"] = (
                checked_at.date().isoformat()
            )

            row["check_month"] = (
                checked_at.month
            )

            row["check_year"] = (
                checked_at.year
            )

            row["check_hour"] = (
                checked_at.hour
            )

            row["check_day_name"] = (
                checked_at.strftime("%A")
            )

            # INGESTION + LOAD TIMESTAMPS

            current_time = datetime.utcnow()

            row["ingestion_time"] = current_time

            row["load_time"] = current_time
            
            # YIELD FINAL TRANSFORMED ROW

            yield row

    # Run the pipeline to load the data into the destination
    pipeline = dlt.pipeline(
        pipeline_name="website_checks_log_pipeline",
        destination="duckdb",
        dataset_name="website_checks_log",
    )
    load_info = pipeline.run(transform_website_checks_log())
    print("Load info:", load_info)


if __name__ == "__main__":
    load_website_checks_log()
    