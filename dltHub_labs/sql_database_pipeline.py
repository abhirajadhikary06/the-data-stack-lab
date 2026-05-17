from os import write
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dotenv import load_dotenv
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

    # Create a DLT pipeline to load the data into DuckDB
    pipeline = dlt.pipeline(
        pipeline_name="website_checks_log_pipeline",
        destination="duckdb",
        dataset_name="website_checks_log",
    #   dev_mode=True # to be used with replace
    )

#   load_pipeline = pipeline.run(source, write_disposition="replace") # This would replace the existing data in the destination with the new data from the source.
    load_pipeline = pipeline.run(source)
    print(f"Pipeline run finished with status: {load_pipeline}")

if __name__ == "__main__":
    load_website_checks_log()
