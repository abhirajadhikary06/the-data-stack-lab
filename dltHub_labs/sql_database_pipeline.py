import dlt
from dlt.sources.sql_database import sql_database, sql_table

def load_website_checks_log():
    # Loading the database source using the credentials from secrets.toml
    source = sql_table(table="website_checks_log") # We could have used sql_database(), but we need just one table.

    pipeline = dlt.pipeline(
        pipeline_name="website_checks_log_pipeline",
        destination="duckdb",
        dataset_name="website_checks_log",
        dev_mode=True
    )

    load_pipeline = pipeline.run(source)
    print(f"Pipeline run finished with status: {load_pipeline}")

if __name__ == "__main__":
    load_website_checks_log()
