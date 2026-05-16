import duckdb

conn = duckdb.connect("website_checks_log_pipeline.duckdb")

# 1. See all schemas (datasets) to find yours
print("Schemas found:", conn.execute("SHOW SCHEMAS").fetchall())

# 2. Show tables specifically from your dlt dataset
dataset_name = "website_checks_log_20260516034723"
print(f"Tables in {dataset_name}:", conn.execute(f"SHOW TABLES FROM {dataset_name}").fetchall())

table_name = "website_checks_log"
print(f"Data from {dataset_name}.{table_name}:", conn.execute(f"SELECT * FROM {dataset_name}.{table_name} LIMIT 5").fetchall())
