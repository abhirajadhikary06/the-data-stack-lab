import duckdb

conn = duckdb.connect("website_checks_log_pipeline.duckdb")

table_name = "website_checks_log"

# 1) Discover schemas that actually contain the target table.
schema_rows = conn.execute(
	"""
	SELECT DISTINCT table_schema
	FROM information_schema.tables
	WHERE table_name = ?
	ORDER BY table_schema
	""",
	[table_name],
).fetchall()
candidate_schemas = [row[0] for row in schema_rows]
print("Candidate schemas for table:", candidate_schemas)

if not candidate_schemas:
	raise RuntimeError(f"No schema contains table '{table_name}'.")

# 2) Prefer stable dataset schema, else latest timestamped schema.
stable_schema = "website_checks_log"
timestamped_schemas = [s for s in candidate_schemas if s.startswith("website_checks_log_")]
if stable_schema in candidate_schemas:
	dataset_name = stable_schema
elif timestamped_schemas:
	dataset_name = sorted(timestamped_schemas)[-1]
else:
	dataset_name = candidate_schemas[-1]

print("Selected schema:", dataset_name)
print(
	f"Tables in {dataset_name}:",
	conn.execute(f'SHOW TABLES FROM "{dataset_name}"').fetchall(),
)

query = f'SELECT * FROM "{dataset_name}"."{table_name}" ORDER BY id DESC LIMIT 5'
print(f"Data from {dataset_name}.{table_name}:", conn.execute(query).fetchall())
