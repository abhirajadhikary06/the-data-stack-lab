import duckdb

# CONNECT TO DUCKDB DATABASE

conn = duckdb.connect("website_checks_log_pipeline.duckdb")

table_name = "transform_website_checks_log"

# FIND ALL SCHEMAS CONTAINING THE TABLE

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
    raise RuntimeError(
        f"No schema contains table '{table_name}'."
    )

# SELECT THE MOST APPROPRIATE SCHEMA

stable_schema = "website_checks_log"

timestamped_schemas = [
    s for s in candidate_schemas
    if s.startswith("website_checks_log_")
]

if stable_schema in candidate_schemas:
    dataset_name = stable_schema

elif timestamped_schemas:
    dataset_name = sorted(timestamped_schemas)[-1]

else:
    dataset_name = candidate_schemas[-1]

print("\nSelected schema:", dataset_name)

# SHOW TABLES

tables = conn.execute(
    f'SHOW TABLES FROM "{dataset_name}"'
).fetchall()

print(f"\nTables in schema '{dataset_name}':")

for table in tables:
    print(table[0])

# SHOW TABLE STRUCTURE

print("\nTable Schema:\n")

schema_query = f"""
DESCRIBE "{dataset_name}"."{table_name}"
"""

schema_info = conn.execute(
    schema_query
).fetchall()

for column in schema_info:
    print(column)

# SHOW SAMPLE DATA

print("\nLatest 5 Rows:\n")

sample_query = f'''
SELECT *
FROM "{dataset_name}"."{table_name}"
ORDER BY id DESC
LIMIT 5
'''

sample_data = conn.execute(
    sample_query
).fetchall()

for row in sample_data:
    print(row)

# TOTAL RECORD COUNT

count_query = f'''
SELECT COUNT(*) AS total_records
FROM "{dataset_name}"."{table_name}"
'''

total_records = conn.execute(
    count_query
).fetchone()[0]

print(f"\nTotal Records: {total_records}")

# STATUS CODE DISTRIBUTION

print("\nStatus Code Distribution:\n")

status_distribution_query = f'''
SELECT
    status_code,
    COUNT(*) AS status_code_count
FROM "{dataset_name}"."{table_name}"
GROUP BY status_code
ORDER BY status_code_count DESC
'''

status_distribution = conn.execute(
    status_distribution_query
).fetchall()

for row in status_distribution:
    print(row)

# PERFORMANCE BUCKET DISTRIBUTION

print("\nPerformance Bucket Distribution:\n")

performance_bucket_query = f'''
SELECT
    performance_bucket,
    COUNT(*) AS bucket_count
FROM "{dataset_name}"."{table_name}"
GROUP BY performance_bucket
ORDER BY bucket_count DESC
'''

performance_distribution = conn.execute(
    performance_bucket_query
).fetchall()

for row in performance_distribution:
    print(row)

# STATUS GROUP DISTRIBUTION

print("\nStatus Group Distribution:\n")

status_group_query = f'''
SELECT
    status_group,
    COUNT(*) AS group_count
FROM "{dataset_name}"."{table_name}"
GROUP BY status_group
ORDER BY group_count DESC
'''

status_group_distribution = conn.execute(
    status_group_query
).fetchall()

for row in status_group_distribution:
    print(row)

# RESPONSE TIME STATISTICS

print("\nResponse Time Statistics:\n")

response_stats_query = f'''
SELECT
    MIN(response_time_ms) AS min_response_time,
    MAX(response_time_ms) AS max_response_time,
    AVG(response_time_ms) AS avg_response_time
FROM "{dataset_name}"."{table_name}"
'''

response_stats = conn.execute(
    response_stats_query
).fetchone()

print(f"Min Response Time : {response_stats[0]} ms")
print(f"Max Response Time : {response_stats[1]} ms")
print(f"Avg Response Time : {response_stats[2]:.2f} ms")

# ERROR ANALYSIS

print("\nError Analysis:\n")

error_analysis_query = f'''
SELECT
    is_error,
    COUNT(*) AS total
FROM "{dataset_name}"."{table_name}"
GROUP BY is_error
'''

error_analysis = conn.execute(
    error_analysis_query
).fetchall()

for row in error_analysis:
    print(row)

# TOP SLOWEST WEBSITES

print("\nTop 10 Slowest Websites:\n")

slow_websites_query = f'''
SELECT
    url,
    AVG(response_time_ms) AS avg_response_time
FROM "{dataset_name}"."{table_name}"
GROUP BY url
ORDER BY avg_response_time DESC
LIMIT 10
'''

slow_websites = conn.execute(
    slow_websites_query
).fetchall()

for row in slow_websites:
    print(row)

# DAILY CHECK VOLUME

print("\nDaily Check Volume:\n")

daily_volume_query = f'''
SELECT
    check_date,
    COUNT(*) AS total_checks
FROM "{dataset_name}"."{table_name}"
GROUP BY check_date
ORDER BY check_date DESC
LIMIT 10
'''

daily_volume = conn.execute(
    daily_volume_query
).fetchall()

for row in daily_volume:
    print(row)

# CLOSE CONNECTION

conn.close()