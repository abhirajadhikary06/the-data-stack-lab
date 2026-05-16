# What is dlt by dltHub?
dlt (short for data load tool) by dltHub is an open-source Python library that automates the tedious parts of building data pipelines, such as schema inference, normalization, and incremental loading. In short it simplifies the data loading part in a data pipeline.

### How to check available sources and destination in dlt
- dlt initialization help command - `dlt init -h`
- dlt available sources - `dlt init --list-sources` 
- dlt available destinations - `dlt init --list-destinations`

### Working with "SQL" as resource and "DuckDB" as destination
- dlt init <source> <destination> - `dlt init sql_database duckdb`

### Establishing a source
- To establish a source there are mainly two function `sql.database()` or `sql_table()`
- `sql_database()` - this is to source all tables in a database - `source = sql_database().with_resources("table_1", "table_2")`
- `sql_table()`- this is to source only a particular table - `source_2 = sql_table(table="table_name")`
- Check if your database and tables are loaded or not:
    - `print(f"Connection successful! Available tables: {list(source.resources.keys())}")`
    - `print(f"Available tables in source2: {source2.table_name}")`

### Building the Pipeline
- To build a pipeline we have a function i.e `dlt.pipeline()`
- Pipeline has some parameters like `pipeline_name`, `destination`, `dataset_name`

```text
pipeline = dlt.pipeline(
    pipeline_name="website_checks_log_pipeline",
    destination="duckdb",
    dataset_name="website_checks_log"
    dev_mode=True
)
```

### Running the pipeline
- To run pipeline we have a command `pipeline.run(source)` where `pipeline` and `source` are the user set variables
- Check if pipeline loading is done or not:
`print(f"Pipeline run finished with status: {load_pipeline}")`

### Querying DuckDB to check if the data is loaded or not
```
import duckdb
conn = duckdb.connect("website_checks_log_pipeline.duckdb")
print("Schemas found:", conn.execute("SHOW SCHEMAS").fetchall()) # Check Schema
dataset_name = "website_checks_log_20260516034723"
print(f"Tables in {dataset_name}:", conn.execute(f"SHOW TABLES FROM {dataset_name}").fetchall()) # Check for all available datasets
table_name = "website_checks_log"
print(f"Data from {dataset_name}.{table_name}:", conn.execute(f"SELECT * FROM {dataset_name}.{table_name} LIMIT 5").fetchall()) # Query into our required table
```
