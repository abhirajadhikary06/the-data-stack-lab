# What is dlt by dltHub?
dlt (short for data load tool) by dltHub is an open-source Python library that automates the tedious parts of building data pipelines, such as schema inference, normalization, and incremental loading. In short it simplifies the data loading part in a data pipeline.

### Imports in your code
import dlt
from dlt.sources.sql_database import sql_database, sql_table

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
    dev_mode=True # to be kept with `replace` only
)
```

### Running the pipeline
- To run pipeline we have a command `pipeline.run(source)` where `pipeline` and `source` are the user set variables
- Check if pipeline loading is done or not:
`print(f"Pipeline run finished with status: {load_pipeline}")`

### Querying DuckDB to check if the data is loaded or not
```text
import duckdb
conn = duckdb.connect("website_checks_log_pipeline.duckdb")
print("Schemas found:", conn.execute("SHOW SCHEMAS").fetchall()) # Check Schema
dataset_name = "website_checks_log_20260516034723"
print(f"Tables in {dataset_name}:", conn.execute(f"SHOW TABLES FROM {dataset_name}").fetchall()) # Check for all available datasets
table_name = "website_checks_log"
print(f"Data from {dataset_name}.{table_name}:", conn.execute(f"SELECT * FROM {dataset_name}.{table_name} LIMIT 5").fetchall()) # Query into our required table
```
### How data is updated to the destination
- `append` - Appends the data to the destination table. When ran repetedly it can duplicate data.
    `source.apply_hints(incremental=dlt.sources.incremental("checked_at"), write_disposition="append")`

- `replace` - Replaces the data in the destination table with the new data.
- `merge` - Merges the new data with the existing data in the destination table based on a primary key.
- These all methods are written a function - `write_disposition="merge"`
    ### Why is `replace` used with `pipeline.run()` and `merge` with `source.tbl_name.apply_hints()` ?
        `replace` - Clearing the whole database/dataset for a full refresh. (Global level Policy)
            - `load_pipeline = pipeline.run(source, write_disposition="replace")`
            - `replace` acts as `full load`
            - Some strategies used in `full load` these strategies are to be placed in `config.toml` under `[destination]`
            ```text
            [destination]
            # Set the optimized replace strategy
            replace_strategy = "staging-optimized"
            ```
                - `truncate-and-insert` - fastest strategy, load data to destination after truncating it's current data.
                - `insert-from-staging` - slowest strategy, first load the data into staging table and then push it in just one transaction to destination after truncating it. 
                - `staging-optimized` - this strategy has all the upsides of the insert-from-staging but implements certain optimizations for faster loading on some destinations.

        `merge` - Updating specific tables that have primary keys. (Granular levl Policy)
        `source.website_checks_log.apply_hints(write_disposition="merge", primary_key=["created_at"])`

## Incremental Loading Strategies in dlt
```text
Is data immutable?
    |
    ├── YES → append
    |
    └── NO
          |
          ├── Need latest state only?
          |        └── merge/upsert
          |
          └── Need historical tracking?
                   └── scd2
```

### Full Refresh `replace`
- Deletes old data and reloads everything
Application: `pipeline.run(source, write_disposition="replace")`

### Append Incremental `append`
- Never updates old rows, only new rows are appended incremtally after the previous timestamp Application: `source.apply_hints(incremental=dlt.sources.incremental("checked_at"), write_disposition="append")`

### Merge Incremental (upsert)
- If rows exist UPDATE it else INSERT it
Application: `source.apply_hints(incremental=dlt.sources.incremental("updated_at"), write_disposition="merge", primary_key=["id"])`

### SCD2 Merge Strategy
- Keep OLD version INSERT new version (dlt atomatically creates scd type 2 tracking columns like `_dlt_valid_from`, `_dlt_valid_to`, `_dlt_id`)
Application: `write_disposition={"disposition": "merge", "strategy": "scd2"}`

### Upsert Strategy
- Special optmised merge based on primary key only, best for data warehouse data syncing
Application: `write_disposition={"disposition": "merge", "strategy": "upsert"}`

### Inset Only
- Insert only if the record does not exists
Application: `write_disposition={"disposition": "merge", "strategy": "insert-only"}`

### Incremental Lag
- Handles late-arriving data.
Application: `incremental=dlt.sources.incremental("created_at", lag=3600)`



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