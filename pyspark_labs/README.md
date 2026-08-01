# PySpark Labs

Hands-on practice with PySpark concepts. Each file in `apps/` covers a specific topic.

---

## 1. Spark Basics

**`first_job.py`**
Created a SparkSession, built a DataFrame from in-memory data, and triggered actions (`show`, `count`) to understand the difference between transformations and actions.

**`lazy_execution.py`**
Demonstrated lazy evaluation — chained `filter` and `select` only build a logical plan; execution happens when an action like `show` is called.

**`spark_dag.py`**
Used `explain(mode="formatted")` to inspect the physical plan and understand how Spark builds a DAG with stages like Filter, HashAggregate, and Exchange.

---

## 2. Read & Write API

**`spark_read.py`**
Read a CSV file using the fluent Reader API with options for header, delimiter, and schema inference.

**`simple_spark_write.py`**
Wrote a DataFrame to CSV using the Writer API with overwrite mode.

**`write-jpc.py`** / **`read-jpc.py`**
Wrote and read the same dataset in CSV, JSON, and Parquet formats. Observed that Parquet is significantly faster to read (~10x vs CSV/JSON).

**`write-mode.py`**
Practiced all four write modes: `overwrite`, `append`, `error`, and `ignore`.

**`overwrite-partition.py`**
Used `partitionBy` with dynamic partition overwrite mode (`spark.sql.sources.partitionOverwriteMode = dynamic`) to overwrite only specific partitions.

---

## 3. Schema

**`schema_inference.py`**
Used `inferSchema=true` to let Spark automatically detect column types from a CSV file.

**`explicit-schema.py`**
Defined a schema explicitly using `StructType` and `StructField`, then applied it at read time.

**`known_wrong_write.py`** / **`infer-explicit-diff.py`**
Wrote data with intentionally bad date values (`"not-found"`). Compared the behavior of inferred vs explicit schema — inferred schema treats the column as `StringType` and keeps the bad values; explicit `DateType` schema nullifies them.

**`structtype.py`**
Defined and applied a nested schema using `StructType` inside `StructType`. Accessed nested fields using dot notation: `col("location.country")`.

**`json-write.py`**
Wrote nested JSON data using `Row` objects or an explicit schema to ensure nested fields get proper names (`city`, `country`) instead of auto-assigned `_1`, `_2`.

**`ddl-schema.py`**
Defined schemas using DDL string syntax (e.g., `"id INT, name STRING, location STRUCT<city:STRING, country:STRING>"`) as a concise alternative to `StructType`.

---

## 4. Transformations

**`basic-transformation.py`**
Used `select`, `filter`, and `withColumn` — the core narrow transformations that don't trigger a shuffle.

**`narrow-vs-wide-transformation.py`**
Contrasted narrow transformations (`filter`, `select`) which stay within a partition, vs wide transformations (`groupBy`) which require a shuffle across partitions.

**`transformation-cost-acc.py`**
Chained multiple transformations and used `explain()` to see how Spark accumulates them into a single optimized physical plan.

**`intermediate-transformations.py`**
Practiced `groupBy`, `distinct`, and `agg` with aggregate functions `avg`, `max`, `min`.

---

## 5. Joins

**`shuffle_join.py`**
Joined a large orders table with a small customers table using the default shuffle join (SortMergeJoin), which moves data across the network.

**`broadcast_join.py`**
Used `broadcast()` hint to send the small table to all executors, avoiding the shuffle entirely.

**`shuffle-vs-broadcast-join.py`**
Compared physical plans of both join strategies side by side. Shuffle join shows `SortMergeJoin` with `Exchange` steps; broadcast join shows `BroadcastHashJoin` with `BroadcastExchange`.

---

## 6. Partitioning

**`repartition.py`**
Used `repartition(n)` to increase partition count. Triggers a full shuffle and distributes data evenly.

**`repartition-by-key.py`**
Used `repartition(n, "key")` to ensure all rows with the same key land in the same partition.

**`coalesce.py`**
Used `coalesce(n)` to reduce partition count without a full shuffle — merges existing partitions locally.

---

## 7. Window Functions

**`window-row_number.py`**
Assigned a unique sequential number to each row within a partition using `row_number()`.

**`window-rank.py`**
Ranked rows within a partition using `rank()` — tied values get the same rank and the next rank skips.

**`window-denserank.py`**
Used `dense_rank()` — same as rank but no gaps in rank sequence after ties.

**`window-lag-lead.py`**
Used `lag()` and `lead()` to access the previous and next row's value within a partition, useful for time-series comparisons.

---

## 8. Data Skew

**`skew_data.py`**
Created a skewed dataset with a `HOT_KEY` that has 100x more rows than other keys. Observed that one partition does most of the work during `groupBy`.

**`salting-skew-data.py`**
Fixed skew by adding a random salt suffix to the hot key (`HOT_KEY_0` through `HOT_KEY_9`), spreading the load across 10 buckets. Also practiced applying salting only to the hot key using `when/otherwise`.

---

## 9. UDFs

**`udf.py`**
Registered a Python function as a UDF using `@udf` decorator and applied it with `withColumn`. Observed `BatchEvalPython` in the physical plan, indicating Python serialization overhead.

**`builtin-vs-udf.py`**
Compared the same operation (`value * 2`) using a built-in Spark function vs a UDF. Built-in functions run in the JVM and show as a simple `Project` in the plan; UDFs require Python serialization and show `BatchEvalPython`.

---

## 10. Spark SQL & Tables

**`sql-vs-dataframe.py`**
Ran the same filter query using both the DataFrame API and Spark SQL via `createOrReplaceTempView`. Used `explain(True)` to confirm both produce identical physical plans.

**`managed_table.py`**
Saved a DataFrame as a managed table using `saveAsTable`. Spark manages both the metadata and the data. Queried it using both SQL and the DataFrame API.

**`external_table.py`**
Created an external table with a custom `path` option. Spark manages only the metadata; the data lives at the specified path and persists if the table is dropped. Ran SQL aggregations (`COUNT`, `SUM`, `HAVING`) on the table.
