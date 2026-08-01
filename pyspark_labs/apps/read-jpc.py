# Multiple read file format - JSON, Parquet, CSV
from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Multiple Read Format").getOrCreate()

# Read from csv format
csv_df_read = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/user_value_csv")
csv_df_read.select("user_id").count()

# Read from json format
json_df_read = spark.read.option("inferSchema", "true").json("/opt/spark-data/user_value_json")
json_df_read.select("user_id").count()

# Read from parquet format
parquet_df_read = spark.read.parquet("/opt/spark-data/user_value_parquet")
parquet_df_read.select("user_id").count()


'''
====Execution Time for each format====
CSV --> 1s
JSON --> 0.8s
Parquet --> 0.1s
'''
