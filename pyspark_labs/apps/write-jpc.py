# Multiple write file format - JSON, Parquet, CSV
from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Multiple Write Format").getOrCreate()

data = [(i, f"user_{i}", i*100, "2024-01-01") for i in range(1_000_000)]
columns = ["id", "user_id", "value", "date"]

df_write = spark.createDataFrame(data, columns)

# Write to csv format
df_write.write.mode("overwrite").option("header", "true").csv("/opt/spark-data/user_value_csv")

# Write to json format
df_write.write.mode("overwrite").json("/opt/spark-data/user_value_json")

# Write to parquet format
df_write.write.mode("overwrite").parquet("/opt/spark-data/user_value_parquet")