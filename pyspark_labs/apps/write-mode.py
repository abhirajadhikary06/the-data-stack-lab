from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Write Modes").getOrCreate()

data = [(i, f"user_{i}", i*100, "2024-01-01") for i in range(1_000_000)]
columns = ["id", "user_id", "value", "date"]

df_write = spark.createDataFrame(data, columns)
write_path = "/opt/spark-data/user_value_parquet"
# Overwrite
df_write.write.mode("overwrite").parquet(write_path)

# Append
df_write.write.mode("append").parquet(write_path)

# Error
df_write.write.mode("error").parquet(write_path)

# Ignore
df_write.write.mode("ignore").parquet(write_path)