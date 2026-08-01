from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Overwrite Partitioning").getOrCreate()

# Setting Spark overwrite mode to dynamic
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
data = [(i, f"user_{i}", i*100, "2024-01-01") for i in range(1_000_000)]
columns = ["id", "user_id", "value", "date"]

df_write = spark.createDataFrame(data, columns)
write_path = "/opt/spark-data/user_value_parquet"

# Overwrite
df_write.write.mode("overwrite").partitionBy("date").parquet(write_path)