from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min
spark = SparkSession.builder.appName("Intermediate Transformation").getOrCreate()

data = [(i, i % 10) for i in range(500_000)]
df = spark.createDataFrame(data, ["id", "group"])

# GroupBy
df.groupBy("group").count().show()

# Distinct
df.select("id").distinct().show()

# Aggregate
df_agg = df.groupBy("group").agg(
    avg(col("id")).alias("avg_id"),
    max(col("id")).alias("max_id"),
    min(col("id")).alias("min_id")
)
df_agg.show()