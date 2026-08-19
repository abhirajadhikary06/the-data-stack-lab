from pyspark.sql.functions import window, avg, to_timestamp
from spark.sql import SparkSession

spark = SparkSession.builder.appName("Deduplicate - Watermarking") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

parsed_ts = parsed_df.withColumn(
    "event_time", to_timestamp(col("fetched_at"), "yyyy-MM-dd'T'HH:mm:ss")
)

dedup = parsed_ts \
    .withWatermark("event_time", "1 hour") \
    .dropDuplicates(["city", "fetched_at"])
