from pyspark.sql.functions import window, avg, to_timestamp
from spark.sql import SparkSession

spark = SparkSession.builder.appName("Sliding Window") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

parsed_ts = parsed_df.withColumn(
    "event_time", to_timestamp(col("fetched_at"), "yyyy-MM-dd'T'HH:mm:ss")
)

city_avg_temp = parsed_ts \
    .withWatermark("event_time", "3 minutes") \
    .groupBy(window(col("event_time"), "2 minutes", "1 minute"), col("city")) \
    .agg(avg("humidity").alias("avg_humidity"))
