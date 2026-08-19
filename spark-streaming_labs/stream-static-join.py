from pyspark.sql.functions import window, avg, to_timestamp
from spark.sql import SparkSession

spark = SparkSession.builder.appName("Streaming Static Join") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

city_metadata = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("/opt/spark-data/dim/city_metadata.csv")

parsed_ts = parsed_df.withColumn("event_time", to_timestamp(col("fetched_at"), "yyyy-MM-dd'T'HH:mm:ss"))

enriched = parsed_ts.join(city_metadata, on="city", how="left")
