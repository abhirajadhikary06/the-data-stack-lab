from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, IntegerType)

spark = SparkSession.builder \
    .appName("Kafka Writer") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

weather_schema = StructType([
    StructField("city",        StringType()),
    StructField("region",      StringType()),
    StructField("country",     StringType()),
    StructField("lat",         DoubleType()),
    StructField("lon",         DoubleType()),
    StructField("localtime",   StringType()),
    StructField("temp_c",      DoubleType()),
    StructField("feelslike_c", DoubleType()),
    StructField("humidity",    IntegerType()),
    StructField("wind_kph",    DoubleType()),
    StructField("pressure_mb", DoubleType()),
    StructField("condition",   StringType()),
    StructField("cloud",       IntegerType()),
    StructField("uv",          DoubleType()),
    StructField("fetched_at",  StringType()),
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = kafka_df \
    .selectExpr("CAST(key AS STRING) AS city_key", "CAST(value AS STRING) AS json_str") \
    .select("city_key", from_json(col("json_str"), weather_schema).alias("d")) \
    .select("city_key", "d.*")

# === Stream 1: print to console for debugging ===
console_query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/parse_check") \
    .start()

# === Stream 2: filter hot alerts and write back to Kafka ===
hot_alerts = parsed_df \
    .filter(col("temp_c") > 20) \
    .select(
        col("city").alias("key"),
        to_json(struct("city", "temp_c", "condition", "fetched_at")).alias("value")
    )

kafka_query = hot_alerts.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "hot-weather-alerts") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/hot_alerts") \
    .start()

try:
    spark.streams.awaitAnyTermination()
finally:
    console_query.stop()
    kafka_query.stop()
    spark.stop()
