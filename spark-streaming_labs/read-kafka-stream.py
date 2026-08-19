from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, IntegerType)

spark = SparkSession.builder \
    .appName("Kafka Consumer") \
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

query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/parse_check") \
    .start()

try:
    query.awaitTermination()
finally:
    query.stop()
    spark.stop()