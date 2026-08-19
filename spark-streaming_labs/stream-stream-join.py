from pyspark.sql.functions import window, avg, to_timestamp
from spark.sql import SparkSession

spark = SparkSession.builder.appName("Streaming Stream Join") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

weather_alerts = parsed_df
hot_alerts = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "hot-weather-alerts") \
    .load()

weather_alerts_wm = weather_alerts.withWatermark("fetched_at", "10 minutes")
hot_alerts_wm = hot_alerts.withWatermark("fetched_at", "20 minutes")
join_condt = "w.weather_alerts.city = h.hot_alerts AND h.fetched_at BETWEEN w.fetched_at AND w.fetched_at + interval 15 minutes"


joined = weather_alerts_wm.join(hot_alerts_wm, expr(join_condt), "inner")