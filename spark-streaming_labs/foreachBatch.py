# foreachBatch --> is used to write to sinks or perform operations that Structured Streaming does not natively support, especially when you need custom batch-level logic.
from pyspark.sql.streaming import Trigger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.appName("foreachBatch") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def process_batch(batch_df, batch_id):
    print(f"Batch {batch_id}: {batch_df.count()} rows")
    batch_df.persist()

    # sink -1 --> archive the raw batch
    batch_df.write.mode("append").parquet(f"/opt/spark-data/archive/weather_batch_{batch_id}")

    # sink -2 --> upsert into a running "latest reading per city" table
    batch_df.createOrReplaceTempView("updates")
    batch_df.SparkSession.sql("""
        MERGE INTO weather_alerts AS w
        USING hot_alerts AS h
        ON w.city = h.city
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSET *
    """)

    batch_df.unpersist()

    write_df = parsed_ts.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/opt/spark-data/checkpoints/foreach_batch") \
        .outputMode("update") \
        .start()