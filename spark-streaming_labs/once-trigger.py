# tigger(once=True) -- runs one micro batch on all currently available data then stop (used for backfilling/testing)
from pyspark.sql.streaming import Trigger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.appName("Once Trigger") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

source_df = spark.readStream.format("rate") \
        .option("rowsPerSecond", 2) \
        .load()

transformed_df = source_df.withColumn("label", upper(col("value").cast("string")))

write_df = transformed_df.writeStream \
            .format("console") \
            .outputMode("update") \
            .trigger(once=True) \
            .start()

write_df.awaitTermination(20)
write_df.stop()

