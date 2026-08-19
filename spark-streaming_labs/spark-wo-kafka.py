from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.appName("Spark Streaming w/o Kafka") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

source_df = spark.readStream.format("rate") \
        .option("rowsPerSecond", 2) \
        .load()

transformed_df = source_df.withColumn("label", upper(col("value").cast("string")))

write_df = transformed_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .start()

write_df.awaitTermination(20)
write_df.stop()
