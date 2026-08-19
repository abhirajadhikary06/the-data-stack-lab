# tigger(continuous="1 second") -- continuous processing mode
from pyspark.sql.streaming import Trigger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.appName("continuous Trigger") \
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
            .trigger(continuous="1 second") \
            .start()

write_df.awaitTermination(20)
write_df.stop()

