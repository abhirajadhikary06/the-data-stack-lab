from pyspark.sql.functions import window, avg, to_timestamp
from spark.sql import SparkSession

spark = SparkSession.builder.appName("Checkpoint & Fault Tolerance") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

query = city_avg_temp.writeStream \
    .format("delta") \
    .option("path", "/opt/spark-data/output/city_avg_temp") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/city_avg_temp") \
    .outputMode("append") \
    .start()

'''
What does checkpoint directory has:
- offsets/ --> WAL of exactly which kafka offset range each micro batch processed enables deterministic replay
- commits/ --> Marks which batches fully completed (both read and write)
- state/ --> serialized aggregation/join state
- sources/ , sink/ --> Metadata about how the data was configured
'''