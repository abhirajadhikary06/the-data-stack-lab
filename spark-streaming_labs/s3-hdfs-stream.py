from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
spark = SparkSession.builder.appName("S3-HDFS Landing Zone") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

PATH = "/opt/spark-data/landing/weather"

schema = StructType([
    StructField("city", StringType()),
    StructField("temp_c", DoubleType()),
    StructField("fetched_at", TimestampType(nullable=False)),
])

order_stream = spark.readStream.format("json").schema(schema).option("maxFilesPerTrigger", 5).load(PATH)

write_stream = order_stream.writeStream.format("parquet").outputMode("append").option("checkpointLocation", "/data/checkpoints/orders_console").start("/opt/spark-data/output/weather")

'''
Observation: We are connecting to a S3 source where streaming data
gets loaded everytime and Spark picks data from there and processes
it in either filecount or bytecount in per trigger and writes it to
a Sink location while keeping a log for checkpoint.
'''