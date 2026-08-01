from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("First PySpark Job") \
    .getOrCreate()

data = [(i, i%10) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["key", "value"])
df.repartition(4)
df.show() # tigger actions
df.count() # trigger actions