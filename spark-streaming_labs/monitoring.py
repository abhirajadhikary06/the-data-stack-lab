from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Monitoring a Running Query") \
        .master("local[*]") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

query = parsed_ts.writeStream.format("console").start()

print(query.id) 
print(query.runId)
print(query.status)
print(query.lastProgress)

for q in spark.streams.active:
    print(q.name. q.status)

spark.streams.awaitAnyTermination()