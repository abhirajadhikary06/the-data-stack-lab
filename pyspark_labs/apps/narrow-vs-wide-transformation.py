from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Narrow vs Wide Transformation").getOrCreate()

# Narrow transformation
data = [(i, i % 5) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["id", "group"])

df_narrow = df.filter(df.group > 2).select("id")
df_narrow.show()

# Wide transformation
df_wide = df.groupBy("group").count()
df_wide.show()