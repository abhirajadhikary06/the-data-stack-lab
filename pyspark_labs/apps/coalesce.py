from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Coalesce").getOrCreate()

data = (
    [(0, i) for i in range(90_000)] +
    [(1, i) for i in range(25_000)] +
    [(2, i) for i in range(25_000)] +
    [(3, i) for i in range(25_000)] +
    [(4, i) for i in range(25_000)]
)
df = spark.createDataFrame(data, ["key", "value"])

# Check default Partition
df.rdd.getNumPartitions()

df_repart = df.repartition(10)

# Coalesce
df_coalesced = df_repart.coalesce(3)
df_coalesced.rdd.getNumPartitions()
df_coalesced.count()