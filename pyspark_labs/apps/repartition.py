from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Default Repartition").getOrCreate()

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

# Repartition
df_repartition = df.repartition(10)
df_repartition.rdd.getNumPartitions()
df_repartition.count()
