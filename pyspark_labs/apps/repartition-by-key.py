from pyspark.sql import SparkSession
spark = SparkSession.builder.app("Repartition By Key").getOrCreate()

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

# Same keys goes to same partition
df_repartition = df.repartition(10, "key")
df_repartition.rdd.getNumPartitions()
df_repartition.count()