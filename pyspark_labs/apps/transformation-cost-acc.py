from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Transformation Cost Accumulation").getOrCreate()

data = [(i, i % 10) for i in range(500_000)]
df = spark.createDataFrame(data, ["id", "group"])

df_transformed = (
    df.select("id", "group")
      .filter(col("group") > 5)
      .withColumn("group_double", col("group") * 2)
      .withColumn("group_plus_one", col("group") + 1)
)

df_transformed.explain()
df_transformed.show()