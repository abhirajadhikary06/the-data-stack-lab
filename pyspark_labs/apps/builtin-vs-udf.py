from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.appName("Builtin vs UDF").getOrCreate()

data = [(i,) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["value"])

# Builtin
df_builtin = df.withColumn("double_value", col("value")*2)
df_builtin.show()
df_builtin.explain(mode="formatted")

# UDF
def double_value(x):
    return x*2

double_udf = udf(double_value, IntegerType())
df_udf = df.withColumn("double_value", double_udf(col("value")))
df_udf.show()
df_udf.explain(mode="formatted")