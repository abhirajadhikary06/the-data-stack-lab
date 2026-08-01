from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.appName("UDF - User Defined Function").getOrCreate()

data = [(i,) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["value"])

def double_value(x):
    return x*2

double_udf = udf(double_value, IntegerType())
df_udf = df.withColumn("double_value", double_udf(col("value")))
df_udf.show()
df_udf.explain(mode="formatted")

'''
====Expected Output====
+-----+------------+
|value|double_value|
+-----+------------+
|    0|           0|
|    1|           2|
|    2|           4|
|    3|           6|
|    4|           8|
|    5|          10|
|    6|          12|
|    7|          14|
|    8|          16|
|    9|          18|
|   10|          20|
|   11|          22|
|   12|          24|
|   13|          26|
|   14|          28|
|   15|          30|
|   16|          32|
|   17|          34|
|   18|          36|
|   19|          38|
+-----+------------+

== Physical Plan ==
* Project (3)
+- BatchEvalPython (2)
   +- * Scan ExistingRDD (1)
'''