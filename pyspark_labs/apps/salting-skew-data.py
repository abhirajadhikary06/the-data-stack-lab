from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, concat, lit, when
spark = SparkSession.builder.appName("Salting Skew Data").getOrCreate()

data = []
for i in range(100000):
    data.append(("HOT_KEY", i))
for i in range(1000):
    data.append((f"KEY_{i}", i))

df = spark.createDataFrame(data, ["key", "value"])

df_salted = df.withColumn(
    "salted_key",
    concat(df.key, lit("_"), (rand()*10).cast("int"))
)
df_salted.groupBy("salted_key").count().show(50)
df_salted.explain(mode="formatted")


# Concat function only on HOTKEY
df_salted_hotkey = df.withColumn(
    "salted_key",
    when(
        col("key") == "HOT_KEY",
        concat(col("key"), lit("_"), (rand()*10).cast("int"))
    ).otherwise(col("key"))
)

df_salted_hotkey.groupBy("salted_key").count().show()
df_salted_hotkey.explain(mode="formatted")

'''
====Expected Output====
+----------+-----+
|salted_key|count|
+----------+-----+
| HOT_KEY_7| 9824|
| HOT_KEY_2|10081|
| HOT_KEY_8| 9963|
| HOT_KEY_6|10033|
| HOT_KEY_0| 9935|
| HOT_KEY_5|10026|
| HOT_KEY_1|10019|
| HOT_KEY_3|10130|
| HOT_KEY_4| 9991|
| HOT_KEY_9| 9998|
|   KEY_646|    1|
|   KEY_905|    1|
|   KEY_909|    1|
|    KEY_26|    1|
|   KEY_118|    1|
|   KEY_140|    1|
|   KEY_150|    1|
|   KEY_183|    1|
|   KEY_244|    1|
|   KEY_464|    1|
+----------+-----+

== Physical Plan ==
* Project (2)
+- * Scan ExistingRDD (1)
'''