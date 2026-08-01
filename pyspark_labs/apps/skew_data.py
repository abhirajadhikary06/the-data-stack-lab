from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Skew Data").getOrCreate()

data = []
for i in range(100000):
    data.append(("HOT_KEY", i))
for i in range(1000):
    data.append((f"KEY_{i}", i))

df = spark.createDataFrame(data, ["key", "value"])
df_grouped = df.groupBy("key").count()
df_grouped.show()
df_grouped.explain()

'''
====Expected Output====
+-------+-----+
|    key|count|
+-------+-----+
| KEY_26|    1|
|KEY_118|    1|
|KEY_140|    1|
|KEY_150|    1|
|KEY_183|    1|
|KEY_244|    1|
|KEY_204|    1|
|KEY_108|    1|
|  KEY_3|    1|
| KEY_61|    1|
| KEY_71|    1|
|KEY_233|    1|
|KEY_200|    1|
|KEY_111|    1|
|KEY_236|    1|
|KEY_169|    1|
| KEY_64|    1|
|KEY_229|    1|
|KEY_210|    1|
|KEY_212|    1|
+-------+-----+

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[key#1083], functions=[count(1)])
   +- Exchange hashpartitioning(key#1083, 200), ENSURE_REQUIREMENTS, [plan_id=1990]
      +- HashAggregate(keys=[key#1083], functions=[partial_count(1)])
         +- Project [key#1083]
            +- Scan ExistingRDD[key#1083,value#1084L]
'''