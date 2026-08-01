from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
spark = SparkSession.builder.app("Window fn - Dense Rank").getOrCreate()

data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 150),
    ("B", "2024-01-01", 50),
    ("B", "2024-01-02", 300)
]
df = spark.createDataFrame(data, ["user", "date", "amount"])

window_specs = Window.partitionBy("user").orderBy("amount")
df_rank = df.withColumn("dense_rank", dense_rank().over(window_specs))
df_rank.show()
df_rank.explain()


'''
====Expected Output===
+----+----------+------+----------+
|user|      date|amount|dense_rank|
+----+----------+------+----------+
|   A|2024-01-01|   100|         1|
|   A|2024-01-03|   150|         2|
|   A|2024-01-02|   200|         3|
|   B|2024-01-01|    50|         1|
|   B|2024-01-02|   300|         2|
+----+----------+------+----------+

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Window [dense_rank(amount#981L) windowspecdefinition(user#979, amount#981L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS dense_rank#987], [user#979], [amount#981L ASC NULLS FIRST]
   +- Sort [user#979 ASC NULLS FIRST, amount#981L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(user#979, 200), ENSURE_REQUIREMENTS, [plan_id=1861]
         +- Scan ExistingRDD[user#979,date#980,amount#981L]
'''

