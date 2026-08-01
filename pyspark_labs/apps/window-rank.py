from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
spark = SparkSession.builder.app("Window fn - Rank").getOrCreate()

data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 150),
    ("B", "2024-01-01", 50),
    ("B", "2024-01-02", 300)
]
df = spark.createDataFrame(data, ["user", "date", "amount"])

window_specs = Window.partitionBy("user").orderBy("amount")
df_rank = df.withColumn("rank", rank().over(window_specs))
df_rank.show()
df_rank.explain()


'''
====Expected Output===
+----+----------+------+----+
|user|      date|amount|rank|
+----+----------+------+----+
|   A|2024-01-01|   100|   1|
|   A|2024-01-03|   150|   2|
|   A|2024-01-02|   200|   3|
|   B|2024-01-01|    50|   1|
|   B|2024-01-02|   300|   2|
+----+----------+------+----+

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Window [rank(amount#922L) windowspecdefinition(user#920, amount#922L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#928], [user#920], [amount#922L ASC NULLS FIRST]
   +- Sort [user#920 ASC NULLS FIRST, amount#922L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(user#920, 200), ENSURE_REQUIREMENTS, [plan_id=1742]
         +- Scan ExistingRDD[user#920,date#921,amount#922L]
'''