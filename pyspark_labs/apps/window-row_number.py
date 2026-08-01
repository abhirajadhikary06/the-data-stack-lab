from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
spark = SparkSession.builder.app("Window fn - Row Number").getOrCreate()

data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 150),
    ("B", "2024-01-01", 50),
    ("B", "2024-01-02", 300)
]
df = spark.createDataFrame(data, ["user", "date", "amount"])

window_specs = Window.partitionBy("user").orderBy("amount")
df_rownumber = df.withColumn("row_number", row_number().over(window_specs))
df_rownumber.show()
df_rownumber.explain()


'''
====Expected Output===
+----+----------+------+----------+
|user|      date|amount|row_number|
+----+----------+------+----------+
|   A|2024-01-01|   100|         1|
|   A|2024-01-03|   150|         2|
|   A|2024-01-02|   200|         3|
|   B|2024-01-01|    50|         1|
|   B|2024-01-02|   300|         2|
+----+----------+------+----------+

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Window [row_number() windowspecdefinition(user#891, amount#893L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_number#898], [user#891], [amount#893L ASC NULLS FIRST]
   +- Sort [user#891 ASC NULLS FIRST, amount#893L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(user#891, 200), ENSURE_REQUIREMENTS, [plan_id=1677]
         +- Scan ExistingRDD[user#891,date#892,amount#893L]
'''