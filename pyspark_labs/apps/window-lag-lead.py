from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead
spark = SparkSession.builder.app("Window fn - Lag & Lead").getOrCreate()

data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 150),
    ("B", "2024-01-01", 50),
    ("B", "2024-01-02", 300)
]
df = spark.createDataFrame(data, ["user", "date", "amount"])

window_specs = Window.partitionBy("user").orderBy("amount")
df_lag_lead = df.withColumn("prev_amt-lag", lag("amount", 1).over(window_specs)) \
                .withColumn("next_amt-lead", lead("amount", 1).over(window_specs))

df_lag_lead.show()
df_lag_lead.explain()


'''
====Expected Output===
+----+----------+------+------------+-------------+
|user|      date|amount|prev_amt-lag|next_amt-lead|
+----+----------+------+------------+-------------+
|   A|2024-01-01|   100|        NULL|          150|
|   A|2024-01-03|   150|         100|          200|
|   A|2024-01-02|   200|         150|         NULL|
|   B|2024-01-01|    50|        NULL|          300|
|   B|2024-01-02|   300|          50|         NULL|
+----+----------+------+------------+-------------+

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Window [lag(amount#1017L, -1, null) windowspecdefinition(user#1015, amount#1017L ASC NULLS FIRST, specifiedwindowframe(RowFrame, -1, -1)) AS prev_amt-lag#1021L, lead(amount#1017L, 1, null) windowspecdefinition(user#1015, amount#1017L ASC NULLS FIRST, specifiedwindowframe(RowFrame, 1, 1)) AS next_amt-lead#1026L], [user#1015], [amount#1017L ASC NULLS FIRST]
   +- Sort [user#1015 ASC NULLS FIRST, amount#1017L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(user#1015, 200), ENSURE_REQUIREMENTS, [plan_id=1926]
         +- Scan ExistingRDD[user#1015,date#1016,amount#1017L]
'''

