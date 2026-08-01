from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Spark DAG").getOrCreate()

data = [(i, i%5) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["category", "value"])

result_df = ( 
    df.filter(col("value") > 1000) 
      .groupBy("category") 
      .count() 
)
result_df.explain(mode="formatted")
result_df.collect()


'''
===== Expected Output =====
== Physical Plan ==
AdaptiveSparkPlan (7)
+- HashAggregate (6)
   +- Exchange (5)
      +- HashAggregate (4)
         +- Project (3)
            +- Filter (2)
               +- Scan ExistingRDD (1)


(1) Scan ExistingRDD
Output [2]: [category#54L, value#55L]
Arguments: [category#54L, value#55L], MapPartitionsRDD[30] at applySchemaToPythonRDD at <unknown>:0, ExistingRDD, UnknownPartitioning(0)

(2) Filter
Input [2]: [category#54L, value#55L]
Condition : (isnotnull(value#55L) AND (value#55L > 1000))

(3) Project
Output [1]: [category#54L]
Input [2]: [category#54L, value#55L]

(4) HashAggregate
Input [1]: [category#54L]
Keys [1]: [category#54L]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#64L]
Results [2]: [category#54L, count#65L]

(5) Exchange
Input [2]: [category#54L, count#65L]
Arguments: hashpartitioning(category#54L, 200), ENSURE_REQUIREMENTS, [plan_id=95]

(6) HashAggregate
Input [2]: [category#54L, count#65L]
Keys [1]: [category#54L]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#60L]
Results [2]: [category#54L, count(1)#60L AS count#61L]

(7) AdaptiveSparkPlan
Output [2]: [category#54L, count#61L]
Arguments: isFinalPlan=false
'''