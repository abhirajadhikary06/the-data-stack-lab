from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.appName("Shuffle vs Broadcast Join").getOrCreate()
# Large dataset
orders = [(i, i % 1000, i * 10) for i in range(1_000_000)]
df_orders = spark.createDataFrame(orders, ["order_id", "customer_id", "amount"])

# Small lookup dataset
customers = [(i, f"Customer_{i}") for i in range(1000)]
df_customers = spark.createDataFrame(customers, ["customer_id", "name"])

# Shuffle Join
df_shuffle_join = df_orders.join(df_customers, "customer_id")
df_shuffle_join.explain(mode="formatted")
df_shuffle_join.show()

# Broadcast Join
df_broadcast_join = df_orders.join(broadcast(df_customers), "customer_id")
df_broadcast_join.explain(mode="formatted")
df_broadcast_join.show()

'''
====Physical Plan Difference of Shuffle & Broadcast====
----- Shuffle Join -----
== Physical Plan ==
AdaptiveSparkPlan (11)
+- Project (10)
   +- SortMergeJoin Inner (9)
      :- Sort (4)
      :  +- Exchange (3)
      :     +- Filter (2)
      :        +- Scan ExistingRDD (1)
      +- Sort (8)
         +- Exchange (7)
            +- Filter (6)
               +- Scan ExistingRDD (5)

----- Broadcast Join -----
== Physical Plan ==
AdaptiveSparkPlan (8)
+- Project (7)
   +- BroadcastHashJoin Inner BuildRight (6)
      :- Filter (2)
      :  +- Scan ExistingRDD (1)
      +- BroadcastExchange (5)
         +- Filter (4)
            +- Scan ExistingRDD (3)
'''