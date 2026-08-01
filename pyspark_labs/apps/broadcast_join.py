from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.appName("Broadcast Join").getOrCreate()

# Large dataset
orders = [(i, i % 1000, i * 10) for i in range(1_000_000)]
df_orders = spark.createDataFrame(orders, ["order_id", "customer_id", "amount"])

# Small lookup dataset
customers = [(i, f"Customer_{i}") for i in range(1000)]
df_customers = spark.createDataFrame(customers, ["customer_id", "name"])

# Broadcast Join
df_broadcast_join = df_orders.join(broadcast(df_customers), "customer_id")
df_broadcast_join.show()