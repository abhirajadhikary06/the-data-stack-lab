from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Shuffle Join").getOrCreate()

# Large dataset
orders = [(i, i % 1000, i * 10) for i in range(1_000_000)]
df_orders = spark.createDataFrame(orders, ["order_id", "customer_id", "amount"])

# Small lookup dataset
customers = [(i, f"Customer_{i}") for i in range(1000)]
df_customers = spark.createDataFrame(customers, ["customer_id", "name"])

# Shuffle Join
df_shuffle_join = df_orders.join(df_customers, "customer_id")
df_shuffle_join.show()