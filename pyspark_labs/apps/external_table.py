from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.app("External Table").getOrCreate()

data = [
    (1, "Alice", "Pizza", 299),
    (2, "Bob", "Burger", 199),
    (3, "Charlie", "Pasta", 349),
    (4, "David", "Sushi", 499),
    (5, "Eva", "Sandwich", 149),
    (6, "Frank", "Salad", 179),
    (7, "Grace", "Tacos", 229),
    (8, "Hannah", "Noodles", 199),
    (9, "Ian", "Biryani", 399),
    (10, "Julia", "Wrap", 159),
    (11, "Alice", "Burger", 199),
    (12, "Bob", "Pizza", 299),
    (13, "Charlie", "Salad", 179),
    (14, "David", "Wrap", 159),
    (15, "Eva", "Noodles", 199),
    (16, "Frank", "Pizza", 299),
    (17, "Grace", "Sandwich", 149),
    (18, "Hannah", "Biryani", 399),
    (19, "Ian", "Sushi", 499),
    (20, "Julia", "Pasta", 349)
]

columns = ["order_id", "customer_name", "food_item", "price"]

df = spark.createDataFrame(data, columns)
df.write \
    .mode("overwrite") \
    .option("path", "/opt/spark-data/ext_food_del_table") \
    .saveAsTable("ext_food_del_table")

spark.sql("SELECT * FROM ext_food_del_table").show()
spark.sql(
    """
    SELECT customer_name, COUNT(*) AS order_count
    FROM ext_food_del_table
    GROUP BY customer_name
    HAVING COUNT(*) > 1
    """
).show()
spark.sql(
    """
    SELECT customer_name, SUM(price) AS total_spent
    FROM ext_food_del_table
    GROUP BY customer_name
    HAVING COUNT(*) > 1
    """
).show()


