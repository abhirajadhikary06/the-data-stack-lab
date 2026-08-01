from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.app("Managed Table").getOrCreate()

data = [
    (1, "Alice", "2024-01-01"),
    (2, "Bob", "2024-01-02"),
    (3, "Charlie", "2024-01-03"),
    (4, "David", "2024-01-04"),
    (5, "Eva", "2024-01-05"),
    (6, "Frank", "2024-01-06"),
    (7, "Grace", "2024-01-07"),
    (8, "Hannah", "2024-01-08"),
    (9, "Ian", "2024-01-09"),
    (10, "Julia", "2024-01-10")
]
columns = ["id", "name", "signup_date"]

df = spark.createDataFrame(data, columns)
df.write.saveAsTable("user_signup_managed")

# SQL Query
spark.sql(
    """
    SELECT * FROM user_signup_managed
    WHERE signup_date > '2024-01-06'
    """
).show()

# Dataframe API 
df_query = spark.table("user_signup_managed")
df_query.filter(col("signup_date")>"2024-01-06").show()