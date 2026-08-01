from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Writer API").getOrCreate()

data = [
    (1, "Alice", "2024-01-01"),
    (2, "Bob", "2024-01-02"),
    (3, "Charlie", "not-found"),
    (4, "David", "2024-01-04"),
    (5, "Eva", "not-found"),
    (6, "Frank", "2024-01-06"),
    (7, "Grace", "2024-01-07"),
    (8, "Hannah", "not-found"),
    (9, "Ian", "2024-01-09"),
    (10, "Julia", "not-found")
]
columns = ["id", "name", "signup_date"]

df_wrong = spark.createDataFrame(data, columns)
write_path = "/opt/spark-data/wrong_data_csv"
df_wrong.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .save(write_path)
