from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Reader API").getOrCreate()

load_path = "/opt/spark-data/user_data"
df_read = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(load_path)

df_read.show()

'''
====Expected Output====
+---+-------+-----------+
| id|   name|signup_date|
+---+-------+-----------+
|  3|Charlie| 2024-01-03|
|  1|  Alice| 2024-01-01|
|  2|    Bob| 2024-01-02|
+---+-------+-----------+
'''
