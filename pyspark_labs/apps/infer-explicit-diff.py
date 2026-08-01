from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
spark = SparkSession.builder.appName("Inference vs Explicit Schema Difference").getOrCreate()

# Schema Inference
df_infer = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/wrong_data_csv")
df_infer.printSchema()
df_infer.show()

# Explicit Schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("signup_date", DateType(), True)
])

df_explicit = spark.read.option("header", "true").schema(schema).csv("/opt/spark-data/wrong_data_csv")
df_explicit.printSchema()
df_explicit.show()

'''
====Output Difference===
--------- Inference Schema ---------
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- signup_date: string (nullable = true)

+---+-------+-----------+
| id|   name|signup_date|
+---+-------+-----------+
|  7|  Grace| 2024-01-07|
|  8| Hannah|  not-found|
|  9|    Ian| 2024-01-09|
| 10|  Julia|  not-found|
|  3|Charlie|  not-found|
|  4|  David| 2024-01-04|
|  1|  Alice| 2024-01-01|
|  2|    Bob| 2024-01-02|
|  5|    Eva|  not-found|
|  6|  Frank| 2024-01-06|
+---+-------+-----------+

--------- Explicit Schema ---------
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- signup_date: date (nullable = true)

It NULL the values where there is inappropriate data
+---+-------+-----------+
| id|   name|signup_date|
+---+-------+-----------+
|  7|  Grace| 2024-01-07|
|  8| Hannah|       NULL|
|  9|    Ian| 2024-01-09|
| 10|  Julia|       NULL|
|  3|Charlie|       NULL|
|  4|  David| 2024-01-04|
|  1|  Alice| 2024-01-01|
|  2|    Bob| 2024-01-02|
|  5|    Eva|       NULL|
|  6|  Frank| 2024-01-06|
+---+-------+-----------+
'''