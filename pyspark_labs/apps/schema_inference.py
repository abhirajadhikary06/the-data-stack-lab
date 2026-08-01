from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Schema Inference").getOrCreate()

df_infer = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/user_data_csv")
df_infer.printSchema()
df_infer.show()

'''
====Expected Output for the printSchema()====
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- signup_date: date (nullable = true)
'''
