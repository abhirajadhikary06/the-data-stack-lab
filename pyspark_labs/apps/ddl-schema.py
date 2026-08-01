from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DDL Schema").getOrCreate()

ddl_schema = """
id INT,
name STRING,
location STRUCT<city:STRING, country:STRING>
"""

df_ddl = spark.read.schema(ddl_schema).json("/opt/spark-data/nested_users")
df_ddl.printSchema()
df_ddl.select(col("location.country")).show()

'''
====Expected Output====
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- location: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)

+-------+
|country|
+-------+
|    USA|
|    USA|
+-------+
'''