from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = SparkSession.builder.appName("JSON Write").getOrCreate()

data = [(1, "Alice", Row(city="NY", country="USA")),
        (2, "Bob", Row(city="LA", country="USA"))]
columns = ["id", "name", "location"]
nested_df = spark.createDataFrame(data, columns)
nested_df.write \
    .mode("overwrite") \
    .json("/opt/spark-data/nested_users")

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