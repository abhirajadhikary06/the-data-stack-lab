from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("StructType Schema").getOrCreate()

struct_schema=StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ]), True)
])
df_struct = spark.read.schema(struct_schema).json("/opt/spark-data/nested_users")
df_struct.printSchema()
df_struct.select(col("location.country")).show()