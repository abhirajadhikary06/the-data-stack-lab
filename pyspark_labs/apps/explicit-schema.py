from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
spark = SparkSession.builder.appName("Explicit Schema").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("signup_date", DateType(), True)
])

df_explicit = spark.read.option("header", "true").schema(schema).csv("/opt/spark-data/user_data_csv")
df_explicit.printSchema()
df_explicit.show()