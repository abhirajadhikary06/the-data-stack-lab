from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff
spark = SparkSession.builder.appName("Basic Transformation").getOrCreate()

# Modifying DataFrame without triggering execution
# select --> filter --> withColumn

basic_df = spark.read.option("inferSchema", "true").option("header","true").csv("/opt/spark-data/user_data_csv")

# Select
df_selected = basic_df.select("name", "signup_date")

# Filter
df_filtered = df_selected.filter(col("name").like("G%"))

# withColumn
df_newcol = df_filtered.withColumn(
    "active time", datediff(current_date(), col("signup_date"))
)

df_newcol.show()