from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder \
    .appName("Lazy Execution") \
    .getOrCreate()     

data = [(i, i%10, i**10) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["value", "mod", "power"])
filtered_df = df.filter(col("power")>50000)
df_selected = filtered_df.select("value", "mod") # creates only a logical plan
df_selected.show() # action triggered runs the logical plan - creates a physical plan
