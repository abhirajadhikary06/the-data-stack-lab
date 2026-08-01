from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SQL-vs-DataFrames").getOrCreate()


# Create table
data = [(1, 500), (2, 1500), (3, 2500)]
df = spark.createDataFrame(data, ["id", "amount"])

# DataFrame query
df_df = df.filter(df.amount > 1000)
df_df.explain(True)

# SQL query
df.createOrReplaceTempView("sales") # It allows you to query the DataFrame using Spark SQL instead of only the DataFrame API.
df_sql = spark.sql("SELECT * FROM sales WHERE amount > 1000")
df_sql.explain(True)