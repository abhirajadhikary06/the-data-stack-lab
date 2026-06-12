import polars as pl
df = pl.read_csv("sb_dataset.csv")
print(df.head())
df.write_parquet("sb_dataset.parquet")