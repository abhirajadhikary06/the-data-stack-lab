import polars as pl
from dotenv import load_dotenv
load_dotenv()
from lakefs_client import download_object, create_branch, commit, get_objects, delete_object, upload_object
from quality.validity import validate_broadcasts
from transformation.clean import clean_broadcasts
from transformation.parquet import write_parquet

# Simple Pipeline (no merge/commit/upload) to demonstrate transformations and lakeFS integration
branch = create_branch("lakefs-tutorial", "main", "feature-polars")

# Download to data/raw
local_csv = download_object(
	repo="lakefs-tutorial",
	branch="main",
	object_path="sb_dataset_v3.csv",
	destination="data/raw/sb_dataset_v3.csv",
)

# Read and run transformations
df = validate_broadcasts(df)
df = pl.read_csv(local_csv)
df = clean_broadcasts(df)

# Write output to data/processed
write_parquet(df, "data/processed/sb_dataset_v3_cleaned.parquet")

