import os
from pathlib import Path
import polars as pl
from dotenv import load_dotenv
load_dotenv()

def write_parquet(df: pl.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)