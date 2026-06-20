import polars as pl


def validate_broadcasts(df: pl.DataFrame) -> pl.DataFrame:
    # 1. Ensuring no `NaN` values in `event_date` and `duration_minutes`
    df = df.filter(
        pl.col("event_date").is_not_null() & pl.col("duration_minutes").is_not_null()
    )

    # 2. Ensure broadcast_id is unique first
    unique_df = df.unique(subset=["broadcast_id"], keep="first")
    
    # 3. Perform row-wise filtering
    valid_df = unique_df.filter(
        (pl.col("duration_minutes") > 0) & (pl.col("duration_minutes") < 300),
        pl.col("event_date").is_not_null(),
        pl.col("event_date").cast(pl.Utf8, strict=False) != "31-12-9999",
    )
    return valid_df