import polars as pl


def clean_broadcasts(df: pl.DataFrame) -> pl.DataFrame:
    # 1. Remove duplicates based on `broadcast_id`
    df = df.unique(subset=["broadcast_id"], keep="first")

    # 2. Filter out rows with invalid `duration_minutes` (e.g., negative or excessively long durations)
    df = df.filter((pl.col("duration_minutes") > 0) & (pl.col("duration_minutes") < 300))

    # 3. Filter out rows with invalid `event_date` (e.g., null values or unrealistic dates)
    df = df.filter(
        pl.col("event_date").is_not_null() &
        (pl.col("event_date").cast(pl.Utf8, strict=False) != "31-12-9999")
    )

    return df