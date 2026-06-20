import polars as pl

def aggregate_broadcasts(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        # 1. Create the points column dynamically
        .with_columns(
            pl.when(pl.col("stream_quality") == "4K").then(3)
            .when(pl.col("stream_quality") == "Full HD").then(2)
            .when(pl.col("stream_quality") == "HD").then(1)
            .otherwise(0)
            .alias("points")
        )
        # 2. Group by sport and aggregate metrics
        .group_by("sport")
        .agg(
            # Total duration and calculated points
            pl.sum("duration_minutes").alias("total_duration"),
            pl.sum("points").alias("total_points"),
            
            # Engagement metrics
            pl.sum("live_viewers").alias("total_live_viewers"),
            pl.mean("peak_viewers").round(0).alias("avg_peak_viewers"),
            
            # Financial metrics
            pl.sum("ad_revenue_usd").alias("total_ad_revenue"),
            
            # Counting unique occurrences
            pl.n_unique("broadcast_id").alias("total_broadcasts"),
            pl.n_unique("network").alias("unique_networks_covered")
        )
        # Optional: Sort by total revenue to see your highest-earning sports first
        .sort("total_ad_revenue", descending=True)
    )
