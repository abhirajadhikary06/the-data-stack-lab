import polars as pl

def aggregate_broadcasts(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        # Derived columns
        .with_columns(
            # Stream quality scoring
            pl.when(pl.col("stream_quality") == "4K").then(3)
            .when(pl.col("stream_quality") == "Full HD").then(2)
            .when(pl.col("stream_quality") == "HD").then(1)
            .otherwise(0)
            .alias("points"),

            # Duration in hours
            (pl.col("duration_minutes") / 60)
            .round(2)
            .alias("duration_hours")
        )

        # Aggregate by sport
        .group_by("sport")
        .agg(
            # Duration metrics
            pl.sum("duration_minutes").alias("total_duration_minutes"),
            pl.sum("duration_hours").round(2).alias("total_duration_hours"),
            pl.mean("duration_minutes").round(2).alias("avg_duration_minutes"),
            pl.max("duration_minutes").alias("max_duration_minutes"),
            pl.min("duration_minutes").alias("min_duration_minutes"),

            # Quality metrics
            pl.sum("points").alias("total_points"),
            pl.mean("points").round(2).alias("avg_quality_score"),

            # Viewer metrics
            pl.sum("live_viewers").alias("total_live_viewers"),
            pl.mean("live_viewers").round(0).alias("avg_live_viewers"),
            pl.max("live_viewers").alias("max_live_viewers"),

            pl.mean("peak_viewers").round(0).alias("avg_peak_viewers"),
            pl.max("peak_viewers").alias("highest_peak_viewers"),

            # Revenue metrics
            pl.sum("ad_revenue_usd").round(2).alias("total_ad_revenue"),
            pl.mean("ad_revenue_usd").round(2).alias("avg_ad_revenue"),
            pl.max("ad_revenue_usd").round(2).alias("highest_ad_revenue"),

            # Operational metrics
            pl.n_unique("broadcast_id").alias("total_broadcasts"),
            pl.n_unique("network").alias("unique_networks_covered"),

            # Efficiency metrics
            (
                pl.sum("ad_revenue_usd") /
                pl.sum("duration_minutes")
            ).round(2).alias("revenue_per_minute"),

            (
                pl.sum("ad_revenue_usd") /
                pl.sum("live_viewers")
            ).round(4).alias("revenue_per_viewer"),

            (
                pl.sum("live_viewers") /
                pl.n_unique("broadcast_id")
            ).round(0).alias("avg_viewers_per_broadcast")
        )

        # Sort by revenue
        .sort("total_ad_revenue", descending=True)
    )