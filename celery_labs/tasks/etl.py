from celery import shared_task, chain
from celery_app import app
import polars as pl
import os
from datetime import datetime

CSV_FILE_PATH = "source/ecommerce_data.csv"
INTERMEDIATE_PATH = "transformed_data/ingestion_output.parquet"

# INGESTION TASK
@shared_task
def ingestion_task():
    try:
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError("CSV file not found")
        
        df = pl.read_csv(CSV_FILE_PATH)
        os.makedirs("transformed_data", exist_ok=True)
        df.write_parquet(INTERMEDIATE_PATH)  # Save to disk
        
        return {
            "status": "success",
            "stage": "ingestion",
            "timestamp": str(datetime.now()),
            "records": len(df),
            "output_file": INTERMEDIATE_PATH,
        }
    except Exception as e:
        raise self.retry(exc=e, countdown=60, max_retries=3)
        return {"status": "failed", "stage": "ingestion", "error": str(e)}

# TRANSFORMATION TASK (chain input)
@shared_task
def transformation_task(ingestion_result):
    try:
        # Get file path from previous task result
        if ingestion_result.get("status") != "success":
            raise Exception(f"Ingestion failed: {ingestion_result.get('error')}")
        
        input_file = ingestion_result["output_file"]
        df = pl.read_parquet(input_file)
        
        transformed_df = (
            df.with_columns([
                pl.col("order_date")
                .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
                .alias("order_datetime"),
                pl.col("price").cast(pl.Float64, strict=False),
                pl.col("quantity").cast(pl.Int64, strict=False),
                pl.col("status").cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias("status"),
                pl.col("category").cast(pl.Utf8).str.strip_chars().alias("category"),
            ])
            .filter(
                pl.col("order_datetime").is_not_null()
                & (pl.col("price") > 0)
                & (pl.col("quantity") > 0)
            )
            .with_columns([
                pl.col("order_datetime").dt.date().alias("order_date_only"),
                pl.col("order_datetime").dt.time().alias("order_time_only"),
                (pl.col("price") * pl.col("quantity")).alias("order_value"),
                pl.col("order_datetime").dt.weekday().alias("order_weekday"),
                pl.col("order_datetime").dt.hour().alias("order_hour"),
            ])
            .unique(subset=["order_id"])
            .sort(["order_date_only", "order_time_only"])
        )
        
        output_file = "transformed_data/transformation_output.parquet"
        transformed_df.write_parquet(output_file)
        
        return {
            "status": "success",
            "stage": "transformation",
            "timestamp": str(datetime.now()),
            "records": len(transformed_df),
            "output_file": output_file,
        }
    except Exception as e:
        return {"status": "failed", "stage": "transformation", "error": str(e)}

# LOADING TASK (chain input)
@shared_task
def loading_task(transformation_result):
    try:
        if transformation_result.get("status") != "success":
            raise Exception(f"Transformation failed: {transformation_result.get('error')}")
        
        input_file = transformation_result["output_file"]
        df = pl.read_parquet(input_file)
        
        output_dir = "transformed_data"
        os.makedirs(output_dir, exist_ok=True)
        
        out_path = os.path.join(
            output_dir,
            f"transformed_{int(datetime.now().timestamp())}.parquet"
        )
        df.write_parquet(out_path)
        
        return {
            "status": "success",
            "stage": "loading",
            "timestamp": str(datetime.now()),
            "records": len(df),
            "output_path": out_path,
        }
    except Exception as e:
        return {"status": "failed", "stage": "loading", "error": str(e)}

# ORCHESTRATION: Chain tasks together
@shared_task(bind=True)
def daily_etl_pipeline(self):
    try:
        job = chain(
            ingestion_task.s(),
            transformation_task.s(),
            loading_task.s(),
        )
        return job.apply_async()
    except Exception as e:
        raise self.retry(exc=e, countdown=60, max_retries=3) # Retry on failure