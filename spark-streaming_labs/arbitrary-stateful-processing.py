# Arbitrary Stateful Processing --> For logic that doesn’t reduce to a built-in sum/count/avg per window — for instance, tracking a “heatwave streak” per city (how many consecutive readings above threshold, resetting once temp drops).
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.streaming.state import GroupStateTimeout
import pandas as pd

spark = SparkSession.builder \
    .appName("Arbitrary Stateful Processing") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

output_schema = StructType([
    StructField("city", StringType()),
    StructField("consecutive_hot_readings", LongType()),
])

state_schema = StructType([
    StructField("streak", LongType())
])

def track_heat_streak(city, batch_iter, state):
    streak = 0
    if state.exists:
        streak = state.get[0]

    for batch_df in batch_iter:
        for temp in batch_df["temp_c"]:
            streak = streak + 1 if temp > 35 else 0

    state.update((streak,))
    state.setTimeoutDuration("30 minutes")

    yield pd.DataFrame({
        "city": [city],
        "consecutive_hot_readings": [streak]
    })

heat_streaks = parsed_ts.groupBy("city").applyInPandasWithState(
    track_heat_streak,
    outputStructType=output_schema,
    stateStructType=state_schema,
    outputMode="update",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
)
