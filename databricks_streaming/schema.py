from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

STOCK_EVENT_SCHEMA = StructType([
    StructField("ticker", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("market_cap", LongType(), True),
    StructField("52_week_high", DoubleType(), True),
    StructField("52_week_low", DoubleType(), True),
    StructField("event_type", StringType(), True),
    StructField("source", StringType(), True),
])
