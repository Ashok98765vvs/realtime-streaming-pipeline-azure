from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
    TimestampType,
)

STOCK_EVENT_SCHEMA = StructType([
    StructField("ticker", StringType(), False),
    # Use real timestamp instead of string
    StructField("event_timestamp", TimestampType(), False),
    
    # OHLC + last traded price
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),            # renamed from current_price
    StructField("volume", LongType(), True),             # 64-bit, typical for volume
    
    # Make market_cap nullable & consider Double if you expect > 9e18
    StructField("market_cap", DoubleType(), True),
    
    StructField("week_52_high", DoubleType(), True),
    StructField("week_52_low", DoubleType(), True),

    # Event metadata
    StructField("event_type", StringType(), True),
    StructField("source", StringType(), True),
])
