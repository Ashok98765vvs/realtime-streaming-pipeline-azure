"""
Bronze Layer — Raw event landing zone (no transformations)
Medallion Architecture: Raw → Bronze → Silver → Gold
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

BRONZE_TABLE = "stock_db.bronze_stock_events"
BRONZE_PATH = "/mnt/datalake/bronze/stock_events"


def optimize_bronze(spark):
    spark.sql(f"OPTIMIZE delta.`{BRONZE_PATH}` ZORDER BY (ticker, event_timestamp)")
    spark.sql(f"VACUUM delta.`{BRONZE_PATH}` RETAIN 168 HOURS")
    print("Bronze layer optimized and vacuumed.")


def create_bronze_table(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
        USING DELTA
        LOCATION '{BRONZE_PATH}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.logRetentionDuration' = 'interval 30 days'
        )
    """)
    print(f"Bronze table {BRONZE_TABLE} registered.")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze-Layer").getOrCreate()
    create_bronze_table(spark)
    optimize_bronze(spark)
