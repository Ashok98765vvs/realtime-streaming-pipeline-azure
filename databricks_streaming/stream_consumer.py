from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp,
    window, avg, max, min, count
)
from schema import STOCK_EVENT_SCHEMA
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-events")
BRONZE_PATH = "/mnt/datalake/bronze/stock_events"
SILVER_PATH = "/mnt/datalake/silver/stock_events_clean"
GOLD_PATH = "/mnt/datalake/gold/stock_agg_5min"
CHECKPOINT_BASE = "/mnt/datalake/checkpoints"


def get_spark():
    return (
        SparkSession.builder
        .appName("RealTime-Stock-Streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    return (
        raw_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), STOCK_EVENT_SCHEMA).alias("data")
        )
        .select("kafka_key", "kafka_partition", "kafka_offset", "kafka_timestamp", "data.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn("ingested_at", current_timestamp())
    )


def write_bronze(parsed_df):
    return (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
        .option("mergeSchema", "true")
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )


def build_silver_stream(parsed_df):
    return (
        parsed_df
        .filter(col("current_price") > 0)
        .filter(col("volume") > 0)
        .filter(col("ticker").isNotNull())
        .withColumn("price_range", col("high") - col("low"))
        .withColumn("price_change_pct",
                    ((col("current_price") - col("open")) / col("open") * 100).cast("decimal(10,4)"))
    )


def write_silver(silver_df):
    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver")
        .trigger(processingTime="15 seconds")
        .start(SILVER_PATH)
    )


def build_gold_aggregates(silver_df):
    return (
        silver_df
        .withWatermark("event_timestamp", "1 minute")
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("ticker")
        )
        .agg(
            avg("current_price").alias("avg_price"),
            max("current_price").alias("max_price"),
            min("current_price").alias("min_price"),
            avg("volume").alias("avg_volume"),
            count("*").alias("event_count"),
            avg("price_change_pct").alias("avg_price_change_pct")
        )
    )


def write_gold(gold_df):
    return (
        gold_df.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold")
        .trigger(processingTime="30 seconds")
        .start(GOLD_PATH)
    )


def main():
    spark = get_spark()
    raw_df = read_kafka_stream(spark)
    parsed_df = parse_events(raw_df)
    silver_df = build_silver_stream(parsed_df)
    gold_df = build_gold_aggregates(silver_df)

    bronze_query = write_bronze(parsed_df)
    silver_query = write_silver(silver_df)
    gold_query = write_gold(gold_df)

    bronze_query.awaitTermination()
    silver_query.awaitTermination()
    gold_query.awaitTermination()


if __name__ == "__main__":
    main()
