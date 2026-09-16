"""
DataNexus Batch ETL: Silver to Gold - User Behavior Analytics
Reads clickstream events from Bronze Data Lake, aggregates
user interaction patterns (views, searches, cart additions, checkouts),
and produces Gold analytical tables for user segmentation and recommendation engines.
"""

import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, sum as spark_sum, count as spark_count,
    when, round as spark_round, current_timestamp
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("UserBehaviorGold")

PROJECT_ROOT = Path(__file__).resolve().parents[4]
BRONZE_CLICKSTREAM_PATH = str(PROJECT_ROOT / "datalake" / "bronze" / "kafka" / "clickstream")
GOLD_BEHAVIOR_PATH = str(PROJECT_ROOT / "datalake" / "gold" / "user_behavior")

def create_spark_session():
    return (
        SparkSession.builder
        .appName("DataNexus-SilverToGold-UserBehavior")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def run_etl():
    spark = create_spark_session()
    logger.info("Spark session initialized.")

    try:
        clickstream_dir = Path(BRONZE_CLICKSTREAM_PATH)
        if clickstream_dir.exists() and any(clickstream_dir.glob("*.json")):
            logger.info(f"Reading clickstream from: {BRONZE_CLICKSTREAM_PATH}")
            df_clicks = spark.read.json(BRONZE_CLICKSTREAM_PATH)
        else:
            logger.info("Bronze clickstream empty. Generating baseline session events.")
            sample_data = [
                ("evt-01", "usr-001", "PAGE_VIEW", None, None, "mobile_android", "Mumbai", "2026-08-18T10:00:00"),
                ("evt-02", "usr-001", "SEARCH", None, "headphones", "mobile_android", "Mumbai", "2026-08-18T10:01:00"),
                ("evt-03", "usr-001", "PRODUCT_VIEW", "prod-A", None, "mobile_android", "Mumbai", "2026-08-18T10:02:00"),
                ("evt-04", "usr-001", "ADD_TO_CART", "prod-A", None, "mobile_android", "Mumbai", "2026-08-18T10:03:00"),
                ("evt-05", "usr-001", "CHECKOUT_START", "prod-A", None, "mobile_android", "Mumbai", "2026-08-18T10:05:00"),
                ("evt-06", "usr-002", "PAGE_VIEW", None, None, "desktop_chrome", "Bengaluru", "2026-08-18T11:00:00"),
                ("evt-07", "usr-002", "SEARCH", None, "gaming mouse", "desktop_chrome", "Bengaluru", "2026-08-18T11:02:00"),
                ("evt-08", "usr-002", "PRODUCT_VIEW", "prod-B", None, "desktop_chrome", "Bengaluru", "2026-08-18T11:04:00"),
                ("evt-09", "usr-003", "PAGE_VIEW", None, None, "mobile_ios", "Delhi", "2026-08-18T12:00:00"),
                ("evt-10", "usr-003", "PRODUCT_VIEW", "prod-C", None, "mobile_ios", "Delhi", "2026-08-18T12:05:00"),
                ("evt-11", "usr-003", "ADD_TO_CART", "prod-C", None, "mobile_ios", "Delhi", "2026-08-18T12:06:00"),
            ]
            columns = ["event_id", "user_id", "event_type", "product_id", "search_query", "device_type", "city", "timestamp"]
            df_clicks = spark.createDataFrame(sample_data, schema=columns)

        # Standardize and compute behavior metrics
        df_parsed = (
            df_clicks
            .withColumn("event_timestamp", to_timestamp(col("timestamp")))
            .withColumn("event_date", to_date(col("event_timestamp")))
        )

        df_behavior = (
            df_parsed
            .groupBy("user_id", "event_date", "city")
            .agg(
                spark_count("event_id").alias("total_interactions"),
                spark_sum(when(col("event_type") == "PAGE_VIEW", 1).otherwise(0)).alias("page_views"),
                spark_sum(when(col("event_type") == "SEARCH", 1).otherwise(0)).alias("searches"),
                spark_sum(when(col("event_type") == "PRODUCT_VIEW", 1).otherwise(0)).alias("product_views"),
                spark_sum(when(col("event_type") == "ADD_TO_CART", 1).otherwise(0)).alias("cart_adds"),
                spark_sum(when(col("event_type") == "CHECKOUT_START", 1).otherwise(0)).alias("checkout_starts")
            )
            .withColumn(
                "cart_to_checkout_rate",
                when(col("cart_adds") > 0, spark_round((col("checkout_starts") / col("cart_adds")) * 100, 1)).otherwise(0.0)
            )
            .withColumn("calculated_at", current_timestamp())
        )

        logger.info(f"User behavior records aggregated: {df_behavior.count()}")

        (
            df_behavior.write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(GOLD_BEHAVIOR_PATH)
        )
        logger.info(f"Gold User Behavior Parquet saved to: {GOLD_BEHAVIOR_PATH}")

    except Exception as e:
        logger.error(f"User Behavior Gold ETL failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()
