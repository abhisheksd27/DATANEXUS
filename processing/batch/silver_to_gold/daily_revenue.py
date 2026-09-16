"""
DataNexus Batch ETL: Silver to Gold - Daily Revenue Aggregation
Reads cleaned Orders and Users from Silver zone, performs
multi-domain join, aggregates daily business KPIs by city and date,
and outputs business-ready Parquet to Gold zone for BigQuery/Athena serving.
"""

import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as spark_sum, avg as spark_avg,
    count as spark_count, countDistinct, when, round as spark_round,
    current_timestamp
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DailyRevenueGold")

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SILVER_ORDERS_PATH = str(PROJECT_ROOT / "datalake" / "silver" / "orders")
SILVER_USERS_PATH = str(PROJECT_ROOT / "datalake" / "silver" / "users")
GOLD_REVENUE_PATH = str(PROJECT_ROOT / "datalake" / "gold" / "daily_revenue")

def create_spark_session():
    return (
        SparkSession.builder
        .appName("DataNexus-SilverToGold-DailyRevenue")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def run_etl():
    spark = create_spark_session()
    logger.info("Spark session initialized.")

    try:
        logger.info(f"Reading Silver Orders from: {SILVER_ORDERS_PATH}")
        df_orders = spark.read.parquet(SILVER_ORDERS_PATH)

        logger.info(f"Reading Silver Users from: {SILVER_USERS_PATH}")
        df_users = spark.read.parquet(SILVER_USERS_PATH)

        # Enrich Orders with User profile data
        df_joined = (
            df_orders.alias("o")
            .join(
                df_users.alias("u"),
                col("o.user_id") == col("u.user_id"),
                "left"
            )
            .select(
                col("o.order_id"),
                col("o.user_id"),
                to_date(col("o.order_timestamp")).alias("order_date"),
                col("o.city").alias("order_city"),
                col("o.total_amount"),
                col("o.status"),
                col("u.full_name").alias("customer_name")
            )
        )

        # Compute Daily Revenue Aggregates
        df_gold_revenue = (
            df_joined
            .groupBy("order_date", "order_city")
            .agg(
                spark_round(spark_sum(when(col("status") == "COMPLETED", col("total_amount")).otherwise(0.0)), 2).alias("gross_revenue"),
                spark_round(spark_avg(when(col("status") == "COMPLETED", col("total_amount"))), 2).alias("avg_order_value"),
                spark_count("order_id").alias("total_orders"),
                spark_sum(when(col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_orders"),
                spark_sum(when(col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"),
                countDistinct("user_id").alias("unique_purchasers")
            )
            .withColumn("order_fulfillment_rate", spark_round((col("completed_orders") / col("total_orders")) * 100, 1))
            .withColumn("calculated_at", current_timestamp())
        )

        logger.info(f"Gold Daily Revenue rows computed: {df_gold_revenue.count()}")

        # Write to Gold partitioned by order_date
        (
            df_gold_revenue.write
            .mode("overwrite")
            .partitionBy("order_date")
            .parquet(GOLD_REVENUE_PATH)
        )
        logger.info(f"Gold Daily Revenue successfully saved to: {GOLD_REVENUE_PATH}")

    except Exception as e:
        logger.error(f"Daily Revenue Gold ETL failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()
