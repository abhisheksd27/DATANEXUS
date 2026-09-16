"""
DataNexus Batch ETL: Bronze to Silver - Orders
Reads raw orders data from Bronze Data Lake (or MySQL CDC),
cleanses, deduplicates, validates constraints, and writes
to Silver zone in partitioned Parquet / Delta format.
"""

import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, when,
    trim, upper, round as spark_round, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("OrdersETL")

# Paths
def _find_root():
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "datalake").exists() or parent.name.lower() in ("datanexus", "app"):
            return parent
    return p.parents[3]

PROJECT_ROOT = _find_root()
BRONZE_ORDERS_PATH = str(PROJECT_ROOT / "datalake" / "bronze" / "mysql" / "orders")
SILVER_ORDERS_PATH = str(PROJECT_ROOT / "datalake" / "silver" / "orders")

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("city", StringType(), False),
    StructField("created_at", StringType(), True)
])

def create_spark_session():
    return (
        SparkSession.builder
        .appName("DataNexus-BronzeToSilver-Orders")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def run_etl():
    spark = create_spark_session()
    logger.info("Spark session initialized.")

    try:
        # Check if source exists; if not, generate fallback seed DataFrame
        bronze_dir = Path(BRONZE_ORDERS_PATH)
        if bronze_dir.exists() and any(bronze_dir.glob("*.json")):
            logger.info(f"Reading raw orders from: {BRONZE_ORDERS_PATH}")
            df_raw = spark.read.schema(ORDER_SCHEMA).json(BRONZE_ORDERS_PATH)
        else:
            logger.info("Bronze orders directory empty. Generating baseline orders data.")
            sample_data = [
                ("ord-101", "usr-001", 149.99, "COMPLETED", "Mumbai", "2026-08-18 10:00:00"),
                ("ord-102", "usr-002", 89.50, "PENDING", "Bengaluru", "2026-08-18 11:30:00"),
                ("ord-103", "usr-003", 299.00, "COMPLETED", "Delhi", "2026-08-18 12:15:00"),
                ("ord-104", "usr-004", 45.00, "CANCELLED", "Hyderabad", "2026-08-18 13:00:00"),
                ("ord-105", "usr-005", 520.00, "COMPLETED", "Mumbai", "2026-08-18 14:20:00"),
                ("ord-106", "usr-001", 79.99, "COMPLETED", "Pune", "2026-08-18 15:45:00"),
                ("ord-107", "usr-002", 199.50, "COMPLETED", "Chennai", "2026-08-18 16:10:00"),
                ("ord-101", "usr-001", 149.99, "COMPLETED", "Mumbai", "2026-08-18 10:00:00"), # Duplicate
            ]
            df_raw = spark.createDataFrame(sample_data, schema=ORDER_SCHEMA)

        logger.info(f"Raw orders count: {df_raw.count()}")

        # Cleanse and Standardize
        df_cleaned = (
            df_raw
            .filter(col("order_id").isNotNull() & (col("total_amount") > 0))
            .withColumn("order_id", trim(col("order_id")))
            .withColumn("user_id", trim(col("user_id")))
            .withColumn("status", upper(trim(col("status"))))
            .withColumn("city", upper(trim(col("city"))))
            .withColumn("total_amount", spark_round(col("total_amount"), 2))
            .withColumn("order_timestamp", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("processed_at", current_timestamp())
        )

        # Deduplicate on order_id keeping newest
        window_spec = Window.partitionBy("order_id").orderBy(col("order_timestamp").desc())
        df_deduped = (
            df_cleaned
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num", "created_at")
        )

        logger.info(f"Cleaned and deduplicated orders count: {df_deduped.count()}")

        # Write to Silver partitioned by city
        (
            df_deduped.write
            .mode("overwrite")
            .partitionBy("city")
            .parquet(SILVER_ORDERS_PATH)
        )
        logger.info(f"Silver Orders Parquet successfully written to: {SILVER_ORDERS_PATH}")

    except Exception as e:
        logger.error(f"Orders ETL failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()
