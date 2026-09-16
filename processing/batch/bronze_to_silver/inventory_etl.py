"""
DataNexus Batch ETL: Bronze to Silver - Inventory
Reads raw inventory catalog from PostgreSQL CDC / Bronze,
validates positive stock counts and pricing, standardizes categories,
and writes to Silver zone in Parquet format.
"""

import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, trim, upper, round as spark_round, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("InventoryETL")

PROJECT_ROOT = Path(__file__).resolve().parents[4]
BRONZE_INV_PATH = str(PROJECT_ROOT / "datalake" / "bronze" / "postgres" / "inventory")
SILVER_INV_PATH = str(PROJECT_ROOT / "datalake" / "silver" / "inventory")

INVENTORY_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("stock_quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False)
])

def create_spark_session():
    return (
        SparkSession.builder
        .appName("DataNexus-BronzeToSilver-Inventory")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def run_etl():
    spark = create_spark_session()
    logger.info("Spark session initialized.")

    try:
        bronze_dir = Path(BRONZE_INV_PATH)
        if bronze_dir.exists() and any(bronze_dir.glob("*.json")):
            logger.info(f"Reading raw inventory from: {BRONZE_INV_PATH}")
            df_raw = spark.read.schema(INVENTORY_SCHEMA).json(BRONZE_INV_PATH)
        else:
            logger.info("Bronze inventory directory empty. Generating baseline catalog data.")
            sample_data = [
                ("prod-A", "Wireless Headphones", "Electronics", 150, 149.99),
                ("prod-B", "Ergonomic Mouse", "Electronics", 300, 44.75),
                ("prod-C", "Mechanical Keyboard", "Electronics", 80, 299.00),
                ("prod-D", "USB-C Fast Charging Cable", "Accessories", 500, 19.99),
                ("prod-E", "Aluminum Laptop Stand", "Accessories", 220, 59.99),
                ("prod-F", "27-inch 4K IPS Monitor", "Displays", 45, 389.50),
                ("prod-G", "Noise-Cancelling Earbuds", "Electronics", 180, 89.00),
            ]
            df_raw = spark.createDataFrame(sample_data, schema=INVENTORY_SCHEMA)

        logger.info(f"Raw inventory count: {df_raw.count()}")

        # Cleanse and enforce business rules
        df_cleaned = (
            df_raw
            .filter(
                col("product_id").isNotNull() &
                (col("stock_quantity") >= 0) &
                (col("unit_price") > 0)
            )
            .withColumn("product_id", trim(col("product_id")))
            .withColumn("product_name", trim(col("product_name")))
            .withColumn("category", upper(trim(col("category"))))
            .withColumn("unit_price", spark_round(col("unit_price"), 2))
            .withColumn("processed_at", current_timestamp())
            .dropDuplicates(["product_id"])
        )

        logger.info(f"Cleaned inventory count: {df_cleaned.count()}")

        (
            df_cleaned.write
            .mode("overwrite")
            .partitionBy("category")
            .parquet(SILVER_INV_PATH)
        )
        logger.info(f"Silver Inventory Parquet successfully written to: {SILVER_INV_PATH}")

    except Exception as e:
        logger.error(f"Inventory ETL failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()
