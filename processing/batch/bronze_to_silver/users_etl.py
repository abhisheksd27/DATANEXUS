"""
DataNexus Batch ETL: Bronze to Silver - Users
Reads raw customer profiles from PostgreSQL CDC / Bronze,
validates email integrity, sanitizes fields, handles nulls,
and writes to Silver zone in Parquet format.
"""

import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, trim, lower, upper, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("UsersETL")

def _find_root():
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "datalake").exists() or parent.name.lower() in ("datanexus", "app"):
            return parent
    return p.parents[3]

PROJECT_ROOT = _find_root()
BRONZE_USERS_PATH = str(PROJECT_ROOT / "datalake" / "bronze" / "postgres" / "users")
SILVER_USERS_PATH = str(PROJECT_ROOT / "datalake" / "silver" / "users")

USER_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("full_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("city", StringType(), False),
    StructField("signup_date", StringType(), True)
])

def create_spark_session():
    return (
        SparkSession.builder
        .appName("DataNexus-BronzeToSilver-Users")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def run_etl():
    spark = create_spark_session()
    logger.info("Spark session initialized.")

    try:
        bronze_dir = Path(BRONZE_USERS_PATH)
        if bronze_dir.exists() and any(bronze_dir.glob("*.json")):
            logger.info(f"Reading raw users from: {BRONZE_USERS_PATH}")
            df_raw = spark.read.schema(USER_SCHEMA).json(BRONZE_USERS_PATH)
        else:
            logger.info("Bronze users directory empty. Generating baseline users data.")
            sample_data = [
                ("usr-001", "Aarav Sharma", "aarav@example.com", "Mumbai", "2026-08-01 09:00:00"),
                ("usr-002", "Priya Patel", "priya@example.com", "Bengaluru", "2026-08-02 10:15:00"),
                ("usr-003", "Rohan Verma", "rohan@example.com", "Delhi", "2026-08-03 11:30:00"),
                ("usr-004", "Ananya Reddy", "ananya@example.com", "Hyderabad", "2026-08-04 14:00:00"),
                ("usr-005", "Vikram Joshi", "vikram@example.com", "Pune", "2026-08-05 16:45:00"),
                ("usr-001", "Aarav Sharma", "aarav@example.com", "Mumbai", "2026-08-01 09:00:00"), # Duplicate
            ]
            df_raw = spark.createDataFrame(sample_data, schema=USER_SCHEMA)

        logger.info(f"Raw users count: {df_raw.count()}")

        # Cleanse, Normalize, and Validate
        email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

        df_cleaned = (
            df_raw
            .filter(col("user_id").isNotNull() & col("email").isNotNull())
            .filter(col("email").rlike(email_regex))
            .withColumn("user_id", trim(col("user_id")))
            .withColumn("full_name", trim(col("full_name")))
            .withColumn("email_normalized", lower(trim(col("email"))))
            .withColumn("city", upper(trim(col("city"))))
            .withColumn("signup_timestamp", to_timestamp(col("signup_date"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("processed_at", current_timestamp())
        )

        # Deduplicate by user_id
        window_spec = Window.partitionBy("user_id").orderBy(col("signup_timestamp").desc())
        df_deduped = (
            df_cleaned
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn", "signup_date", "email")
            .withColumnRenamed("email_normalized", "email")
        )

        logger.info(f"Cleaned users count: {df_deduped.count()}")

        (
            df_deduped.write
            .mode("overwrite")
            .parquet(SILVER_USERS_PATH)
        )
        logger.info(f"Silver Users Parquet successfully written to: {SILVER_USERS_PATH}")

    except Exception as e:
        logger.error(f"Users ETL failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()
