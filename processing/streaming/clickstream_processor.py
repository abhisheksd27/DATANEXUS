"""
DataNexus Real-Time Streaming: Kafka Clickstream to Bronze Data Lake
Consumes continuous real-time user browsing events from Kafka topic `clickstream_events`,
deserializes JSON payloads, validates schema, and writes micro-batches
to Bronze Data Lake using Spark Structured Streaming.
"""

import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ClickstreamStreamProcessor")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_PATH = str(PROJECT_ROOT / "datalake" / "bronze" / "kafka" / "clickstream")
CHECKPOINT_PATH = str(PROJECT_ROOT / "datalake" / "checkpoints" / "clickstream")

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("search_query", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", StringType(), False)
])

def create_spark_streaming_session():
    return (
        SparkSession.builder
        .appName("DataNexus-Streaming-Clickstream")
        .master("local[*]")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

def start_streaming():
    spark = create_spark_streaming_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Structured Streaming session initialized.")

    try:
        # Read continuous stream from Kafka broker
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "clickstream_events")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Deserialize binary payload into typed schema
        parsed_stream = (
            kafka_stream
            .selectExpr("CAST(value AS STRING) as json_payload")
            .select(from_json(col("json_payload"), CLICKSTREAM_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("ingested_at", current_timestamp())
        )

        logger.info(f"Writing stream to Bronze Data Lake: {OUTPUT_PATH}")

        # Sink to Data Lake with checkpointing
        query = (
            parsed_stream.writeStream
            .format("json")
            .outputMode("append")
            .option("path", OUTPUT_PATH)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime="10 seconds")
            .start()
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Streaming stopped by user.")
    except Exception as e:
        logger.error(f"Streaming error: {e}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    start_streaming()
