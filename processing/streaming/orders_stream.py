"""
DataNexus Real-Time Streaming: Kafka Orders Stream
Consumes live order transactions from Kafka topic `order_events`,
computes tumbling/sliding window aggregations (revenue & order velocity per city),
and outputs real-time metrics for alerting and fast dashboard serving.
"""

import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, sum as spark_sum,
    count as spark_count, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("OrdersStreamProcessor")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_PATH = str(PROJECT_ROOT / "datalake" / "bronze" / "kafka" / "orders_stream")
CHECKPOINT_PATH = str(PROJECT_ROOT / "datalake" / "checkpoints" / "orders_stream")

ORDER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("shipping_city", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), False),
    StructField("timestamp", StringType(), False)
])

def create_spark_streaming_session():
    return (
        SparkSession.builder
        .appName("DataNexus-Streaming-Orders")
        .master("local[*]")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

def start_streaming():
    spark = create_spark_streaming_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Streaming Orders session initialized.")

    try:
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "order_events")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed_stream = (
            kafka_stream
            .selectExpr("CAST(value AS STRING) as json_payload")
            .select(from_json(col("json_payload"), ORDER_EVENT_SCHEMA).alias("order"))
            .select("order.*")
            .withColumn("order_time", to_timestamp(col("timestamp")))
        )

        # 10-minute tumbling window metrics by city
        windowed_metrics = (
            parsed_stream
            .withWatermark("order_time", "10 minutes")
            .groupBy(
                window(col("order_time"), "10 minutes", "5 minutes"),
                col("shipping_city")
            )
            .agg(
                spark_round(spark_sum("total_amount"), 2).alias("window_revenue"),
                spark_count("order_id").alias("window_order_count")
            )
        )

        query = (
            windowed_metrics.writeStream
            .format("console")
            .outputMode("update")
            .option("truncate", "false")
            .trigger(processingTime="15 seconds")
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
