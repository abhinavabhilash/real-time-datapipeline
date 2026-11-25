"""Spark Structured Streaming consumer for real-time e-commerce analytics.

Reads JSON events from Apache Kafka, validates and deduplicates records,
routes invalid messages to both a Kafka Dead-Letter Queue topic and PostgreSQL,
and writes curated events plus event-time windowed aggregations to PostgreSQL
with checkpointing for fault-tolerant recovery.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from confluent_kafka import Producer
from pyspark.sql import DataFrame, SparkSession

from config.settings import settings
from database.db_connection import init_database
from database.db_operations import (
    insert_dead_letter_events,
    insert_valid_events,
    upsert_event_type_metrics,
    upsert_product_metrics,
    upsert_windowed_metrics,
)
from kafka.topic_config import ensure_topics_exist
from spark.aggregations import (
    compute_event_type_metrics,
    compute_product_metrics,
    compute_windowed_metrics,
)
from spark.transformations import (
    deduplicate_events,
    parse_kafka_events,
    split_valid_and_invalid_events,
)

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def create_spark_session() -> SparkSession:
    """Create and configure a SparkSession with Kafka Structured Streaming support."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if not os.environ.get("JAVA_HOME") and os.path.isdir("/opt/homebrew/opt/openjdk@17"):
        os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

    spark = (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", str(settings.spark.shuffle_partitions))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _write_valid_events_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (
            r["event_id"],
            r["event_type"],
            r["event_time"],
            r["user_id"],
            r["session_id"],
            r["product_id"],
            r["product_name"],
            r["category"],
            r["quantity"],
            r["unit_price"],
            r["total_amount"],
            r["recognized_revenue"],
            r["currency"],
            r["order_id"],
            r["payment_status"],
            r["device_type"],
            r["country"],
            r["is_payment_event"],
            r["is_order_event"],
            r["kafka_partition"],
            r["kafka_offset"],
            r["processed_at"],
        )
        for r in batch_df.collect()
    ]
    if rows:
        inserted = insert_valid_events(rows)
        logger.info("Batch %d | valid_events=%d inserted=%d", batch_id, len(rows), inserted)


def _write_windowed_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (
            r["window_start"],
            r["window_end"],
            r["total_events"],
            r["total_orders"],
            r["successful_payments"],
            r["cancelled_orders"],
            r["total_revenue"],
            r["active_users"],
        )
        for r in batch_df.collect()
    ]
    if rows:
        upsert_windowed_metrics(rows)
        logger.info("Batch %d | upserted windowed_metrics=%d", batch_id, len(rows))


def _write_product_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (
            r["window_start"],
            r["window_end"],
            r["product_id"],
            r["product_name"],
            r["category"],
            r["view_count"],
            r["cart_add_count"],
            r["order_count"],
            r["units_sold"],
            r["product_revenue"],
        )
        for r in batch_df.collect()
    ]
    if rows:
        upsert_product_metrics(rows)
        logger.info("Batch %d | upserted product_metrics=%d", batch_id, len(rows))


def _write_event_type_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (
            r["window_start"],
            r["window_end"],
            r["event_type"],
            r["event_count"],
            r["recognized_revenue"],
        )
        for r in batch_df.collect()
    ]
    if rows:
        upsert_event_type_metrics(rows)
        logger.info("Batch %d | upserted event_type_metrics=%d", batch_id, len(rows))


def _write_dlq_batch(batch_df: DataFrame, batch_id: int) -> None:
    records = batch_df.collect()
    if not records:
        return

    db_rows = [
        (
            r["raw_key"],
            r["raw_value"],
            r["validation_error"],
            r["kafka_topic"],
            r["kafka_partition"],
            r["kafka_offset"],
            r["failed_at"],
        )
        for r in records
    ]
    insert_dead_letter_events(db_rows)

    dlq_producer = Producer({"bootstrap.servers": settings.kafka.bootstrap_servers})
    for r in records:
        dlq_payload = json.dumps(
            {
                "validation_error": r["validation_error"],
                "kafka_topic": r["kafka_topic"],
                "kafka_partition": r["kafka_partition"],
                "kafka_offset": r["kafka_offset"],
                "raw_key": r["raw_key"],
                "raw_value": r["raw_value"],
            }
        )
        dlq_producer.produce(
            topic=settings.kafka.dlq_topic,
            key=(r["raw_key"] or "dlq").encode("utf-8"),
            value=dlq_payload.encode("utf-8"),
        )
    dlq_producer.flush(timeout=5.0)
    logger.warning(
        "Batch %d | routed %d invalid record(s) to DLQ topic '%s' and PostgreSQL.",
        batch_id,
        len(records),
        settings.kafka.dlq_topic,
    )


def start_streaming(
    starting_offsets: str = "earliest",
    trigger_interval: str = "5 seconds",
    run_once: bool = False,
) -> None:
    """Start all Spark Structured Streaming queries and block until termination."""
    ensure_topics_exist()
    init_database()

    checkpoint_base = Path(settings.spark.checkpoint_dir)
    checkpoint_base.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    logger.info(
        "Starting Spark Structured Streaming (topic='%s', offsets='%s', checkpoint='%s')",
        settings.kafka.events_topic,
        starting_offsets,
        checkpoint_base,
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.events_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = parse_kafka_events(raw_stream)
    valid_stream, invalid_stream = split_valid_and_invalid_events(parsed_stream)
    deduped_stream = deduplicate_events(
        valid_stream, watermark_delay=settings.spark.watermark_delay
    )

    windowed_kpi_stream = compute_windowed_metrics(
        deduped_stream,
        window_duration=settings.spark.window_duration,
        watermark_delay=settings.spark.watermark_delay,
        apply_watermark=False,
    )
    product_kpi_stream = compute_product_metrics(
        deduped_stream,
        window_duration=settings.spark.window_duration,
        watermark_delay=settings.spark.watermark_delay,
        apply_watermark=False,
    )
    event_type_kpi_stream = compute_event_type_metrics(
        deduped_stream,
        window_duration=settings.spark.window_duration,
        watermark_delay=settings.spark.watermark_delay,
        apply_watermark=False,
    )

    trigger_kwargs = (
        {"availableNow": True}
        if run_once
        else {"processingTime": trigger_interval}
    )

    queries = [
        invalid_stream.writeStream.queryName("dlq_sink")
        .outputMode("append")
        .foreachBatch(_write_dlq_batch)
        .option("checkpointLocation", str(checkpoint_base / "dlq"))
        .trigger(**trigger_kwargs)
        .start(),
        deduped_stream.writeStream.queryName("valid_events_sink")
        .outputMode("append")
        .foreachBatch(_write_valid_events_batch)
        .option("checkpointLocation", str(checkpoint_base / "events"))
        .trigger(**trigger_kwargs)
        .start(),
        windowed_kpi_stream.writeStream.queryName("windowed_metrics_sink")
        .outputMode("update")
        .foreachBatch(_write_windowed_metrics_batch)
        .option("checkpointLocation", str(checkpoint_base / "windowed_metrics"))
        .trigger(**trigger_kwargs)
        .start(),
        product_kpi_stream.writeStream.queryName("product_metrics_sink")
        .outputMode("update")
        .foreachBatch(_write_product_metrics_batch)
        .option("checkpointLocation", str(checkpoint_base / "product_metrics"))
        .trigger(**trigger_kwargs)
        .start(),
        event_type_kpi_stream.writeStream.queryName("event_type_metrics_sink")
        .outputMode("update")
        .foreachBatch(_write_event_type_metrics_batch)
        .option("checkpointLocation", str(checkpoint_base / "event_type_metrics"))
        .trigger(**trigger_kwargs)
        .start(),
    ]

    try:
        if run_once:
            for q in queries:
                q.awaitTermination()
            logger.info("All availableNow streaming micro-batches finished.")
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Stopping Spark Structured Streaming queries gracefully...")
        for q in queries:
            if q.isActive:
                q.stop()
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the PySpark Structured Streaming e-commerce consumer."
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="earliest",
        help="Kafka offset position when starting without an existing checkpoint.",
    )
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default="5 seconds",
        help="Micro-batch trigger interval (e.g. '5 seconds').",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process all currently available Kafka messages once and exit (availableNow).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    start_streaming(
        starting_offsets=args.starting_offsets,
        trigger_interval=args.trigger_interval,
        run_once=args.once,
    )
