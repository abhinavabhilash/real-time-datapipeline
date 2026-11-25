"""PySpark data parsing, validation, dead-letter routing, and deduplication."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas import RAW_EVENT_SCHEMA

VALID_EVENT_TYPES = (
    "product_view",
    "cart_update",
    "order_placed",
    "payment_success",
    "order_cancelled",
)


def parse_kafka_events(raw_df: DataFrame) -> DataFrame:
    """Deserialize Kafka binary key/value columns into typed event columns."""
    kafka_meta_cols = []
    for col_name in ("topic", "partition", "offset", "timestamp"):
        if col_name in raw_df.columns:
            kafka_meta_cols.append(F.col(col_name).alias(f"kafka_{col_name}"))

    string_df = raw_df.select(
        F.col("key").cast("string").alias("raw_key"),
        F.col("value").cast("string").alias("raw_value"),
        *kafka_meta_cols,
    )

    parsed_df = string_df.withColumn(
        "parsed", F.from_json(F.col("raw_value"), RAW_EVENT_SCHEMA)
    )

    flattened_df = parsed_df.select(
        "raw_key",
        "raw_value",
        *[c for c in parsed_df.columns if c.startswith("kafka_")],
        "parsed",
        "parsed.*",
    )

    # Parse ISO-8601 UTC timestamp string into Spark TimestampType
    return flattened_df.withColumn(
        "event_time",
        F.to_timestamp(F.col("event_timestamp")),
    )


def split_valid_and_invalid_events(
    parsed_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Validate parsed events and split into (valid_events_df, dead_letter_df).

    Business Rule:
    - Recognized revenue is counted ONLY on `payment_success` events with
      `payment_status == 'SUCCESS'`, preventing double-counting across
      `order_placed` and `payment_success`.
    """
    malformed_json_expr = F.col("parsed").isNull() | (
        F.col("event_id").isNull()
        & F.col("event_type").isNull()
        & F.col("event_timestamp").isNull()
        & F.col("user_id").isNull()
        & F.col("product_id").isNull()
    )

    validation_expr = (
        F.when(malformed_json_expr, F.lit("malformed_json"))
        .when(
            F.col("event_id").isNull() | (F.trim(F.col("event_id")) == ""),
            F.lit("missing_event_id"),
        )
        .when(
            F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""),
            F.lit("missing_user_id"),
        )
        .when(
            F.col("product_id").isNull() | (F.trim(F.col("product_id")) == ""),
            F.lit("missing_product_id"),
        )
        .when(F.col("event_time").isNull(), F.lit("invalid_timestamp"))
        .when(
            ~F.col("event_type").isin(*VALID_EVENT_TYPES),
            F.lit("unknown_event_type"),
        )
        .when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            F.lit("invalid_quantity"),
        )
        .when(
            F.col("total_amount").isNull() | (F.col("total_amount") < 0),
            F.lit("negative_total_amount"),
        )
        .when(
            (F.col("event_type") != "product_view") & (F.col("total_amount") <= 0),
            F.lit("non_positive_order_amount"),
        )
        .otherwise(F.lit(None).cast("string"))
    )

    tagged_df = parsed_df.withColumn("validation_error", validation_expr)

    valid_df = (
        tagged_df.filter(F.col("validation_error").isNull())
        .drop("parsed", "raw_key", "raw_value", "validation_error")
        .withColumn("currency", F.coalesce(F.col("currency"), F.lit("USD")))
        .withColumn(
            "is_payment_event",
            (F.col("event_type") == "payment_success")
            & (F.col("payment_status") == "SUCCESS"),
        )
        .withColumn(
            "is_order_event",
            F.col("event_type") == "order_placed",
        )
        .withColumn(
            "recognized_revenue",
            F.when(F.col("is_payment_event"), F.round(F.col("total_amount"), 2))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    dlq_cols = [
        "raw_key",
        "raw_value",
        "validation_error",
        *[c for c in tagged_df.columns if c.startswith("kafka_")],
    ]
    invalid_df = (
        tagged_df.filter(F.col("validation_error").isNotNull())
        .select(*dlq_cols)
        .withColumn("failed_at", F.current_timestamp())
    )

    return valid_df, invalid_df


def deduplicate_events(
    valid_df: DataFrame,
    watermark_delay: str = "2 minutes",
) -> DataFrame:
    """Remove duplicate event_id records, using watermarked state for streams."""
    if valid_df.isStreaming:
        return valid_df.withWatermark(
            "event_time", watermark_delay
        ).dropDuplicatesWithinWatermark(["event_id"])
    return valid_df.dropDuplicates(["event_id"])
