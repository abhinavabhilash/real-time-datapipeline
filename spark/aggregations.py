"""Event-time window aggregations for e-commerce KPIs and product analytics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _with_optional_watermark(
    df: DataFrame, watermark_delay: str, apply_watermark: bool = True
) -> DataFrame:
    """Apply an event-time watermark when running in Structured Streaming mode."""
    if apply_watermark and df.isStreaming:
        return df.withWatermark("event_time", watermark_delay)
    return df


def compute_windowed_metrics(
    events_df: DataFrame,
    window_duration: str = "1 minute",
    watermark_delay: str = "2 minutes",
    apply_watermark: bool = True,
) -> DataFrame:
    """Aggregate overall traffic, orders, payments, cancellations, revenue, and active users."""
    watermarked_df = _with_optional_watermark(
        events_df, watermark_delay, apply_watermark=apply_watermark
    )

    return (
        watermarked_df.groupBy(F.window(F.col("event_time"), window_duration))
        .agg(
            F.count("event_id").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).alias(
                "total_orders"
            ),
            F.sum(F.when(F.col("is_payment_event"), 1).otherwise(0)).alias(
                "successful_payments"
            ),
            F.sum(
                F.when(F.col("event_type") == "order_cancelled", 1).otherwise(0)
            ).alias("cancelled_orders"),
            F.round(F.sum("recognized_revenue"), 2).alias("total_revenue"),
            F.approx_count_distinct("user_id").alias("active_users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_events",
            "total_orders",
            "successful_payments",
            "cancelled_orders",
            "total_revenue",
            "active_users",
        )
    )


def compute_product_metrics(
    events_df: DataFrame,
    window_duration: str = "1 minute",
    watermark_delay: str = "2 minutes",
    apply_watermark: bool = True,
) -> DataFrame:
    """Aggregate product-level views, orders, units sold, and recognized revenue."""
    watermarked_df = _with_optional_watermark(
        events_df, watermark_delay, apply_watermark=apply_watermark
    )

    return (
        watermarked_df.groupBy(
            F.window(F.col("event_time"), window_duration),
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias(
                "view_count"
            ),
            F.sum(F.when(F.col("event_type") == "cart_update", 1).otherwise(0)).alias(
                "cart_add_count"
            ),
            F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).alias(
                "order_count"
            ),
            F.sum(
                F.when(F.col("is_payment_event"), F.col("quantity")).otherwise(0)
            ).alias("units_sold"),
            F.round(F.sum("recognized_revenue"), 2).alias("product_revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_id",
            "product_name",
            "category",
            "view_count",
            "cart_add_count",
            "order_count",
            "units_sold",
            "product_revenue",
        )
    )


def compute_event_type_metrics(
    events_df: DataFrame,
    window_duration: str = "1 minute",
    watermark_delay: str = "2 minutes",
    apply_watermark: bool = True,
) -> DataFrame:
    """Aggregate event counts per event_type within each event-time window."""
    watermarked_df = _with_optional_watermark(
        events_df, watermark_delay, apply_watermark=apply_watermark
    )

    return (
        watermarked_df.groupBy(
            F.window(F.col("event_time"), window_duration),
            F.col("event_type"),
        )
        .agg(
            F.count("event_id").alias("event_count"),
            F.round(F.sum("recognized_revenue"), 2).alias("recognized_revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "recognized_revenue",
        )
    )
