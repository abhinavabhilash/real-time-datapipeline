"""Explicit PySpark schemas for raw e-commerce Kafka events."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Event timestamp is initially read as StringType so malformed timestamps
# are caught by explicit validation rather than dropping the whole JSON struct.
RAW_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("payment_status", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
    ]
)
