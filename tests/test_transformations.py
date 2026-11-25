"""Unit tests for PySpark parsing, validation, deduplication, and aggregations."""

import json
import os
import sys

import pytest
from pyspark.sql import SparkSession

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


@pytest.fixture(scope="session")
def spark():
    # Ensure Spark worker uses the same Python interpreter as pytest (.venv)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if not os.environ.get("JAVA_HOME") and os.path.isdir("/opt/homebrew/opt/openjdk@17"):
        os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

    session = (
        SparkSession.builder.master("local[2]")
        .appName("TestEcommerceTransformations")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_raw_df(spark: SparkSession, payloads: list[tuple[str, str]]):
    rows = [
        (key.encode("utf-8"), value.encode("utf-8"), "ecommerce-events", 0, idx)
        for idx, (key, value) in enumerate(payloads)
    ]
    return spark.createDataFrame(
        rows, schema=["key", "value", "topic", "partition", "offset"]
    )


def _sample_event(**overrides) -> dict:
    base = {
        "event_id": "evt-1",
        "event_type": "payment_success",
        "event_timestamp": "2026-11-25T10:00:15+00:00",
        "user_id": "USR-0001",
        "session_id": "SES-12345678",
        "product_id": "PROD-101",
        "product_name": "Wireless Headphones",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 100.0,
        "total_amount": 200.0,
        "currency": "USD",
        "order_id": "ORD-001",
        "payment_status": "SUCCESS",
        "device_type": "web_desktop",
        "country": "US",
    }
    base.update(overrides)
    return base


def test_split_valid_and_invalid_events_and_revenue_logic(spark: SparkSession):
    payloads = [
        ("USR-0001", json.dumps(_sample_event(event_id="e1", event_type="product_view", total_amount=0.0, payment_status=None, order_id=None))),
        ("USR-0001", json.dumps(_sample_event(event_id="e2", event_type="order_placed", total_amount=200.0, payment_status="PENDING"))),
        ("USR-0001", json.dumps(_sample_event(event_id="e3", event_type="payment_success", total_amount=200.0, payment_status="SUCCESS"))),
        # Invalid records
        ("bad-json", '{"event_id": "corrupted"'),
        ("USR-0002", json.dumps(_sample_event(event_id="e4", user_id=None))),
        ("USR-0003", json.dumps(_sample_event(event_id="e5", total_amount=-15.0))),
        ("USR-0004", json.dumps(_sample_event(event_id="e6", event_type="hacked_event"))),
        ("USR-0005", json.dumps(_sample_event(event_id="e7", event_timestamp="bad-date"))),
        ("USR-0006", json.dumps(_sample_event(event_id="e8", quantity=0))),
    ]

    parsed_df = parse_kafka_events(_make_raw_df(spark, payloads))
    valid_df, invalid_df = split_valid_and_invalid_events(parsed_df)

    valid_rows = {row["event_id"]: row for row in valid_df.collect()}
    assert set(valid_rows.keys()) == {"e1", "e2", "e3"}

    # Ensure zero double-counting: only payment_success recognizes revenue
    assert valid_rows["e1"]["recognized_revenue"] == 0.0
    assert valid_rows["e2"]["recognized_revenue"] == 0.0
    assert valid_rows["e3"]["recognized_revenue"] == 200.0

    invalid_errors = {row["validation_error"] for row in invalid_df.collect()}
    assert invalid_errors == {
        "malformed_json",
        "missing_user_id",
        "negative_total_amount",
        "unknown_event_type",
        "invalid_timestamp",
        "invalid_quantity",
    }


def test_deduplicate_events_removes_duplicate_ids(spark: SparkSession):
    event = _sample_event(event_id="dup-1")
    payloads = [
        ("USR-0001", json.dumps(event)),
        ("USR-0001", json.dumps(event)),
        ("USR-0002", json.dumps(_sample_event(event_id="unique-2", user_id="USR-0002"))),
    ]

    parsed_df = parse_kafka_events(_make_raw_df(spark, payloads))
    valid_df, _ = split_valid_and_invalid_events(parsed_df)
    deduped_df = deduplicate_events(valid_df)

    assert deduped_df.count() == 2


def test_windowed_and_product_aggregations(spark: SparkSession):
    payloads = [
        ("USR-0001", json.dumps(_sample_event(event_id="w1", event_type="product_view", total_amount=0.0, payment_status=None, order_id=None))),
        ("USR-0001", json.dumps(_sample_event(event_id="w2", event_type="order_placed", total_amount=200.0, payment_status="PENDING"))),
        ("USR-0001", json.dumps(_sample_event(event_id="w3", event_type="payment_success", quantity=2, total_amount=200.0, payment_status="SUCCESS"))),
        ("USR-0002", json.dumps(_sample_event(event_id="w4", user_id="USR-0002", product_id="PROD-102", product_name="Keyboard", event_type="payment_success", quantity=1, total_amount=129.5, payment_status="SUCCESS"))),
    ]

    parsed_df = parse_kafka_events(_make_raw_df(spark, payloads))
    valid_df, _ = split_valid_and_invalid_events(parsed_df)
    deduped_df = deduplicate_events(valid_df)

    window_rows = compute_windowed_metrics(deduped_df, window_duration="1 minute").collect()
    assert len(window_rows) == 1
    row = window_rows[0]
    assert row["total_events"] == 4
    assert row["total_orders"] == 1
    assert row["successful_payments"] == 2
    assert row["total_revenue"] == 329.5
    assert row["active_users"] == 2

    product_rows = {
        r["product_id"]: r
        for r in compute_product_metrics(deduped_df, window_duration="1 minute").collect()
    }
    assert product_rows["PROD-101"]["view_count"] == 1
    assert product_rows["PROD-101"]["units_sold"] == 2
    assert product_rows["PROD-101"]["product_revenue"] == 200.0
    assert product_rows["PROD-102"]["units_sold"] == 1
    assert product_rows["PROD-102"]["product_revenue"] == 129.5

    type_rows = {
        r["event_type"]: r["event_count"]
        for r in compute_event_type_metrics(deduped_df, window_duration="1 minute").collect()
    }
    assert type_rows == {
        "product_view": 1,
        "order_placed": 1,
        "payment_success": 2,
    }
