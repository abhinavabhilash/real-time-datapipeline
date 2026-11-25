"""Unit tests for the e-commerce event data generator."""

import json
import uuid
from datetime import datetime

import pytest

from producer.data_generator import EcommerceDataGenerator, VALID_EVENT_TYPES


REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "event_timestamp",
    "user_id",
    "session_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "total_amount",
    "currency",
    "order_id",
    "payment_status",
    "device_type",
    "country",
}


def test_valid_event_structure_and_types():
    generator = EcommerceDataGenerator(invalid_rate=0.0, duplicate_rate=0.0, seed=42)
    event = generator.generate_valid_event()

    assert set(event.keys()) == REQUIRED_FIELDS
    assert uuid.UUID(event["event_id"])
    assert event["event_type"] in VALID_EVENT_TYPES
    datetime.fromisoformat(event["event_timestamp"])
    assert event["user_id"].startswith("USR-")
    assert event["product_id"].startswith("PROD-")
    assert event["currency"] == "USD"
    assert event["quantity"] >= 1
    assert event["unit_price"] > 0


def test_revenue_and_payment_status_rules():
    generator = EcommerceDataGenerator(invalid_rate=0.0, duplicate_rate=0.0, seed=7)

    view_event = generator.generate_valid_event(force_event_type="product_view")
    assert view_event["total_amount"] == 0.0
    assert view_event["payment_status"] is None
    assert view_event["order_id"] is None

    order_event = generator.generate_valid_event(force_event_type="order_placed")
    assert order_event["payment_status"] == "PENDING"
    assert order_event["order_id"].startswith("ORD-")
    assert order_event["total_amount"] > 0

    payment_event = generator.generate_valid_event(force_event_type="payment_success")
    assert payment_event["payment_status"] == "SUCCESS"
    assert payment_event["order_id"].startswith("ORD-")
    assert payment_event["total_amount"] == round(
        payment_event["unit_price"] * payment_event["quantity"], 2
    )


def test_event_ids_are_unique():
    generator = EcommerceDataGenerator(invalid_rate=0.0, duplicate_rate=0.0, seed=123)
    event_ids = {generator.generate_valid_event()["event_id"] for _ in range(500)}
    assert len(event_ids) == 500


def test_invalid_events_are_generated_when_configured():
    generator = EcommerceDataGenerator(invalid_rate=1.0, duplicate_rate=0.0, seed=99)
    classifications = []
    invalid_detected = 0

    for _ in range(50):
        _, payload, kind = generator.next_payload()
        classifications.append(kind)
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            invalid_detected += 1
            continue

        is_bad = (
            data.get("user_id") is None
            or data.get("total_amount", 0) < 0
            or data.get("event_type") not in VALID_EVENT_TYPES
            or data.get("quantity", 0) <= 0
        )
        if not is_bad:
            try:
                datetime.fromisoformat(data.get("event_timestamp", ""))
            except ValueError:
                is_bad = True
        if is_bad:
            invalid_detected += 1

    assert all(kind == "invalid" for kind in classifications)
    assert invalid_detected == 50


def test_stream_events_respects_max_events():
    generator = EcommerceDataGenerator(invalid_rate=0.1, duplicate_rate=0.1, seed=11)
    events = list(generator.stream_events(interval_sec=0.0, max_events=12))
    assert len(events) == 12


def test_invalid_rate_bounds_raise_value_error():
    with pytest.raises(ValueError):
        EcommerceDataGenerator(invalid_rate=1.5)
    with pytest.raises(ValueError):
        EcommerceDataGenerator(duplicate_rate=-0.1)
