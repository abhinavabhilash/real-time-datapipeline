"""Realistic e-commerce streaming event generator.

Produces valid JSON events (product views, cart updates, orders, payments,
and cancellations) alongside configurable invalid, duplicate, and late-arriving
events for downstream validation, deduplication, and watermark testing.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from faker import Faker

from config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

VALID_EVENT_TYPES = (
    "product_view",
    "cart_update",
    "order_placed",
    "payment_success",
    "order_cancelled",
)

# Weighted probabilities simulate a realistic e-commerce conversion funnel
EVENT_TYPE_WEIGHTS = (0.45, 0.25, 0.14, 0.12, 0.04)

PRODUCT_CATALOG: tuple[dict[str, Any], ...] = (
    {"product_id": "PROD-101", "product_name": "Wireless Noise-Canceling Headphones", "category": "Electronics", "unit_price": 199.99},
    {"product_id": "PROD-102", "product_name": "Mechanical Gaming Keyboard", "category": "Electronics", "unit_price": 129.50},
    {"product_id": "PROD-103", "product_name": "4K USB-C Monitor 27-inch", "category": "Electronics", "unit_price": 349.00},
    {"product_id": "PROD-104", "product_name": "Ergonomic Laptop Stand", "category": "Accessories", "unit_price": 45.00},
    {"product_id": "PROD-105", "product_name": "USB-C Multiport Hub", "category": "Accessories", "unit_price": 39.99},
    {"product_id": "PROD-106", "product_name": "Smart Fitness Watch", "category": "Electronics", "unit_price": 249.00},
    {"product_id": "PROD-107", "product_name": "Insulated Stainless Water Bottle", "category": "Home & Kitchen", "unit_price": 28.50},
    {"product_id": "PROD-108", "product_name": "Pour-Over Coffee Maker", "category": "Home & Kitchen", "unit_price": 64.00},
    {"product_id": "PROD-109", "product_name": "Designing Data-Intensive Applications", "category": "Books", "unit_price": 48.99},
    {"product_id": "PROD-110", "product_name": "Cotton Crewneck Sweatshirt", "category": "Apparel", "unit_price": 54.00},
)

DEVICE_TYPES = ("mobile_ios", "mobile_android", "web_desktop", "web_tablet")
COUNTRIES = ("US", "IN", "GB", "CA", "DE", "AU", "SG")


class EcommerceDataGenerator:
    """Generates simulated e-commerce events as JSON strings."""

    def __init__(
        self,
        invalid_rate: float = settings.generator.invalid_event_rate,
        duplicate_rate: float = 0.02,
        late_event_rate: float = 0.03,
        user_pool_size: int = 200,
        seed: int | None = None,
    ) -> None:
        if not 0.0 <= invalid_rate <= 1.0:
            raise ValueError("invalid_rate must be between 0.0 and 1.0")
        if not 0.0 <= duplicate_rate <= 1.0:
            raise ValueError("duplicate_rate must be between 0.0 and 1.0")
        if not 0.0 <= late_event_rate <= 1.0:
            raise ValueError("late_event_rate must be between 0.0 and 1.0")

        self.invalid_rate = invalid_rate
        self.duplicate_rate = duplicate_rate
        self.late_event_rate = late_event_rate
        self.user_ids = [f"USR-{i:04d}" for i in range(1, user_pool_size + 1)]

        self._rng = random.Random(seed)
        self._faker = Faker()
        if seed is not None:
            self._faker.seed_instance(seed)

        self._recent_valid_events: list[dict[str, Any]] = []
        self._pending_orders: list[dict[str, Any]] = []

    def _now_utc(self, late_seconds: int = 0) -> str:
        ts = datetime.now(timezone.utc) - timedelta(seconds=late_seconds)
        return ts.isoformat()

    def generate_valid_event(self, force_event_type: str | None = None) -> dict[str, Any]:
        """Build a single schema-compliant e-commerce event dictionary."""
        event_type = force_event_type or self._rng.choices(
            VALID_EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1
        )[0]

        # Simulate realistic correlation: payment_success or order_cancelled can reference a recent pending order
        if event_type in ("payment_success", "order_cancelled") and self._pending_orders and self._rng.random() < 0.7:
            base_order = self._pending_orders.pop(0)
            user_id = base_order["user_id"]
            product_id = base_order["product_id"]
            product_name = base_order["product_name"]
            category = base_order["category"]
            quantity = base_order["quantity"]
            unit_price = base_order["unit_price"]
            total_amount = base_order["total_amount"]
            order_id = base_order["order_id"]
            device_type = base_order["device_type"]
            country = base_order["country"]
            session_id = base_order["session_id"]
        else:
            product = self._rng.choice(PRODUCT_CATALOG)
            user_id = self._rng.choice(self.user_ids)
            product_id = product["product_id"]
            product_name = product["product_name"]
            category = product["category"]
            unit_price = float(product["unit_price"])
            quantity = 1 if event_type == "product_view" else self._rng.randint(1, 4)
            total_amount = 0.0 if event_type == "product_view" else round(unit_price * quantity, 2)
            order_id = (
                f"ORD-{uuid.uuid4().hex[:10].upper()}"
                if event_type in ("order_placed", "payment_success", "order_cancelled")
                else None
            )
            device_type = self._rng.choice(DEVICE_TYPES)
            country = self._rng.choice(COUNTRIES)
            session_id = f"SES-{self._faker.lexify(text='????????').upper()}"

        payment_status_map = {
            "product_view": None,
            "cart_update": None,
            "order_placed": "PENDING",
            "payment_success": "SUCCESS",
            "order_cancelled": "CANCELLED",
        }

        late_seconds = (
            self._rng.randint(30, 180)
            if self._rng.random() < self.late_event_rate
            else 0
        )

        event: dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_timestamp": self._now_utc(late_seconds=late_seconds),
            "user_id": user_id,
            "session_id": session_id,
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "currency": "USD",
            "order_id": order_id,
            "payment_status": payment_status_map[event_type],
            "device_type": device_type,
            "country": country,
        }

        if event_type == "order_placed" and len(self._pending_orders) < 50:
            self._pending_orders.append(event)

        if len(self._recent_valid_events) >= 100:
            self._recent_valid_events.pop(0)
        self._recent_valid_events.append(event)

        return event

    def generate_invalid_event(self) -> tuple[str, str]:
        """Return a malformed or business-invalid JSON payload for validation/DLQ testing."""
        fault_type = self._rng.choice(
            (
                "missing_user_id",
                "negative_amount",
                "unknown_event_type",
                "invalid_timestamp",
                "non_positive_quantity",
                "corrupted_json",
            )
        )

        if fault_type == "corrupted_json":
            return f"invalid-{uuid.uuid4().hex[:6]}", '{"event_id": "corrupted", "total_amount": '

        event = self.generate_valid_event(force_event_type="order_placed")
        if fault_type == "missing_user_id":
            event["user_id"] = None
        elif fault_type == "negative_amount":
            event["total_amount"] = -round(self._rng.uniform(10.0, 200.0), 2)
        elif fault_type == "unknown_event_type":
            event["event_type"] = "invalid_action"
        elif fault_type == "invalid_timestamp":
            event["event_timestamp"] = "not-a-valid-iso-timestamp"
        elif fault_type == "non_positive_quantity":
            event["quantity"] = 0

        key = str(event.get("user_id") or event.get("event_id"))
        return key, json.dumps(event)

    def next_payload(self) -> tuple[str, str, str]:
        """Produce the next (partition_key, json_payload, classification) tuple.

        classification is one of: 'valid', 'duplicate', or 'invalid'.
        """
        roll = self._rng.random()

        if roll < self.invalid_rate:
            key, payload = self.generate_invalid_event()
            return key, payload, "invalid"

        if (
            self._recent_valid_events
            and roll < self.invalid_rate + self.duplicate_rate
        ):
            dup_event = dict(self._rng.choice(self._recent_valid_events))
            return str(dup_event["user_id"]), json.dumps(dup_event), "duplicate"

        event = self.generate_valid_event()
        return str(event["user_id"]), json.dumps(event), "valid"

    def stream_events(
        self,
        interval_sec: float = settings.generator.interval_sec,
        max_events: int | None = None,
    ) -> Iterator[tuple[str, str, str]]:
        """Continuously yield (partition_key, json_payload, classification) events."""
        emitted = 0
        while max_events is None or emitted < max_events:
            yield self.next_payload()
            emitted += 1
            if max_events is None or emitted < max_events:
                if interval_sec > 0:
                    time.sleep(interval_sec)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the standalone e-commerce JSON event generator."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of events to generate (0 for infinite stream).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=settings.generator.interval_sec,
        help="Delay in seconds between generated events.",
    )
    parser.add_argument(
        "--invalid-rate",
        type=float,
        default=settings.generator.invalid_event_rate,
        help="Probability [0.0-1.0] of emitting an invalid test event.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    max_events = None if args.count == 0 else args.count
    generator = EcommerceDataGenerator(invalid_rate=args.invalid_rate)

    logger.info(
        "Starting event generator (count=%s, interval=%.2fs, invalid_rate=%.2f)",
        "infinite" if max_events is None else max_events,
        args.interval,
        args.invalid_rate,
    )
    try:
        for idx, (key, payload, kind) in enumerate(
            generator.stream_events(interval_sec=args.interval, max_events=max_events),
            start=1,
        ):
            print(f"[{idx}] ({kind.upper()}) key={key} payload={payload}")
    except KeyboardInterrupt:
        logger.info("Event generator stopped by user.")


if __name__ == "__main__":
    main()
