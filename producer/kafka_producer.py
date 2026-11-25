"""Reliable Kafka producer that streams generated e-commerce events."""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any

from confluent_kafka import KafkaException, Producer

from config.settings import settings
from kafka.topic_config import ensure_topics_exist
from producer.data_generator import EcommerceDataGenerator

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


class EcommerceKafkaProducer:
    """Wraps confluent_kafka.Producer with idempotent delivery and retry handling."""

    def __init__(
        self,
        bootstrap_servers: str = settings.kafka.bootstrap_servers,
        topic: str = settings.kafka.events_topic,
    ) -> None:
        self.topic = topic
        self.delivered_count = 0
        self.failed_count = 0

        producer_config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "ecommerce-event-producer",
            # Reliability & ordering guarantees
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 200,
            # Throughput & batching
            "linger.ms": 20,
            "batch.size": 32768,
            "compression.type": "snappy",
        }
        self._producer = Producer(producer_config)

    def _on_delivery(self, err: Any, msg: Any) -> None:
        """Callback invoked asynchronously when Kafka acknowledges (or rejects) a record."""
        if err is not None:
            self.failed_count += 1
            logger.error(
                "Delivery failed for topic=%s key=%s: %s",
                msg.topic() if msg else self.topic,
                msg.key().decode("utf-8", errors="replace") if msg and msg.key() else None,
                err,
            )
        else:
            self.delivered_count += 1
            logger.debug(
                "Delivered to %s [partition=%d, offset=%d]",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def send_event(self, key: str, payload: str, max_buffer_retries: int = 5) -> None:
        """Publish a single event to Kafka, backing off if the local queue is full."""
        for attempt in range(1, max_buffer_retries + 1):
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=key.encode("utf-8"),
                    value=payload.encode("utf-8"),
                    on_delivery=self._on_delivery,
                )
                self._producer.poll(0)
                return
            except BufferError:
                logger.warning(
                    "Producer queue full (attempt %d/%d); flushing...",
                    attempt,
                    max_buffer_retries,
                )
                self._producer.poll(0.5)
            except KafkaException as exc:
                logger.error("Kafka produce error: %s", exc)
                raise

        raise BufferError("Kafka producer local queue remained full after retries.")

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all buffered messages to be delivered."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("%d message(s) remained un-flushed after %.1fs.", remaining, timeout)
        return remaining


def run_producer(
    count: int = 0,
    interval_sec: float = settings.generator.interval_sec,
    invalid_rate: float = settings.generator.invalid_event_rate,
    duplicate_rate: float = 0.02,
    topic: str = settings.kafka.events_topic,
) -> None:
    """Ensure Kafka topics exist and stream generated events until count or Ctrl+C."""
    ensure_topics_exist()

    generator = EcommerceDataGenerator(
        invalid_rate=invalid_rate,
        duplicate_rate=duplicate_rate,
    )
    producer = EcommerceKafkaProducer(topic=topic)
    max_events = None if count == 0 else count

    logger.info(
        "Streaming to topic '%s' (count=%s, interval=%.2fs, invalid_rate=%.2f)",
        topic,
        "infinite" if max_events is None else max_events,
        interval_sec,
        invalid_rate,
    )

    sent = 0
    start_time = time.monotonic()
    try:
        for key, payload, kind in generator.stream_events(
            interval_sec=interval_sec,
            max_events=max_events,
        ):
            producer.send_event(key=key, payload=payload)
            sent += 1
            if sent % 10 == 0 or max_events is not None:
                logger.info("Produced event #%d (%s) key=%s", sent, kind, key)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user. Flushing remaining messages...")
    finally:
        producer.flush(timeout=10.0)
        elapsed = time.monotonic() - start_time
        logger.info(
            "Producer summary: sent=%d delivered=%d failed=%d elapsed=%.2fs",
            sent,
            producer.delivered_count,
            producer.failed_count,
            elapsed,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream simulated e-commerce events into Apache Kafka."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Total events to produce (0 = stream continuously until Ctrl+C).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=settings.generator.interval_sec,
        help="Seconds between generated events.",
    )
    parser.add_argument(
        "--invalid-rate",
        type=float,
        default=settings.generator.invalid_event_rate,
        help="Fraction of intentionally invalid test events [0.0-1.0].",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.02,
        help="Fraction of duplicate events to emit [0.0-1.0].",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=settings.kafka.events_topic,
        help="Target Kafka topic.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_producer(
        count=args.count,
        interval_sec=args.interval,
        invalid_rate=args.invalid_rate,
        duplicate_rate=args.duplicate_rate,
        topic=args.topic,
    )
