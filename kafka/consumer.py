"""Debug Kafka consumer for inspecting events from any pipeline topic."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def consume_messages(
    topic: str = settings.kafka.events_topic,
    group_id: str = settings.kafka.consumer_group,
    from_beginning: bool = True,
    max_messages: int = 0,
    bootstrap_servers: str = settings.kafka.bootstrap_servers,
) -> int:
    """Subscribe to `topic` and log incoming messages with manual offset commits."""
    consumer_config: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    logger.info(
        "Subscribed to topic '%s' (group='%s', offset_reset='%s')",
        topic,
        group_id,
        consumer_config["auto.offset.reset"],
    )

    consumed = 0
    try:
        while max_messages == 0 or consumed < max_messages:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "Reached end of %s [%d] at offset %d",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                    continue
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
            raw_value = msg.value().decode("utf-8", errors="replace") if msg.value() else ""

            try:
                parsed = json.loads(raw_value)
                event_summary = (
                    f"event_type={parsed.get('event_type')} "
                    f"user_id={parsed.get('user_id')} "
                    f"amount={parsed.get('total_amount')}"
                )
            except json.JSONDecodeError:
                event_summary = f"MALFORMED_JSON={raw_value}"

            consumed += 1
            print(
                f"[{consumed}] partition={msg.partition()} offset={msg.offset()} "
                f"key={key} | {event_summary}"
            )
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        logger.info("Consumer closed. Total messages read: %d", consumed)

    return consumed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume and inspect Kafka topic messages.")
    parser.add_argument(
        "--topic",
        type=str,
        default=settings.kafka.events_topic,
        help="Kafka topic to consume from.",
    )
    parser.add_argument(
        "--group",
        type=str,
        default=settings.kafka.consumer_group,
        help="Kafka consumer group ID.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after reading N messages (0 = run continuously).",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Start reading from latest offset instead of earliest.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    consume_messages(
        topic=args.topic,
        group_id=args.group,
        from_beginning=not args.latest,
        max_messages=args.max_messages,
    )
