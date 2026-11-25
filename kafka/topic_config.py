"""Kafka topic administration and verification utilities."""

from __future__ import annotations

import logging
import time
from typing import Iterable

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.cimpl import KafkaError

from config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def get_admin_client(bootstrap_servers: str = settings.kafka.bootstrap_servers) -> AdminClient:
    """Create a Kafka AdminClient instance."""
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def ensure_topics_exist(
    topics: Iterable[tuple[str, int, int]] | None = None,
    bootstrap_servers: str = settings.kafka.bootstrap_servers,
    max_retries: int = 3,
    retry_delay_sec: float = 2.0,
) -> dict[str, str]:
    """Create required Kafka topics idempotently with retry handling.

    Each item in `topics` is (topic_name, num_partitions, replication_factor).
    Returns a mapping of topic_name -> status ('created' or 'exists').
    """
    target_topics = list(
        topics
        or [
            (
                settings.kafka.events_topic,
                settings.kafka.partitions,
                settings.kafka.replication_factor,
            ),
            (
                settings.kafka.dlq_topic,
                settings.kafka.partitions,
                settings.kafka.replication_factor,
            ),
        ]
    )

    admin = get_admin_client(bootstrap_servers)

    for attempt in range(1, max_retries + 1):
        try:
            metadata = admin.list_topics(timeout=10.0)
            existing = set(metadata.topics.keys())
            break
        except KafkaException as exc:
            logger.warning(
                "Attempt %d/%d: Kafka broker unreachable (%s). Retrying in %.1fs...",
                attempt,
                max_retries,
                exc,
                retry_delay_sec,
            )
            if attempt == max_retries:
                raise
            time.sleep(retry_delay_sec)

    results: dict[str, str] = {}
    new_topics: list[NewTopic] = []

    for name, partitions, replication_factor in target_topics:
        if name in existing:
            results[name] = "exists"
            logger.info("Topic '%s' already exists.", name)
        else:
            new_topics.append(
                NewTopic(
                    topic=name,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                    config={
                        "cleanup.policy": "delete",
                        "retention.ms": str(24 * 60 * 60 * 1000),  # 24 hours
                    },
                )
            )

    if new_topics:
        futures = admin.create_topics(new_topics, operation_timeout=15.0)
        for topic_name, future in futures.items():
            try:
                future.result()
                results[topic_name] = "created"
                logger.info("Created Kafka topic '%s'.", topic_name)
            except KafkaException as exc:
                err = exc.args[0]
                if hasattr(err, "code") and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    results[topic_name] = "exists"
                    logger.info("Topic '%s' already exists.", topic_name)
                else:
                    logger.error("Failed to create topic '%s': %s", topic_name, exc)
                    raise

    return results


def describe_topics(bootstrap_servers: str = settings.kafka.bootstrap_servers) -> dict[str, int]:
    """Return a dict of non-internal topic names mapped to their partition counts."""
    admin = get_admin_client(bootstrap_servers)
    metadata = admin.list_topics(timeout=10.0)
    return {
        name: len(topic_meta.partitions)
        for name, topic_meta in metadata.topics.items()
        if not name.startswith("__")
    }


def main() -> None:
    logger.info("Configuring Kafka topics on %s...", settings.kafka.bootstrap_servers)
    statuses = ensure_topics_exist()
    topics = describe_topics()
    for topic_name, status in statuses.items():
        partitions = topics.get(topic_name, "?")
        logger.info("Topic %-25s | status=%-8s | partitions=%s", topic_name, status, partitions)


if __name__ == "__main__":
    main()
